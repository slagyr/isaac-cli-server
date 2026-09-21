(ns isaac.cli-server.dispatch-spec
  (:require
    [cheshire.core :as json]
    [isaac.cli-server.dispatch :as sut]
    [isaac.cli.host :as host]
    [isaac.cli.registry :as registry]
    [isaac.logger :as log]
    [isaac.nexus :as nexus]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(defn- decode-data [frame]
  (when-let [data (:data frame)]
    (String. (.decode (java.util.Base64/getDecoder) data) "UTF-8")))

(defn- stdout-text [sent]
  (->> sent (filter #(= "stdout" (:type %))) (map decode-data) (apply str)))

(defn- exited? [sent]
  (some (fn [frame] (= "exit" (:type frame))) sent))

(defn- start! [channel argv send!]
  (sut/receive-line! channel (json/generate-string {:type "start" :argv argv}) send!))

(describe "dispatch"
  (around [it]
    (binding [sut/*principal* nil]
      (log/capture-logs (it))))

  (it "refuses a mutating hosted command when the loaded basis is stale"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "mutate" :run-fn (constantly 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*basis-current?*    (constantly false)
                  sut/*stream-id-factory* (constantly "stale-mutate")]
          (start! channel ["mutate"] #(swap! sent conj %))))
      (helper/await-condition #(exited? @sent) 5000)
      (should-contain "restart pending" (->> @sent (filter #(= "stderr" (:type %))) first decode-data))
      (should= 75 (:code (last @sent)))
      (should= :cli/refused-stale-basis (:event (first @log/captured-logs)))))

  (it "runs a read-only hosted command when the loaded basis is stale"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "read" :read-only true
                           :run-fn (fn [_] (println "read ok") 0)})
      ;; The hosted task runs on a future and reads the nexus; keep the
      ;; scope open until it exits or the restore races the command.
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*basis-current?*    (constantly false)
                  sut/*stream-id-factory* (constantly "stale-read")]
          (start! channel ["read"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (should= 0 (:code (last @sent)))))

  (it "applies read-only sets to the first subcommand"
    (should (#'sut/read-only-command? ["multi" "--quiet" "list"] {:read-only #{"list"}}))
    (should-not (#'sut/read-only-command? ["multi" "set"] {:read-only #{"list"}})))

  (it "runs commands on an embedded task"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "hosted-print"
                           :run-fn (fn [_] (println "hello hosted") 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-hosted")]
          (start! channel ["hosted-print"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (should= {:type "start-ack" :stream-id "stream-hosted"} (first @sent))
      (should= "hello hosted\n" (stdout-text @sent))
      (should= 0 (:code (last @sent)))))

  (it "runs a command that carries no hosted marker on an embedded task (isaac-dqy9)"
    ;; The transitional :hosted marker is gone: every command embeds, and a
    ;; command the registry never marked runs on a server thread all the same.
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "unmarked"
                           :run-fn (fn [_] (println "no marker needed") 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-unmarked")]
          (start! channel ["unmarked"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (should= "no marker needed\n" (stdout-text @sent))
      (should= 0 (:code (last @sent)))))

  (it "cancels a hosted command at the hot-reloaded wall-clock timeout"
    (let [sent      (atom [])
          channel   (Object.)
          shutdown? (promise)
          cfg       (atom {:cli-server {:timeout-ms 1}})]
      (registry/register! {:name "hosted-block"
                           :run-fn (fn [_]
                                     (host/on-shutdown! #(deliver shutdown? true))
                                     (host/block-until-cancelled!)
                                     0)})
      (nexus/-with-nexus {:root "/srv/isaac" :config cfg}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-timeout")]
          (start! channel ["hosted-block"] #(swap! sent conj %))
          (helper/await-condition #(exited? @sent) 5000)))
      (should= true (deref shutdown? 1000 false))
      (should= 124 (:code (last @sent)))))

  (it "streams stderr separately from stdout"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "two-streams"
                           :run-fn (fn [_]
                                     (println "ok-out")
                                     (binding [*out* *err*] (println "ok-err"))
                                     2)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-two")]
          (start! channel ["two-streams"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (let [stderr-text (->> @sent (filter #(= "stderr" (:type %))) (map decode-data) (apply str))]
        (should-contain "ok-out" (stdout-text @sent))
        (should-contain "ok-err" stderr-text)
        (should= 2 (:code (last @sent))))))

  (it "replays buffered frames after attach and renders them once"
    (let [sent-1    (atom [])
          sent-2    (atom [])
          channel-1 (Object.)
          channel-2 (Object.)
          gate      (promise)]
      (registry/register! {:name "two-lines"
                           :run-fn (fn [_]
                                     (println "first")
                                     (deref gate 5000 nil)
                                     (println "second")
                                     0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-1")
                  sut/*grace-period-ms*   5000]
          (start! channel-1 ["two-lines"] #(swap! sent-1 conj %))
          (helper/await-condition #(= "first\n" (stdout-text @sent-1)) 5000)
          (sut/disconnect! channel-1)
          (deliver gate true)
          (sut/receive-line! channel-2
                             (json/generate-string {:type "attach" :stream-id "stream-1"})
                             #(swap! sent-2 conj %)))
        (helper/await-condition #(exited? @sent-2) 5000))
      (should= "second\n" (stdout-text @sent-2))
      (should= "first\n" (stdout-text @sent-1))
      (should= 0 (:code (last @sent-2)))))

  (it "logs command start and finish with argv, stream id, exit code, and duration"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "exit-seven" :run-fn (fn [_] (host/exit! 7))})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-1")]
          (start! channel ["exit-seven" "now"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (let [started  (some #(when (= :cli/command-started (:event %)) %) @log/captured-logs)
            finished (some #(when (= :cli/command-finished (:event %)) %) @log/captured-logs)]
        (should-not-be-nil started)
        (should-not-be-nil finished)
        (should= ["exit-seven" "now"] (:argv started))
        (should= "stream-1" (:stream-id started))
        (should= "stream-1" (:stream-id finished))
        (should= 7 (:code finished))
        (should (integer? (:duration-ms finished)))
        (should (<= 0 (:duration-ms finished))))))

  (it "logs every command as hosted (isaac-dqy9)"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "marker-free" :run-fn (constantly 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-hosted-log")]
          (start! channel ["marker-free"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (let [started (some #(when (= :cli/command-started (:event %)) %) @log/captured-logs)]
        (should= true (:hosted started)))))

  (it "logs an exited detached stream as an abandoned finished command"
    (let [channel (Object.)
          gate    (promise)]
      (registry/register! {:name "gated-zero" :run-fn (fn [_] (deref gate 5000 nil) 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-1")
                  sut/*grace-period-ms*   5000]
          (start! channel ["gated-zero"] (fn [_]))
          (sut/disconnect! channel)
          (deliver gate true))
        (helper/await-condition #(some (fn [entry]
                                         (and (= :cli/command-finished (:event entry))
                                              (= :abandoned-stream (:reason entry))
                                              (contains? entry :code)))
                                       @log/captured-logs)
                                 5000))
      (let [finished (some #(when (and (= :abandoned-stream (:reason %))
                                       (contains? % :code))
                              %)
                           @log/captured-logs)]
        (should-not-be-nil finished)
        (should= "stream-1" (:stream-id finished))
        (should= ["gated-zero"] (:argv finished))
        (should= 0 (:code finished)))))

  (it "logs grace-window expiry as a finished command with a reason"
    (let [channel (Object.)]
      (registry/register! {:name "parked" :run-fn (fn [_] (host/block-until-cancelled!) 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*            "/srv/isaac"
                  sut/*stream-id-factory*      (constantly "stream-1")
                  sut/*grace-period-ms*        1
                  sut/*schedule-grace-timeout* (fn [_ f] (future (f)))
                  sut/*cancel-grace-timeout*   (fn [_] nil)]
          (start! channel ["parked"] (fn [_]))
          (sut/disconnect! channel))
        (helper/await-condition #(some (fn [entry]
                                         (and (= :cli/command-finished (:event entry))
                                              (= :grace-window-expired (:reason entry))))
                                       @log/captured-logs)
                                 5000))
      (let [finished (some #(when (= :grace-window-expired (:reason %)) %) @log/captured-logs)]
        (should-not-be-nil finished)
        (should= "stream-1" (:stream-id finished))
        (should= ["parked"] (:argv finished))
        (should-not (contains? finished :code))
        (should (integer? (:duration-ms finished)))
        (should (<= 0 (:duration-ms finished))))))

  (it "runs a read-only hosted command for a principal scoped cli/read"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "read" :read-only true
                           :run-fn (fn [_] (println "read ok") 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*principal*         {:name :viewer :scopes #{:cli/read}}
                  sut/*stream-id-factory* (constantly "scope-read")]
          (start! channel ["read"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (should= 0 (:code (last @sent)))))

  (it "refuses a mutating command for a principal scoped only cli/read"
    (let [sent    (atom [])
          channel (Object.)
          ran?    (atom false)]
      (registry/register! {:name "mutate"
                           :run-fn (fn [_] (reset! ran? true) 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*principal*         {:name :viewer :scopes #{:cli/read}}
                  sut/*stream-id-factory* (constantly "scope-mutate")]
          (start! channel ["mutate"] #(swap! sent conj %))))
      (helper/await-condition #(exited? @sent) 5000)
      (should-contain "requires cli" (->> @sent (filter #(= "stderr" (:type %))) first decode-data))
      (should= 77 (:code (last @sent)))
      (should-not @ran?)
      (should= :cli/refused-scope (:event (first @log/captured-logs)))
      (should= "viewer" (:principal (first @log/captured-logs)))))

  (it "runs a mutating command for a principal scoped cli and logs the principal"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "mutate" :run-fn (constantly 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*principal*         {:name :ops :scopes #{:cli}}
                  sut/*stream-id-factory* (constantly "scope-cli")]
          (start! channel ["mutate"] #(swap! sent conj %)))
        (helper/await-condition #(exited? @sent) 5000))
      (should= 0 (:code (last @sent)))
      (let [started (some #(when (= :cli/command-started (:event %)) %) @log/captured-logs)]
        (should= "ops" (:principal started)))))
)
