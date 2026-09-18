(ns isaac.cli-server.dispatch-spec
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [isaac.cli-server.dispatch :as sut]
    [isaac.cli.registry :as registry]
    [isaac.logger :as log]
    [isaac.nexus :as nexus]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(defn- decode-data [frame]
  (when-let [data (:data frame)]
    (String. (.decode (java.util.Base64/getDecoder) data) "UTF-8")))

(describe "dispatch"
  (around [it]
    (binding [sut/*principal* nil]
      (log/capture-logs (it))))

  (it "refuses a mutating hosted command when the loaded basis is stale"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "mutate" :hosted true :run-fn (constantly 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*basis-current?*    (constantly false)
                  sut/*stream-id-factory* (constantly "stale-mutate")]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["mutate"]})
                             #(swap! sent conj %))))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should-contain "restart pending" (->> @sent (filter #(= "stderr" (:type %))) first decode-data))
      (should= 75 (:code (last @sent)))
      (should= :cli/refused-stale-basis (:event (first @log/captured-logs)))))

  (it "runs a read-only hosted command when the loaded basis is stale"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "read" :hosted true :read-only true
                           :run-fn (fn [_] (println "read ok") 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*basis-current?*    (constantly false)
                  sut/*stream-id-factory* (constantly "stale-read")]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["read"]})
                             #(swap! sent conj %))))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= 0 (:code (last @sent)))))

  (it "applies read-only sets to the first subcommand"
    (should (#'sut/read-only-command? ["multi" "--quiet" "list"] {:read-only #{"list"}}))
    (should-not (#'sut/read-only-command? ["multi" "set"] {:read-only #{"list"}})))

  (it "runs hosted commands on an embedded task without spawning a process"
    (let [sent     (atom [])
          channel  (Object.)
          spawned? (atom false)]
      (registry/register! {:name "hosted-print"
                           :hosted true
                           :run-fn (fn [_] (println "hello hosted") 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-hosted")
                  sut/*spawn-process*     (fn [& _] (reset! spawned? true))]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["hosted-print"]})
                             #(swap! sent conj %))))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= false @spawned?)
      (should= {:type "start-ack" :stream-id "stream-hosted"} (first @sent))
      (should= "hello hosted\n" (->> @sent (filter #(= "stdout" (:type %))) (map decode-data) (apply str)))
      (should= 0 (:code (last @sent)))))

  (it "cancels a hosted command at the hot-reloaded wall-clock timeout"
    (let [sent      (atom [])
          channel   (Object.)
          shutdown? (promise)
          cfg       (atom {:cli-server {:timeout-ms 1}})]
      (registry/register! {:name "hosted-block"
                           :hosted true
                           :run-fn (fn [_]
                                     (isaac.cli.host/on-shutdown! #(deliver shutdown? true))
                                     (isaac.cli.host/block-until-cancelled!)
                                     0)})
      (nexus/-with-nexus {:root "/srv/isaac" :config cfg}
        (binding [sut/*server-root*       "/srv/isaac"
                  sut/*stream-id-factory* (constantly "stream-timeout")]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["hosted-block"]})
                             #(swap! sent conj %))
          (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= true (deref shutdown? 1000 false))
      (should= 124 (:code (last @sent)))))

  (it "spawns the isaac launcher with the client argv and emits a stream-id"
    (let [sent        (atom [])
          send!       (fn [frame] (swap! sent conj frame))
          channel     (Object.)
          spawn-args* (atom nil)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*spawn-process*     (fn [command opts]
                                          (reset! spawn-args* {:command command :opts opts})
                                          (p/process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= ["isaac" "sessions" "list"] (:command @spawn-args*))
      (should= {:in :pipe :out :pipe :err :pipe} (:opts @spawn-args*))
      (should= {:type "start-ack" :stream-id "stream-1"} (first @sent))
      (should= 0 (:code (last @sent)))))

  (it "adds FORCE_COLOR to the spawned process when the client reports a tty stdout"
    (let [sent        (atom [])
          send!       (fn [frame] (swap! sent conj frame))
          channel     (Object.)
          spawn-args* (atom nil)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*spawn-process*     (fn [command opts]
                                          (reset! spawn-args* {:command command :opts opts})
                                          (p/process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"] :stdout-tty true})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= ["isaac" "sessions" "list"] (:command @spawn-args*))
      (should= {:in :pipe :out :pipe :err :pipe :extra-env {"FORCE_COLOR" "1"}} (:opts @spawn-args*))))

  (it "allows the launcher command to be overridden"
    (let [sent        (atom [])
          send!       (fn [frame] (swap! sent conj frame))
          channel     (Object.)
          spawn-args* (atom nil)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*launcher-command*  ["/tmp/isaac-shim"]
                sut/*spawn-process*     (fn [command opts]
                                          (reset! spawn-args* {:command command :opts opts})
                                          (p/process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= ["/tmp/isaac-shim" "sessions" "list"] (:command @spawn-args*))
      (should= {:in :pipe :out :pipe :err :pipe} (:opts @spawn-args*))
      (should= 0 (:code (last @sent)))))

  (it "spawns from the explicit root when argv carries --root"
    (let [sent         (atom [])
          send!        (fn [frame] (swap! sent conj frame))
          channel      (Object.)
          cwd          (atom nil)
          real-process p/process]
      (with-redefs [p/process (fn [_command opts]
                                (reset! cwd (:dir opts))
                                (real-process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (binding [sut/*stream-id-factory* (constantly "stream-1")]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["--root" "/tmp/fixture" "acp"]})
                             send!)))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= "/tmp/fixture" @cwd)
      (should= 0 (:code (last @sent)))))

  (it "streams stderr separately from stdout for a spawned process"
    (let [sent    (atom [])
          send!   (fn [frame] (swap! sent conj frame))
          channel (Object.)]
      (binding [sut/*spawn-process* (fn [_command _opts]
                                      (p/process ["sh" "-c" "printf ok-out\\n ; printf ok-err\\n >&2 ; exit 2"]
                                                 {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["test"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (let [stdout-frame (some #(when (= "stdout" (:type %)) %) @sent)
            stderr-frame (some #(when (= "stderr" (:type %)) %) @sent)
            exit-frame   (last @sent)]
        (should-not-be-nil stdout-frame)
        (should-not-be-nil stderr-frame)
        (should= "exit" (:type exit-frame))
        (should (re-find #"ok-out" (decode-data stdout-frame)))
        (should (re-find #"ok-err" (decode-data stderr-frame)))
        (should= 2 (:code exit-frame)))))

  (it "replays buffered frames after attach and renders them once"
    (let [sent-1    (atom [])
          sent-2    (atom [])
          channel-1 (Object.)
          channel-2 (Object.)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*grace-period-ms*   1000
                sut/*spawn-process*     (fn [_ _opts]
                                          (p/process ["sh" "-c" "printf 'first\\n' ; sleep 0.1 ; printf 'second\\n' ; exit 0"]
                                                     {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel-1
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           #(swap! sent-1 conj %))
        (helper/await-condition #(some (fn [frame] (re-find #"first" (or (decode-data frame) ""))) @sent-1) 5000)
        (sut/disconnect! channel-1)
        (helper/await-condition #(>= (count @sent-1) 2) 5000)
        (sut/receive-line! channel-2
                           (json/generate-string {:type "attach" :stream-id "stream-1"})
                           #(swap! sent-2 conj %)))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent-2) 5000)
      (should= ["second\n"] (->> @sent-2 (filter #(= "stdout" (:type %))) (map decode-data) vec))
      (should= 0 (:code (last @sent-2)))))

  (it "logs command start and finish with argv, stream id, exit code, and duration"
    (let [sent        (atom [])
          send!       (fn [frame] (swap! sent conj frame))
          channel     (Object.)
          spawn-args* (atom nil)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*spawn-process*     (fn [command opts]
                                          (reset! spawn-args* {:command command :opts opts})
                                          (p/process ["sh" "-c" "exit 7"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (let [started  (some #(when (= :cli/command-started (:event %)) %) @log/captured-logs)
            finished (some #(when (= :cli/command-finished (:event %)) %) @log/captured-logs)]
        (should= ["isaac" "sessions" "list"] (:command @spawn-args*))
        (should-not-be-nil started)
        (should-not-be-nil finished)
        (should= ["sessions" "list"] (:argv started))
        (should= "stream-1" (:stream-id started))
        (should= "stream-1" (:stream-id finished))
        (should= 7 (:code finished))
        (should (integer? (:duration-ms finished)))
        (should (<= 0 (:duration-ms finished))))))

  (it "logs an exited detached stream as an abandoned finished command"
    (let [channel (Object.)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*grace-period-ms*   1000
                sut/*spawn-process*     (fn [_ _opts]
                                          (p/process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           (fn [_]))
        (sut/disconnect! channel))
      (helper/await-condition #(some (fn [entry]
                                       (and (= :cli/command-finished (:event entry))
                                            (= :abandoned-stream (:reason entry))
                                            (contains? entry :code)))
                                     @log/captured-logs)
                               5000)
      (let [finished (some #(when (and (= :abandoned-stream (:reason %))
                                       (contains? % :code))
                              %)
                           @log/captured-logs)]
        (should-not-be-nil finished)
        (should= "stream-1" (:stream-id finished))
        (should= ["sessions" "list"] (:argv finished))
        (should= 0 (:code finished)))))

  (it "logs grace-window expiry as a finished command with a reason"
    (let [channel (Object.)]
      (binding [sut/*stream-id-factory*      (constantly "stream-1")
                sut/*grace-period-ms*        1
                sut/*schedule-grace-timeout* (fn [_ f] (future (f)))
                sut/*cancel-grace-timeout*   (fn [_] nil)
                sut/*spawn-process*          (fn [_ _opts]
                                               (p/process ["sh" "-c" "sleep 60"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           (fn [_]))
        (sut/disconnect! channel))
      (helper/await-condition #(some (fn [entry]
                                       (and (= :cli/command-finished (:event entry))
                                            (= :grace-window-expired (:reason entry))))
                                     @log/captured-logs)
                               5000)
      (let [finished (some #(when (= :grace-window-expired (:reason %)) %) @log/captured-logs)]
        (should-not-be-nil finished)
        (should= "stream-1" (:stream-id finished))
        (should= ["sessions" "list"] (:argv finished))
        (should-not (contains? finished :code))
        (should (integer? (:duration-ms finished)))
        (should (<= 0 (:duration-ms finished))))))

  (it "runs a read-only hosted command for a principal scoped cli/read"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "read" :hosted true :read-only true
                           :run-fn (fn [_] (println "read ok") 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*principal*         {:name :viewer :scopes #{:cli/read}}
                  sut/*stream-id-factory* (constantly "scope-read")]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["read"]})
                             #(swap! sent conj %))))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= 0 (:code (last @sent)))))

  (it "refuses a mutating command for a principal scoped only cli/read"
    (let [sent    (atom [])
          channel (Object.)
          ran?    (atom false)]
      (registry/register! {:name "mutate" :hosted true
                           :run-fn (fn [_] (reset! ran? true) 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*principal*         {:name :viewer :scopes #{:cli/read}}
                  sut/*stream-id-factory* (constantly "scope-mutate")]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["mutate"]})
                             #(swap! sent conj %))))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should-contain "requires cli" (->> @sent (filter #(= "stderr" (:type %))) first decode-data))
      (should= 77 (:code (last @sent)))
      (should-not @ran?)
      (should= :cli/refused-scope (:event (first @log/captured-logs)))
      (should= "viewer" (:principal (first @log/captured-logs)))))

  (it "runs a mutating command for a principal scoped cli and logs the principal"
    (let [sent    (atom [])
          channel (Object.)]
      (registry/register! {:name "mutate" :hosted true :run-fn (constantly 0)})
      (nexus/-with-nexus {:root "/srv/isaac"}
        (binding [sut/*principal*         {:name :ops :scopes #{:cli}}
                  sut/*stream-id-factory* (constantly "scope-cli")]
          (sut/receive-line! channel
                             (json/generate-string {:type "start" :argv ["mutate"]})
                             #(swap! sent conj %))))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= 0 (:code (last @sent)))
      (let [started (some #(when (= :cli/command-started (:event %)) %) @log/captured-logs)]
        (should= "ops" (:principal started)))))
)
