(ns isaac.cli-server.dispatch-spec
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [isaac.cli-server.dispatch :as sut]
    [isaac.logger :as log]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(defn- decode-data [frame]
  (when-let [data (:data frame)]
    (String. (.decode (java.util.Base64/getDecoder) data) "UTF-8")))

(describe "dispatch"
  (around [it] (log/capture-logs (it)))

  (it "spawns the isaac launcher with the client argv and emits a stream-id"
    (let [sent    (atom [])
          send!   (fn [frame] (swap! sent conj frame))
          channel (Object.)
          spawned (atom nil)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*spawn-process*     (fn [command]
                                          (reset! spawned command)
                                          (p/process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= ["isaac" "sessions" "list"] @spawned)
      (should= {:type "start-ack" :stream-id "stream-1"} (first @sent))
      (should= 0 (:code (last @sent)))))

  (it "allows the launcher command to be overridden"
    (let [sent    (atom [])
          send!   (fn [frame] (swap! sent conj frame))
          channel (Object.)
          spawned (atom nil)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*launcher-command*  ["/tmp/isaac-shim"]
                sut/*spawn-process*     (fn [command]
                                          (reset! spawned command)
                                          (p/process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= ["/tmp/isaac-shim" "sessions" "list"] @spawned)
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
      (binding [sut/*spawn-process* (fn [_command]
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
                sut/*spawn-process*     (fn [_]
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
    (let [sent    (atom [])
          send!   (fn [frame] (swap! sent conj frame))
          channel (Object.)
          spawned (atom nil)]
      (binding [sut/*stream-id-factory* (constantly "stream-1")
                sut/*spawn-process*     (fn [command]
                                          (reset! spawned command)
                                          (p/process ["sh" "-c" "exit 7"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (let [started  (some #(when (= :cli/command-started (:event %)) %) @log/captured-logs)
            finished (some #(when (= :cli/command-finished (:event %)) %) @log/captured-logs)]
        (should= ["isaac" "sessions" "list"] @spawned)
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
                sut/*spawn-process*     (fn [_]
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
                sut/*spawn-process*          (fn [_]
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
        (should (<= 0 (:duration-ms finished)))))))
