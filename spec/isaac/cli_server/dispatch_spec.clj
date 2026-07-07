(ns isaac.cli-server.dispatch-spec
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [isaac.cli-server.dispatch :as sut]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(defn- decode-data [frame]
  (when-let [data (:data frame)]
    (String. (.decode (java.util.Base64/getDecoder) data) "UTF-8")))

(describe "dispatch"

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
      (should= 0 (:code (last @sent-2))))))
