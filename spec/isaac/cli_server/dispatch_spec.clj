(ns isaac.cli-server.dispatch-spec
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [isaac.cli-server.dispatch :as sut]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(defn- decode-data [frame]
  (when-let [data (:data frame)]
    (String. (.decode (java.util.Base64/getDecoder) data))))

(describe "dispatch"

  (it "spawns the isaac launcher with the client argv"
    (let [sent    (atom [])
          send!   (fn [frame] (swap! sent conj frame))
          channel (Object.)
          spawned (atom nil)]
      (binding [sut/*spawn-process* (fn [command]
                                      (reset! spawned command)
                                      (p/process ["sh" "-c" "exit 0"] {:in :pipe :out :pipe :err :pipe}))]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["sessions" "list"]})
                           send!))
      (helper/await-condition #(some (fn [frame] (= "exit" (:type frame))) @sent) 5000)
      (should= ["isaac" "sessions" "list"] @spawned)
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
        (should= 2 (:code exit-frame))))))
