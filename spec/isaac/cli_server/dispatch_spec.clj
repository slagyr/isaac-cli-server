(ns isaac.cli-server.dispatch-spec
  (:require
    [cheshire.core :as json]
    [isaac.cli-server.dispatch :as sut]
    [isaac.main :as main]
    [ring.util.codec :as codec]
    [speclj.core :refer :all]))

(defn- decode-data [frame]
  (when-let [data (:data frame)]
    (String. (.decode (java.util.Base64/getDecoder) data))))

(describe "dispatch"

  (it "frames stderr separately from stdout"
    (let [sent (atom [])
          send! (fn [frame] (swap! sent conj frame))
          channel (Object.)]
      (with-redefs [main/run (fn [_]
                               (println "ok-out")
                               (.println *err* "ok-err")
                               2)]
        (sut/receive-line! channel
                           (json/generate-string {:type "start" :argv ["test"]})
                           send!))
      (should= ["stdout" "stderr" "exit"] (mapv :type @sent))
      (should (re-find #"ok-out" (decode-data (first @sent))))
      (should (re-find #"ok-err" (decode-data (second @sent))))
      (should= 2 (:code (last @sent))))))