(ns isaac.cli-server.cli-server-steps
  (:require
    [cheshire.core :as json]
    [clojure.edn :as edn]
    [clojure.string :as str]
    [gherclj.core :as g :refer [defgiven defwhen defthen helper!]]
    [isaac.cli-server.dispatch :as dispatch]
    [isaac.cli-server.ws :as ws]
    [isaac.step-tables :as match]
    [org.httpkit.server :as httpkit]
    [ring.util.codec :as codec]
    [babashka.process :as p]))

(helper! isaac.cli-server.cli-server-steps)

(defn- reset-handler-state! []
  (g/assoc! :cli-server-channel-opts nil)
  (g/assoc! :cli-server-sent-frames [])
  (g/assoc! :cli-server-ws-channel (Object.))
  (alter-var-root #'ws/*frame-sender* (constantly nil)))

(defn- capture-frame! [_channel payload]
  (g/update! :cli-server-sent-frames conj (json/parse-string payload true)))

(defn- with-captured-frames! [thunk]
  (alter-var-root #'ws/*frame-sender* (constantly capture-frame!))
  (try
    (thunk)
    (finally
      (alter-var-root #'ws/*frame-sender* (constantly nil)))))

(defn- decode-frame-data [frame]
  (if-let [data (:data frame)]
    (assoc frame :data (String. (.decode (java.util.Base64/getDecoder) data)))
    frame))

(defn- frames-for-matching []
  (->> (g/get :cli-server-sent-frames)
       (mapv decode-frame-data)))

(defn- invoke-handler! []
  (with-redefs [httpkit/as-channel (fn [_request opts]
                                    (g/assoc! :cli-server-channel-opts opts)
                                    {:body :channel})]
    (let [response (ws/handler {:websocket? true :uri "/cli" :headers {}})]
      (g/should= :channel (:body response))
      (when-let [on-open (:on-open (g/get :cli-server-channel-opts))]
        (on-open (g/get :cli-server-ws-channel))))))

(defn- send-client-line! [line]
  (let [on-receive (:on-receive (g/get :cli-server-channel-opts))]
    (g/should (fn? on-receive))
    (on-receive (g/get :cli-server-ws-channel) line)))

(defn cli-server-handler []
  (reset-handler-state!)
  (with-captured-frames! invoke-handler!))

(defn- parse-argv [argv-text]
  (let [text (str/trim argv-text)]
    (cond
      (str/starts-with? text "[") (edn/read-string text)
      (str/includes? text ",")      (mapv str/trim (str/split text #","))
      :else                         (vec (remove str/blank? (str/split text #"\s+"))))))

(defn cli-client-sends-start [argv-text]
  (when-not (g/get :cli-server-channel-opts)
    (cli-server-handler))
  (with-captured-frames!
    #(send-client-line! (json/generate-string {:type "start" :argv (parse-argv argv-text)}))))

(defn cli-client-sends-stdin [text]
  (with-captured-frames!
    #(send-client-line! (json/generate-string {:type "stdin"
                                              :data  (codec/base64-encode (.getBytes text))}))))

(defn cli-client-sends-stdin-close []
  (with-captured-frames!
    #(send-client-line! (json/generate-string {:type "stdin-close"}))))

(defn handler-sends-frames [table]
  (let [entries (frames-for-matching)
        result  (match/match-entries table entries)]
    (g/should= [] (:failures result))))

(g/after-scenario reset-handler-state!)

(defgiven "the cli-server handler" isaac.cli-server.cli-server-steps/cli-server-handler)

(defwhen "a /cli client sends start with argv {argv:string}" isaac.cli-server.cli-server-steps/cli-client-sends-start)

(defwhen "the /cli client sends stdin {text:string}" isaac.cli-server.cli-server-steps/cli-client-sends-stdin)

(defwhen "the /cli client sends stdin-close" isaac.cli-server.cli-server-steps/cli-client-sends-stdin-close)

(defthen "the handler sends frames:" isaac.cli-server.cli-server-steps/handler-sends-frames)