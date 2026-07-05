(ns isaac.cli-server.cli-server-steps
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [clojure.edn :as edn]
    [clojure.string :as str]
    [gherclj.core :as g :refer [defgiven defwhen defthen helper!]]
    [isaac.cli-server.dispatch :as dispatch]
    [isaac.cli-server.ws :as ws]
    [isaac.spec-helper :as helper]
    [isaac.step-tables :as match]
    [org.httpkit.server :as httpkit]
    [ring.util.codec :as codec]))

(helper! isaac.cli-server.cli-server-steps)

(defn- capture-frame! [sent-frames _channel payload]
  (swap! sent-frames conj (json/parse-string payload true)))

(defn- decode-frame-data [frame]
  (if-let [data (:data frame)]
    (assoc frame :data (String. (.decode (java.util.Base64/getDecoder) data)))
    frame))

(defn- frames-for-matching []
  (->> @(g/get :cli-server-sent-frames)
       (mapv decode-frame-data)))

(defn- process-options []
  {:in :pipe :out :pipe :err :pipe})

(defn- shell-process [command]
  (let [proc (p/process ["sh" "-c" command] (process-options))]
    (g/assoc! :cli-server-proc proc)
    proc))

(defn- recording-process [command]
  (g/assoc! :cli-server-recorded-command command)
  (shell-process "exit 0"))

(defn- reset-handler-state! []
  (when-let [channel (g/get :cli-server-ws-channel)]
    (try
      (dispatch/disconnect! channel)
      (catch Exception _)))
  (g/assoc! :cli-server-channel-opts nil)
  (g/assoc! :cli-server-proc nil)
  (g/assoc! :cli-server-recorded-command nil)
  (g/assoc! :cli-server-sent-frames (atom []))
  (g/assoc! :cli-server-spawn-factory nil)
  (g/assoc! :cli-server-ws-channel (Object.))
  (alter-var-root #'dispatch/*spawn-process* (constantly nil))
  (alter-var-root #'ws/*frame-sender* (constantly nil)))

(defn- invoke-handler! []
  (with-redefs [httpkit/as-channel (fn [_request opts]
                                     (g/assoc! :cli-server-channel-opts opts)
                                     {:body :channel})]
    (let [response (ws/handler {:websocket? true :uri "/cli" :headers {}})]
      (g/should= :channel (:body response))
      (when-let [on-open (:on-open (g/get :cli-server-channel-opts))]
        (on-open (g/get :cli-server-ws-channel))))))

(defn- install-handler! []
  (reset-handler-state!)
  (let [sent-frames (g/get :cli-server-sent-frames)]
    (alter-var-root #'ws/*frame-sender* (constantly (partial capture-frame! sent-frames))))
  (invoke-handler!))

(defn cli-server-handler []
  (install-handler!))

(defn cli-server-handler-with-spawn-command [command]
  (install-handler!)
  (g/assoc! :cli-server-spawn-factory (fn [_request]
                                        (shell-process command))))

(defn cli-server-handler-with-recording-spawn-stub []
  (install-handler!)
  (g/assoc! :cli-server-spawn-factory recording-process))

(defn- parse-argv [argv-text]
  (let [text (str/trim argv-text)]
    (cond
      (str/starts-with? text "[") (edn/read-string text)
      (str/includes? text ",")    (mapv str/trim (str/split text #","))
      :else                        (vec (remove str/blank? (str/split text #"\s+"))))))

(defn- send-client-line! [line]
  (let [on-receive (:on-receive (g/get :cli-server-channel-opts))]
    (g/should (fn? on-receive))
    (binding [dispatch/*spawn-process* (g/get :cli-server-spawn-factory)]
      (on-receive (g/get :cli-server-ws-channel) line))))

(defn cli-client-sends-start [argv-text]
  (when-not (g/get :cli-server-channel-opts)
    (cli-server-handler))
  (send-client-line! (json/generate-string {:type "start" :argv (parse-argv argv-text)})))

(defn cli-client-sends-stdin [text]
  (send-client-line! (json/generate-string {:type "stdin"
                                            :data (codec/base64-encode (.getBytes text))})))

(defn cli-client-sends-stdin-close []
  (send-client-line! (json/generate-string {:type "stdin-close"})))

(defn cli-client-disconnects []
  (let [on-close (:on-close (g/get :cli-server-channel-opts))]
    (g/should (fn? on-close))
    (on-close (g/get :cli-server-ws-channel) 1000)))

(defn handler-sends-frames [table]
  (helper/await-condition #(empty? (:failures (match/match-entries table (frames-for-matching)))) 5000)
  (let [result (match/match-entries table (frames-for-matching))]
    (g/should= [] (:failures result))))

(defn recorded-spawn-command-is [argv-text]
  (let [expected (into ["isaac"] (parse-argv argv-text))]
    (helper/await-condition #(some? (g/get :cli-server-recorded-command)) 5000)
    (g/should= expected (g/get :cli-server-recorded-command))))

(defn spawned-subprocess-not-running []
  (helper/await-condition #(let [proc (g/get :cli-server-proc)]
                              (and proc (not (.isAlive (:proc proc))))) 5000)
  (let [proc (g/get :cli-server-proc)]
    (g/should (and proc (not (.isAlive (:proc proc)))))))

(g/after-scenario reset-handler-state!)

(defgiven "the cli-server handler" isaac.cli-server.cli-server-steps/cli-server-handler)
(defgiven "the cli-server handler with spawn command {command:string}" isaac.cli-server.cli-server-steps/cli-server-handler-with-spawn-command)
(defgiven "the cli-server handler with a recording spawn stub" isaac.cli-server.cli-server-steps/cli-server-handler-with-recording-spawn-stub)

(defwhen "a /cli client sends start with argv {argv:string}" isaac.cli-server.cli-server-steps/cli-client-sends-start)
(defwhen "the /cli client sends stdin {text:string}" isaac.cli-server.cli-server-steps/cli-client-sends-stdin)
(defwhen "the /cli client sends stdin-close" isaac.cli-server.cli-server-steps/cli-client-sends-stdin-close)
(defwhen "the /cli client disconnects" isaac.cli-server.cli-server-steps/cli-client-disconnects)

(defthen "the handler sends frames:" isaac.cli-server.cli-server-steps/handler-sends-frames)
(defthen "the recorded spawn command is the isaac launcher with args {argv:string}" isaac.cli-server.cli-server-steps/recorded-spawn-command-is)
(defthen "the spawned subprocess is no longer running" isaac.cli-server.cli-server-steps/spawned-subprocess-not-running)
