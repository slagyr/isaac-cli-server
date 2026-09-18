(ns isaac.cli-server.cli-server-steps
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [clojure.edn :as edn]
    [clojure.string :as str]
    [gherclj.core :as g :refer [defgiven defwhen defthen helper!]]
    [isaac.cli.host :as host]
    [isaac.cli.registry :as registry]
    [isaac.cli-server.dispatch :as dispatch]
    [isaac.cli-server.ws :as ws]
    [isaac.foundation.log-steps :as foundation-log]
    [isaac.config.api :as config]
    [isaac.logger :as log]
    [isaac.nexus :as nexus]
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
       (map decode-frame-data)
       (partition-by :type)
       (mapcat (fn [frames]
                 (if (contains? #{"stdout" "stderr"} (:type (first frames)))
                   [(assoc (first frames) :data (apply str (map :data frames)))]
                   frames)))
       vec))

(defn- process-options []
  {:in :pipe :out :pipe :err :pipe})

(defn- shell-process [command]
  (let [proc (p/process ["sh" "-c" command] (process-options))]
    (g/assoc! :cli-server-proc proc)
    proc))

(defn- recording-process [command _opts]
  (g/update! :cli-server-spawn-count (fnil inc 0))
  (g/assoc! :cli-server-recorded-command command)
  (shell-process "exit 0"))

(defn- recording-process-with-exit-code [exit-code]
  (fn [command _opts]
    (g/assoc! :cli-server-recorded-command command)
    (shell-process (str "exit " exit-code))))

(defn- reset-handler-state! []
  (log/set-output! :memory)
  (log/clear-entries!)
  (when-let [channel (g/get :cli-server-ws-channel)]
    (try
      (dispatch/disconnect! channel)
      (catch Exception _)))
  (g/assoc! :cli-server-basis-current? true)
  (g/assoc! :cli-server-channel-opts nil)
  (g/assoc! :cli-server-grace-period-ms nil)
  (g/assoc! :cli-server-grace-tasks {})
  (g/assoc! :cli-server-disconnected? false)
  (g/assoc! :cli-server-proc nil)
  (g/assoc! :cli-server-recorded-command nil)
  (g/assoc! :cli-server-spawn-count 0)
  (g/assoc! :cli-server-shutdown-ran? (atom false))
  (g/assoc! :cli-server-sent-frames (atom []))
  (g/assoc! :cli-server-spawn-factory nil)
  (g/assoc! :cli-server-ws-channel (Object.))
  (alter-var-root #'dispatch/*spawn-process* (constantly nil))
  (alter-var-root #'dispatch/*server-root* (constantly "/srv/isaac"))
  (alter-var-root #'ws/*frame-sender* (constantly nil)))

(defn- parse-scopes [scopes]
  (->> (str/split (str scopes) #",")
       (map str/trim)
       (remove str/blank?)
       (map #(if (= "*" %) :* (keyword %)))
       set))

(defn- invoke-handler! []
  (with-redefs [httpkit/as-channel (fn [_request opts]
                                     (g/assoc! :cli-server-channel-opts opts)
                                     {:body :channel})]
    (let [principal (g/get :cli-server-principal)
          request   (cond-> {:websocket? true :uri "/cli" :headers {}}
                      principal (assoc :isaac/principal principal))
          response  (ws/handler request)]
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
  (g/assoc! :cli-server-spawn-factory (fn [_request _opts]
                                        (shell-process command))))

(defn cli-server-handler-with-spawn-command-and-grace-window [command grace-ms]
  (install-handler!)
  (g/assoc! :cli-server-grace-period-ms (Long/parseLong (str grace-ms)))
  (g/assoc! :cli-server-spawn-factory (fn [_request _opts]
                                        (shell-process command))))

(defn cli-server-handler-with-recording-spawn-stub []
  (install-handler!)
  (g/assoc! :cli-server-spawn-factory recording-process))

(defn cli-server-handler-with-recording-spawn-stub-that-exits-with-code [exit-code]
  (install-handler!)
  (g/assoc! :cli-server-spawn-factory (recording-process-with-exit-code exit-code)))

(defn- register-fixture-commands! []
  (let [shutdown-ran? (g/get :cli-server-shutdown-ran?)]
    (registry/register! {:name "fx-echo" :hosted true
                         :run-fn (fn [_]
                                   (loop []
                                     (when-let [line (read-line)]
                                       (println line)
                                       (recur)))
                                   0)})
    (registry/register! {:name "fx-print" :hosted true
                         :run-fn (fn [{:keys [_raw-args]}] (println (str/join " " _raw-args)) 0)})
    (registry/register! {:name "fx-read" :hosted true :read-only true
                         :run-fn (fn [_] (println "read ok") 0)})
    (registry/register! {:name "fx-multi" :hosted true :read-only #{"list"}
                         :run-fn (constantly 0)})
    (registry/register! {:name "fx-exit" :hosted true
                         :run-fn (fn [{:keys [_raw-args]}] (host/exit! (parse-long (first _raw-args))))})
    (registry/register! {:name "fx-throw" :hosted true
                         :run-fn (fn [{:keys [_raw-args]}] (throw (ex-info (first _raw-args) {})))})
    (registry/register! {:name "fx-block" :hosted true
                         :run-fn (fn [_]
                                   (host/on-shutdown! #(reset! shutdown-ran? true))
                                   (host/block-until-cancelled!)
                                   0)})
    (registry/register! {:name "fx-local" :hosted true :local-only true :run-fn (constantly 0)})
    (registry/register! {:name "fx-legacy" :run-fn (constantly 0)})))

(defn cli-server-handler-with-fixture-commands []
  (install-handler!)
  (nexus/init! {:root "/srv/isaac"})
  (register-fixture-commands!)
  (g/assoc! :cli-server-spawn-factory recording-process))

(defn cli-server-handler-with-fixture-commands-and-grace [grace-ms]
  (cli-server-handler-with-fixture-commands)
  (g/assoc! :cli-server-grace-period-ms (Long/parseLong (str grace-ms))))

(defn- parse-argv [argv-text]
  (let [text (str/trim argv-text)]
    (cond
      (str/starts-with? text "[") (edn/read-string text)
      (str/includes? text ",")    (mapv str/trim (str/split text #","))
      :else                        (vec (remove str/blank? (str/split text #"\s+"))))))

(defn- schedule-grace-timeout! [delay-ms f]
  (let [token (Object.)]
    (g/update! :cli-server-grace-tasks assoc token f)
    token))

(defn- cancel-grace-timeout! [token]
  (g/update! :cli-server-grace-tasks dissoc token)
  nil)

(defn- send-client-line! [line]
  (let [on-receive (:on-receive (g/get :cli-server-channel-opts))]
    (g/should (fn? on-receive))
    (binding [dispatch/*grace-period-ms*      (or (g/get :cli-server-grace-period-ms) dispatch/*grace-period-ms*)
              dispatch/*schedule-grace-timeout* schedule-grace-timeout!
              dispatch/*cancel-grace-timeout* cancel-grace-timeout!
              dispatch/*spawn-process*        (g/get :cli-server-spawn-factory)
              dispatch/*basis-current?*        #(not (false? (g/get :cli-server-basis-current?)))]
      (on-receive (g/get :cli-server-ws-channel) line))))

(defn server-basis-behind []
  (g/assoc! :cli-server-basis-current? false))

(defn cli-client-is-principal [name scopes]
  (g/assoc! :cli-server-principal {:name (keyword name) :scopes (parse-scopes scopes)})
  (when (g/get :cli-server-channel-opts)
    (invoke-handler!)))

(defn cli-client-sends-start [argv-text]
  (when-not (g/get :cli-server-channel-opts)
    (cli-server-handler))
  (send-client-line! (json/generate-string {:type "start" :argv (parse-argv argv-text)})))

(defn cli-client-sends-stdin [text]
  (if (g/get :cli-server-disconnected?)
    (dispatch/-send-stdin! (:stream-id (first @(g/get :cli-server-sent-frames))) text)
    (send-client-line! (json/generate-string {:type "stdin"
                                              :data (codec/base64-encode (.getBytes text))}))))

(defn cli-client-sends-stdin-close []
  (send-client-line! (json/generate-string {:type "stdin-close"})))

(defn cli-client-attaches-to-issued-stream []
  (let [stream-id (:stream-id (first @(g/get :cli-server-sent-frames)))
        on-receive (:on-receive (g/get :cli-server-channel-opts))]
    (g/assoc! :cli-server-ws-channel (Object.))
    (binding [dispatch/*grace-period-ms*      (or (g/get :cli-server-grace-period-ms) dispatch/*grace-period-ms*)
              dispatch/*schedule-grace-timeout* schedule-grace-timeout!
              dispatch/*cancel-grace-timeout* cancel-grace-timeout!
              dispatch/*spawn-process*        (g/get :cli-server-spawn-factory)
              dispatch/*basis-current?*        #(not (false? (g/get :cli-server-basis-current?)))]
      (on-receive (g/get :cli-server-ws-channel)
                  (json/generate-string {:type "attach" :stream-id stream-id})))))

(defn cli-client-disconnects []
  (let [on-close (:on-close (g/get :cli-server-channel-opts))]
    (g/should (fn? on-close))
    (g/assoc! :cli-server-disconnected? true)
    (binding [dispatch/*grace-period-ms*      (or (g/get :cli-server-grace-period-ms) dispatch/*grace-period-ms*)
              dispatch/*schedule-grace-timeout* schedule-grace-timeout!
              dispatch/*cancel-grace-timeout* cancel-grace-timeout!]
      (on-close (g/get :cli-server-ws-channel) 1000))))

(defn- frame-type-expected? [expected-types frame]
  (contains? expected-types (:type frame)))

(defn- index-table [table]
  (if (some #(= "#index" %) (:headers table))
    table
    {:headers (into ["#index"] (:headers table))
     :rows    (mapv (fn [idx row] (into [(str idx)] row))
                    (range)
                    (:rows table))}))

(defn- handler-frame-result [table]
  (let [expected-types (->> (:rows table) (map first) set)
        entries        (->> (frames-for-matching)
                            (filter (partial frame-type-expected? expected-types))
                            vec)]
    (match/match-entries table entries)))

(defn handler-sends-frames [table]
  (helper/await-condition #(empty? (:failures (handler-frame-result table))) 15000)
  (g/should= [] (:failures (handler-frame-result table))))

(defn recorded-spawn-command-is [argv-text]
  (let [expected (into ["isaac"] (parse-argv argv-text))]
    (helper/await-condition #(some? (g/get :cli-server-recorded-command)) 5000)
    (g/should= expected (g/get :cli-server-recorded-command))))

(defn spawned-subprocess-running []
  (let [proc (g/get :cli-server-proc)]
    (g/should (and proc (.isAlive (:proc proc))))))

(defn grace-window-elapses []
  (doseq [[token task] (g/get :cli-server-grace-tasks)]
    (when (= task (get (g/get :cli-server-grace-tasks) token))
      (g/update! :cli-server-grace-tasks dissoc token)
      (task))))

(defn no-subprocess-spawned []
  (g/should= 0 (g/get :cli-server-spawn-count)))

(defn hosted-command-running []
  (g/should (dispatch/task-running?)))

(defn hosted-command-not-running []
  (helper/await-condition #(not (dispatch/task-running?)) 5000)
  (g/should-not (dispatch/task-running?)))

(defn hosted-shutdown-ran []
  (g/should @(g/get :cli-server-shutdown-ran?)))

(defn process-state-snapshotted []
  (g/assoc! :cli-server-process-state
            {:config   (config/process-memo-snapshot)
             :logger   (dissoc (log/snapshot) :entries)
             :nexus    (nexus/necho)
             :registry (registry/snapshot)}))

(defn process-state-unchanged []
  (g/should= (g/get :cli-server-process-state)
             {:config   (config/process-memo-snapshot)
              :logger   (dissoc (log/snapshot) :entries)
              :nexus    (nexus/necho)
              :registry (registry/snapshot)}))

(defn spawned-subprocess-not-running []
  (helper/await-condition #(let [proc (g/get :cli-server-proc)]
                              (and proc (not (.isAlive (:proc proc))))) 5000)
  (let [proc (g/get :cli-server-proc)]
    (g/should (and proc (not (.isAlive (:proc proc)))))))

(g/after-scenario reset-handler-state!)

(def cli-log-entries-match #'foundation-log/log-entries-match)
(def cli-log-entries-dont-match #'foundation-log/log-entries-dont-match)

(defgiven "the cli-server handler" isaac.cli-server.cli-server-steps/cli-server-handler)
(defgiven #"^the cli-server handler with spawn command \"([^\"]+)\"$" isaac.cli-server.cli-server-steps/cli-server-handler-with-spawn-command)
(defgiven #"^the cli-server handler with spawn command \"([^\"]+)\" and grace window (\d+) ms$"
  isaac.cli-server.cli-server-steps/cli-server-handler-with-spawn-command-and-grace-window)
(defgiven "the cli-server handler with a recording spawn stub" isaac.cli-server.cli-server-steps/cli-server-handler-with-recording-spawn-stub)
(defgiven "the cli-server handler with the fixture commands registered" isaac.cli-server.cli-server-steps/cli-server-handler-with-fixture-commands)
(defgiven #"^the cli-server handler with the fixture commands registered and grace window (\d+) ms$"
  isaac.cli-server.cli-server-steps/cli-server-handler-with-fixture-commands-and-grace)
(defgiven "the server process state is snapshotted" isaac.cli-server.cli-server-steps/process-state-snapshotted)
(defgiven "the server's loaded module basis is behind the on-disk basis" isaac.cli-server.cli-server-steps/server-basis-behind)
(defgiven "the /cli client is principal {name:string} with scopes {scopes:string}"
  isaac.cli-server.cli-server-steps/cli-client-is-principal)
(defgiven #"^the cli-server handler with a recording spawn stub that exits with code (\d+)$"
  isaac.cli-server.cli-server-steps/cli-server-handler-with-recording-spawn-stub-that-exits-with-code)

(defwhen "a /cli client sends start with argv {argv:string}" isaac.cli-server.cli-server-steps/cli-client-sends-start)
(defwhen "the /cli client sends stdin {text:string}" isaac.cli-server.cli-server-steps/cli-client-sends-stdin)
(defwhen "the /cli client sends stdin-close" isaac.cli-server.cli-server-steps/cli-client-sends-stdin-close)
(defwhen "the /cli client disconnects" isaac.cli-server.cli-server-steps/cli-client-disconnects)
(defwhen "a /cli client sends attach with the issued stream-id" isaac.cli-server.cli-server-steps/cli-client-attaches-to-issued-stream)
(defwhen "the grace window elapses" isaac.cli-server.cli-server-steps/grace-window-elapses)

(defthen "the handler sends frames:" isaac.cli-server.cli-server-steps/handler-sends-frames)
(defthen "the recorded spawn command is the isaac launcher with args {argv:string}" isaac.cli-server.cli-server-steps/recorded-spawn-command-is)
(defthen "the spawned subprocess is still running" isaac.cli-server.cli-server-steps/spawned-subprocess-running)
(defthen "the spawned subprocess is no longer running" isaac.cli-server.cli-server-steps/spawned-subprocess-not-running)
(defthen "no subprocess was spawned" isaac.cli-server.cli-server-steps/no-subprocess-spawned)
(defthen "the hosted command is still running" isaac.cli-server.cli-server-steps/hosted-command-running)
(defthen "the hosted command is no longer running" isaac.cli-server.cli-server-steps/hosted-command-not-running)
(defthen "the hosted command's shutdown fn ran" isaac.cli-server.cli-server-steps/hosted-shutdown-ran)
(defthen "the server process state is unchanged" isaac.cli-server.cli-server-steps/process-state-unchanged)
(defthen "the cli log has entries matching:" isaac.cli-server.cli-server-steps/cli-log-entries-match)
(defthen "the cli log has no entries matching:" isaac.cli-server.cli-server-steps/cli-log-entries-dont-match)
