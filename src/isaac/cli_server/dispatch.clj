(ns isaac.cli-server.dispatch
  (:require
    [cheshire.core :as json]
    [isaac.main :as main]
    [babashka.process :as p]
    [ring.util.codec :as codec])
  (:import
    (java.io ByteArrayOutputStream InputStreamReader BufferedReader PrintWriter)))

(defn- b64-encode [^String s]
  (when (seq s)
    (codec/base64-encode (.getBytes s "UTF-8"))))

(defn- send-frame! [send! frame]
  (send! frame))

(defn- send-error! [send! message]
  (send-frame! send! {:type "error" :message message}))

(defn- stream-bytes! [send! type ^ByteArrayOutputStream buf]
  (when-let [text (not-empty (str buf))]
    (send-frame! send! {:type type :data (b64-encode text)})))

;; --- old in-process path (for non-wip batch scenarios during migration) ---
(defn- run-argv! [argv _cwd send!]
  (let [out-buf (ByteArrayOutputStream.)
        err-buf (ByteArrayOutputStream.)
        out-w   (PrintWriter. out-buf true)
        err-w   (PrintWriter. err-buf true)
        argv    (vec (or argv []))]
    (binding [*out* out-w
              *err* err-w]
      (let [code (try
                   (main/run argv)
                   (catch Exception e
                     (.printStackTrace e err-w)
                     1))]
        (.flush out-w)
        (.flush err-w)
        (stream-bytes! send! "stdout" out-buf)
        (stream-bytes! send! "stderr" err-buf)
        (send-frame! send! {:type "exit" :code (long code)})))))

;; --- new subprocess path (for 895i AC) ---
(def ^:dynamic *spawn-command* nil)

(defn- default-spawn [argv]
  (p/process (into ["isaac"] (vec (or argv [])))
             {:in :pipe :out :pipe :err :pipe}))

(defn- effective-spawn [argv]
  (cond
    (fn? *spawn-command*) (*spawn-command* argv)
    (vector? *spawn-command*) (p/process (into *spawn-command* (vec (or argv [])))
                                         {:in :pipe :out :pipe :err :pipe})
    :else (default-spawn argv)))

(defonce ^:private running-procs (atom {})) ; ck -> {:proc proc :send! send! :stdin-writer writer}

(defn- channel-key [channel]
  (System/identityHashCode channel))

(defn- start-streaming! [proc send! ck]
  (future
    (try
      (with-open [r (BufferedReader. (InputStreamReader. (:out proc)))]
        (loop []
          (when-let [line (.readLine r)]
            (send! {:type "stdout" :data (b64-encode (str line "\n"))})
            (recur))))
      (catch Exception _)))

  (future
    (try
      (with-open [r (BufferedReader. (InputStreamReader. (:err proc)))]
        (loop []
          (when-let [line (.readLine r)]
            (send! {:type "stderr" :data (b64-encode (str line "\n"))})
            (recur))))
      (catch Exception _)))

  (future
    (try
      (let [finished (p/check proc)
            code (:exit finished)]
        (send! {:type "exit" :code (long code)})
        (swap! running-procs dissoc ck))
      (catch Exception e
        (send! {:type "error" :message (.getMessage e)})))))

(defn- run-subprocess! [argv send!]
  (let [ck (channel-key send!)
        proc (effective-spawn argv)
        stdin-writer (PrintWriter. (:in proc) true)]
    (swap! running-procs assoc ck {:proc proc :send! send! :stdin-writer stdin-writer})
    (start-streaming! proc send! ck)
    ck))

(defn- send-stdin! [channel data]
  (let [ck (channel-key channel)]
    (when-let [w (:stdin-writer (get @running-procs ck))]
      (try
        (.println w data)
        (catch Exception _)))))

(defn- close-stdin! [channel]
  (let [ck (channel-key channel)]
    (when-let [{:keys [stdin-writer]} (get @running-procs ck)]
      (try (.close stdin-writer) (catch Exception _)))
    (swap! running-procs update ck dissoc :stdin-writer)))

(defn- kill-proc! [channel]
  (let [ck (channel-key channel)]
    (when-let [{:keys [proc]} (get @running-procs ck)]
      (try (p/destroy proc) (catch Exception _)))
    (swap! running-procs dissoc ck)))

(defn receive-line!
  "Handle one client JSON frame on `channel`. `send!` is invoked with wire maps."
  [channel line send!]
  (try
    (let [msg (json/parse-string line true)]
      (case (:type msg)
        "start"
        (do
          (kill-proc! channel)
          (if *spawn-command*
            (run-subprocess! (:argv msg) send!)
            (run-argv! (:argv msg) (:cwd msg) send!)))

        "stdin"
        (send-stdin! channel
                     (String. (.decode (java.util.Base64/getDecoder) (:data msg))))

        "stdin-close"
        (close-stdin! channel)

        (send-error! send! (str "unknown frame type: " (:type msg)))))
    (catch Exception e
      (send-error! send! (.getMessage e)))))

(defn disconnect! [channel]
  (kill-proc! channel))