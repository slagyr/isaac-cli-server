(ns isaac.cli-server.dispatch
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [ring.util.codec :as codec])
  (:import
    (java.io BufferedReader InputStreamReader PrintWriter)))

(def ^:dynamic *spawn-process* nil)

(defonce ^:private running-procs (atom {}))

(defn- b64-encode [^String s]
  (when (seq s)
    (codec/base64-encode (.getBytes s "UTF-8"))))

(defn- send-frame! [send! frame]
  (send! frame))

(defn- send-error! [send! message]
  (send-frame! send! {:type "error" :message message}))

(defn- spawn-options []
  {:in :pipe :out :pipe :err :pipe})

(defn- launcher-command [argv]
  (into ["isaac"] (vec (or argv []))))

(defn- start-process! [argv]
  (let [command (launcher-command argv)]
    (if *spawn-process*
      (*spawn-process* command)
      (p/process command (spawn-options)))))

(defn- channel-key [channel]
  (System/identityHashCode channel))

(defn- stream-frames! [stream type send!]
  (future
    (try
      (with-open [reader (BufferedReader. (InputStreamReader. stream))]
        (loop []
          (when-let [line (.readLine reader)]
            (send-frame! send! {:type type :data (b64-encode (str line "\n"))})
            (recur))))
      (catch Exception _))))

(defn- await-exit! [proc send! ck stdout-f stderr-f]
  (future
    (try
      (let [process ^Process (:proc proc)]
        (.waitFor process)
        @stdout-f
        @stderr-f
        (send-frame! send! {:type "exit" :code (long (.exitValue process))})
        (swap! running-procs dissoc ck))
      (catch Exception e
        (send-error! send! (.getMessage e))))))

(defn- start-streaming! [proc send! ck]
  (let [stdout-f (stream-frames! (:out proc) "stdout" send!)
        stderr-f (stream-frames! (:err proc) "stderr" send!)]
    (await-exit! proc send! ck stdout-f stderr-f)))

(defn- run-subprocess! [channel argv send!]
  (let [ck           (channel-key channel)
        proc         (start-process! argv)
        stdin-writer (PrintWriter. (:in proc) true)]
    (swap! running-procs assoc ck {:proc proc :stdin-writer stdin-writer})
    (start-streaming! proc send! ck)
    ck))

(defn- send-stdin! [channel data]
  (let [ck (channel-key channel)]
    (when-let [writer (:stdin-writer (get @running-procs ck))]
      (try
        (.println writer data)
        (catch Exception _)))))

(defn- close-stdin! [channel]
  (let [ck (channel-key channel)]
    (when-let [{:keys [stdin-writer]} (get @running-procs ck)]
      (try
        (.close stdin-writer)
        (catch Exception _)))
    (swap! running-procs update ck dissoc :stdin-writer)))

(defn- kill-proc! [channel]
  (let [ck (channel-key channel)]
    (when-let [{:keys [proc]} (get @running-procs ck)]
      (try
        (p/destroy proc)
        (catch Exception _)))
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
          (run-subprocess! channel (:argv msg) send!))

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
