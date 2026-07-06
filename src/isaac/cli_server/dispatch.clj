(ns isaac.cli-server.dispatch
  (:require
    [babashka.process :as p]
    [cheshire.core :as json]
    [ring.util.codec :as codec])
  (:import
    (java.io BufferedReader InputStreamReader PrintWriter)
    (java.util UUID)))

(def ^:dynamic *spawn-process* nil)
(def ^:dynamic *stream-id-factory* #(str (UUID/randomUUID)))
(def ^:dynamic *grace-period-ms* 2000)
(def ^:dynamic *schedule-grace-timeout*
  (fn [delay-ms f]
    (future
      (Thread/sleep delay-ms)
      (f))))
(def ^:dynamic *cancel-grace-timeout*
  (fn [token]
    (when (future? token)
      (future-cancel token))))

(defonce ^:private streams (atom {}))
(defonce ^:private channel->stream-id (atom {}))

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

(defn- bind-channel! [channel stream-id]
  (swap! channel->stream-id assoc (channel-key channel) stream-id))

(defn- unbind-channel! [channel]
  (swap! channel->stream-id dissoc (channel-key channel)))

(defn- stream-id-for-channel [channel]
  (get @channel->stream-id (channel-key channel)))

(defn- stream-state [stream-id]
  (get @streams stream-id))

(declare buffer-frame!)

(defn- send-live-frame! [stream-id frame]
  (when-let [send! (:send! (stream-state stream-id))]
    (try
      (send-frame! send! frame)
      (catch Exception _
        (buffer-frame! stream-id frame)))))

(defn- buffer-frame! [stream-id frame]
  (swap! streams update-in [stream-id :buffer] (fnil conj []) frame))

(defn- attached? [stream-id]
  (boolean (:send! (stream-state stream-id))))

(defn- clear-grace-token! [stream-id]
  (when-let [task (:grace-task (stream-state stream-id))]
    (*cancel-grace-timeout* task))
  (swap! streams update stream-id assoc :grace-task nil :grace-token nil))

(defn- destroy-stream! [stream-id]
  (when-let [{:keys [proc stdin-writer channel]} (stream-state stream-id)]
    (try
      (when stdin-writer
        (.close ^PrintWriter stdin-writer))
      (catch Exception _))
    (try
      (p/destroy proc)
      (catch Exception _))
    (when channel
      (unbind-channel! channel))
    (swap! streams dissoc stream-id)))

(defn- expire-grace! [stream-id token]
  (let [{:keys [grace-token send! exited?]} (stream-state stream-id)]
    (when (and (= token grace-token) (nil? send!))
      (if exited?
        (swap! streams dissoc stream-id)
        (destroy-stream! stream-id)))))

(defn- schedule-grace-expiry! [stream-id]
  (let [delay-ms (long (max 0 (or *grace-period-ms* 0)))
        token    (Object.)]
    (if (zero? delay-ms)
      (expire-grace! stream-id token)
      (let [task (*schedule-grace-timeout* delay-ms #(expire-grace! stream-id token))]
        (swap! streams assoc-in [stream-id :grace-token] token)
        (swap! streams assoc-in [stream-id :grace-task] task)))))

(defn- disconnect-stream! [stream-id]
  (when-let [{:keys [channel exited?]} (stream-state stream-id)]
    (when channel
      (unbind-channel! channel))
    (swap! streams update stream-id assoc :channel nil :send! nil :grace-task nil)
    (if exited?
      (swap! streams dissoc stream-id)
      (schedule-grace-expiry! stream-id))))

(defn- route-frame! [stream-id frame]
  (if (attached? stream-id)
    (send-live-frame! stream-id frame)
    (buffer-frame! stream-id frame)))

(defn- stream-frames! [stream-id stream type]
  (future
    (try
      (with-open [reader (BufferedReader. (InputStreamReader. stream))]
        (loop []
          (when-let [line (.readLine reader)]
            (route-frame! stream-id {:type type :data (b64-encode (str line "\n"))})
            (recur))))
      (catch Exception _))))

(defn- await-exit! [stream-id proc stdout-f stderr-f]
  (future
    (try
      (let [process    ^Process (:proc proc)
            exit-frame (do
                         (.waitFor process)
                         @stdout-f
                         @stderr-f
                         {:type "exit" :code (long (.exitValue process))})]
        (swap! streams assoc-in [stream-id :exited?] true)
        (route-frame! stream-id exit-frame)
        (when (attached? stream-id)
          (swap! streams dissoc stream-id)))
      (catch Exception e
        (when-let [send! (:send! (stream-state stream-id))]
          (send-error! send! (.getMessage e)))))))

(defn- start-streaming! [stream-id proc]
  (let [stdout-f (stream-frames! stream-id (:out proc) "stdout")
        stderr-f (stream-frames! stream-id (:err proc) "stderr")]
    (await-exit! stream-id proc stdout-f stderr-f)))

(defn- start-stream! [channel argv send!]
  (when-let [existing-stream-id (stream-id-for-channel channel)]
    (destroy-stream! existing-stream-id))
  (let [stream-id    (*stream-id-factory*)
        proc         (start-process! argv)
        stdin-writer (PrintWriter. (:in proc) true)]
    (swap! streams assoc stream-id {:buffer       []
                                    :channel      channel
                                    :exited?      false
                                    :grace-task   nil
                                    :grace-token  nil
                                    :proc         proc
                                    :send!        send!
                                    :stdin-writer stdin-writer
                                    :stream-id    stream-id})
    (bind-channel! channel stream-id)
    (send-frame! send! {:type "start-ack" :stream-id stream-id})
    (start-streaming! stream-id proc)
    stream-id))

(defn- attach-stream! [channel stream-id send!]
  (if-let [{:keys [buffer exited?]} (stream-state stream-id)]
    (do
      (clear-grace-token! stream-id)
      (swap! streams update stream-id assoc :buffer [] :channel channel :send! send! :grace-task nil)
      (bind-channel! channel stream-id)
      (doseq [frame buffer]
        (send-frame! send! frame))
      (when exited?
        (swap! streams dissoc stream-id)))
    (send-error! send! (str "unknown stream-id: " stream-id))))

(defn- send-stdin! [channel data]
  (when-let [stream-id (stream-id-for-channel channel)]
    (when-let [writer (:stdin-writer (stream-state stream-id))]
      (try
        (.println ^PrintWriter writer data)
        (catch Exception _)))))

(defn- close-stdin! [channel]
  (when-let [stream-id (stream-id-for-channel channel)]
    (when-let [stdin-writer (:stdin-writer (stream-state stream-id))]
      (try
        (.close ^PrintWriter stdin-writer)
        (catch Exception _))
      (swap! streams update stream-id dissoc :stdin-writer))))

(defn receive-line!
  "Handle one client JSON frame on `channel`. `send!` is invoked with wire maps."
  [channel line send!]
  (try
    (let [msg (json/parse-string line true)]
      (case (:type msg)
        "start"
        (start-stream! channel (:argv msg) send!)

        "attach"
        (attach-stream! channel (:stream-id msg) send!)

        "stdin"
        (send-stdin! channel
                     (String. (.decode (java.util.Base64/getDecoder) (:data msg))))

        "stdin-close"
        (close-stdin! channel)

        (send-error! send! (str "unknown frame type: " (:type msg)))))
    (catch Exception e
      (send-error! send! (.getMessage e)))))

(defn disconnect! [channel]
  (when-let [stream-id (stream-id-for-channel channel)]
    (disconnect-stream! stream-id)))