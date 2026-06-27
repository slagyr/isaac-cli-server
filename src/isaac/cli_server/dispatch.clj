(ns isaac.cli-server.dispatch
  (:require
    [cheshire.core :as json]
    [isaac.main :as main]
    [ring.util.codec :as codec])
  (:import
    (java.io ByteArrayOutputStream PrintWriter)))

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

(defonce ^:private stdin-buffers (atom {}))

(defn- channel-key [channel]
  (System/identityHashCode channel))

(defn- append-stdin! [channel chunk]
  (swap! stdin-buffers update (channel-key channel) str (or chunk "")))

(defn receive-line!
  "Handle one client JSON frame on `channel`. `send!` is invoked with wire maps."
  [channel line send!]
  (try
    (let [msg (json/parse-string line true)]
      (case (:type msg)
        "start"
        (do
          (swap! stdin-buffers dissoc (channel-key channel))
          (run-argv! (:argv msg) (:cwd msg) send!))

        "stdin"
        (append-stdin! channel
                       (String. (.decode (java.util.Base64/getDecoder) (:data msg))))

        "stdin-close"
        (swap! stdin-buffers dissoc (channel-key channel))

        (send-error! send! (str "unknown frame type: " (:type msg)))))
    (catch Exception e
      (send-error! send! (.getMessage e)))))