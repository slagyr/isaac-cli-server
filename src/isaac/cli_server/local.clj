(ns isaac.cli-server.local
  "Private discovery credential for this server's /cli endpoint."
  (:require
    [clojure.string :as str]
    [isaac.foundation.component.factory :as factory]
    [isaac.foundation.component.protocol :as component]
    [isaac.foundation.component.registry :as registry]
    [isaac.foundation.fs :as fs])
  (:import
    (java.nio.file Files OpenOption StandardOpenOption)
    (java.nio.file.attribute PosixFilePermission)
    (java.security MessageDigest SecureRandom)
    (java.util Base64)))

(defonce ^:private active-token (atom nil))

(defn- path [root] (str root "/state/cli-server.edn"))

(defn- random-token []
  (let [bytes (byte-array 32)]
    (.nextBytes (SecureRandom.) bytes)
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes)))

(defn publish! [root fs* port]
  (let [token (random-token)
        file  (path root)]
    (fs/mkdirs fs* (str root "/state"))
    (if (instance? isaac.foundation.fs.RealFs fs*)
      (let [p (.toPath (java.io.File. file))
            perms #{PosixFilePermission/OWNER_READ PosixFilePermission/OWNER_WRITE}]
        ;; CREATE_NEW plus mode-600 attributes avoids a world-readable creation window.
        (Files/deleteIfExists p)
        (let [attr (java.nio.file.attribute.PosixFilePermissions/asFileAttribute perms)]
          (Files/createFile p (into-array java.nio.file.attribute.FileAttribute [attr])))
        (Files/write p (.getBytes (pr-str {:url (str "ws://127.0.0.1:" port "/cli") :token token}) "UTF-8")
                     (into-array OpenOption [StandardOpenOption/WRITE StandardOpenOption/TRUNCATE_EXISTING])))
      (fs/spit fs* file (pr-str {:url (str "ws://127.0.0.1:" port "/cli") :token token})))
    (reset! active-token token)))

(defn remove! [root fs*]
  (reset! active-token nil)
  (fs/delete fs* (path root)))

(defn verify [request]
  (let [provided (some-> (get-in request [:headers "authorization"])
                         (str/replace-first #"(?i)^Bearer\s+" ""))
        expected @active-token]
    (when (and (= "/cli" (:uri request))
               expected provided
               (MessageDigest/isEqual (.getBytes ^String expected "UTF-8")
                                      (.getBytes ^String provided "UTF-8")))
      {:name :local-cli :scopes #{:cli}})))

(defn register-identity! []
  ((requiring-resolve 'isaac.http.auth/register-identity-verifier!)
   (fn [request] (verify request))))

(deftype LocalServer [root fs*]
  component/Component
  (start [this]
    (when-let [port (some-> (registry/instance-for :http) component/bound-port)]
      (publish! root fs* port))
    this)
  (stop [this]
    (remove! root fs*)
    this))

(defmethod factory/create :cli-server-local [_ {:keys [root opts]}]
  (->LocalServer root (or (:fs opts) (fs/real-fs))))
