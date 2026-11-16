(ns isaac.cli-server.local-spec
  (:require
    [clojure.edn :as edn]
    [isaac.cli-server.local :as sut]
    [isaac.foundation.fs :as fs]
    [speclj.core :refer :all]))

(describe "local CLI server discovery"
  (it "publishes a new private bearer and removes it on stop"
    (let [root (str (java.nio.file.Files/createTempDirectory "cli-local" (make-array java.nio.file.attribute.FileAttribute 0)))
          path (str root "/state/cli-server.edn")]
      (try
        (sut/publish! root (fs/real-fs) 4321)
        (let [{:keys [url token]} (edn/read-string (slurp path))]
          (should= "ws://127.0.0.1:4321/cli" url)
          (should (<= 20 (count token)))
          (should= 0600 (java.lang.Integer/parseInt (format "%o" (reduce (fn [m [p bit]]
                          (if (contains? (java.nio.file.Files/getPosixFilePermissions (.toPath (java.io.File. path)) (make-array java.nio.file.LinkOption 0)) p) (+ m bit) m)) 0
                          [[java.nio.file.attribute.PosixFilePermission/OWNER_READ 0400]
                           [java.nio.file.attribute.PosixFilePermission/OWNER_WRITE 0200]])) 8))
          (should= {:name :local-cli :scopes #{:cli}} (sut/verify {:uri "/cli" :headers {"authorization" (str "Bearer " token)}}))
          (should-be-nil (sut/verify {:uri "/cli" :headers {"authorization" "Bearer wrong"}}))
          (sut/publish! root (fs/real-fs) 4322)
          (should-not= token (:token (edn/read-string (slurp path)))))
        (finally
          (sut/remove! root (fs/real-fs))
          (should-not (.exists (java.io.File. path))))))))
