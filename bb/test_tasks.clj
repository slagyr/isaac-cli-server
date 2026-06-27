(ns bb.test-tasks
  (:require
    [babashka.fs :as fs]
    [babashka.process :as process]
    [gherclj.main :as gherclj]
    [speclj.main :as speclj]))

(defn- clean! []
  (fs/delete-tree "target")
  (println "Cleaned target/"))

(defn run-spec! [& args]
  (if (seq args)
    (apply speclj/-main "-c" "-D" "spec" args)
    (speclj/-main "-c" "-D" "spec")))

(defn run-features! [& args]
  (clean!)
  (apply gherclj/-main
    (concat ["-f" "features"]
            ["-s" "isaac.**-steps" "-t" "~slow" "-t" "~wip"]
            args)))

(defn- check-exit! [{:keys [exit]}]
  (when (pos? exit)
    (System/exit exit)))

(defn run-ci! []
  ;; speclj/gherclj call System/exit; subprocesses keep ci alive between suites.
  (check-exit! (if (seq *command-line-args*)
                 (apply process/shell "bb" "spec" *command-line-args*)
                 (process/shell "bb" "spec")))
  (check-exit! (process/shell "bb" "features")))