#!/usr/bin/env bb
;; Flags production namespaces that read Isaac config content via raw slurp +
;; edn/read-string instead of isaac.config.loader / isaac.config.api.
;;
;; Canonical copy: isaac-foundation/config_bypass_lint.bb
;; Each Isaac module repo vendors an identical copy for standalone CI.
;;
;; Usage: bb config_bypass_lint.bb [src-dir ...]
;; Default target: src/

(require '[babashka.fs :as fs]
         '[clojure.java.io :as io]
         '[clojure.string :as str])

(def default-targets ["src"])

(def allowed-ns-prefixes
  #{"isaac.config."})

(def allowed-exact-ns
  #{"isaac.cli.registry"})

(def suspicious-patterns
  [#"config/isaac\.edn"
   #"root-config-file"
   #"paths/config-path"
   #"\"/config/"
   #"\.edn\""])

(defn- ns-from-file [path content]
  (some->> content
           (str/split-lines)
           (filter #(str/starts-with? % "(ns "))
           first
           (subs 4)
           (str/trim)
           (str/split #" ")
           first))

(defn- allowed-ns? [ns-name]
  (or (contains? allowed-exact-ns ns-name)
      (some #(str/starts-with? ns-name %) allowed-ns-prefixes)))

(defn- reads-config-content? [content]
  (and (re-find #"edn/read-string" content)
       (or (re-find #"fs/slurp" content)
           (re-find #"\(slurp " content))
       (some #(re-find % content) suspicious-patterns)))

(defn- lint-file [path]
  (let [content (slurp path)
        ns-name (ns-from-file path content)]
    (when (and ns-name (not (allowed-ns? ns-name)) (reads-config-content? content))
      {:path path :ns ns-name})))

(defn- collect-clj-files [dir]
  (->> (fs/list-dir dir {:recursive true})
       (filter #(str/ends-with? (:name %) ".clj"))
       (map :path)
       vec))

(defn -main [& args]
  (let [targets (if (seq args) args default-targets)
        files   (mapcat collect-clj-files targets)
        hits    (vec (keep lint-file files))]
    (doseq [{:keys [path ns]} hits]
      (println (str path ": " ns " reads config content outside isaac.config.*")))
    (if (seq hits)
      (do (println (str "\nconfig-bypass-lint: " (count hits) " violation(s)"))
          (System/exit 1))
      (println "config-bypass-lint: ok"))))

(apply -main *command-line-args*)