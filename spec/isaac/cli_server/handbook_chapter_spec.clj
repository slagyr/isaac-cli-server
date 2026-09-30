(ns isaac.cli-server.handbook-chapter-spec
  "Lint for isaac-cli-server's own handbook chapter (isaac-erjv): every
   backtick `config:<dotted.path>` reference (no angle-bracket placeholder
   inside the path) must resolve against the composed config schema, and
   the word right after `isaac ` in every `isaac <command>` invocation
   must name a registered top-level CLI command. Keep both literal and
   real when you write one — this lint fails the build once either drifts
   from what Isaac actually exposes. `<placeholder>` shapes (e.g.
   `config:<dotted.path>` itself, or `<module-id>#<slug>`) are
   intentionally skipped. See isaac.foundation.handbook-chapter-spec for
   the pattern this follows.

   isaac-cli-server's own manifest does not declare :builtin? true, and
   this bean intentionally does not add it (:builtin? also controls eager
   module loading — a behavior change outside this bean's ungated scope,
   per isaac-imessage's handbook-chapter-lint precedent). Instead this
   spec builds its own module index by reading the raw isaac-manifest.edn
   classpath resource directly and merging it into
   isaac.module.discovery/builtin-index — enough for schema-compose to see
   this module's :cli-server config contribution, without touching how the
   module loads at runtime."
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [isaac.config.schema-compose :as schema-compose]
    [isaac.config.schema.resolve :as schema-resolve]
    [isaac.fs :as fs]
    [isaac.module.discovery :as discovery]
    [isaac.nexus :as nexus]
    [speclj.core :refer :all]))

(def ^:private chapter-resource "isaac/cli_server/handbook.md")
(def ^:private manifest-resource "isaac-manifest.edn")

(defn- chapter-text []
  (some-> (io/resource chapter-resource) slurp))

(defn- own-manifest []
  (some-> (io/resource manifest-resource) slurp edn/read-string))

(defn- own-module-index
  "Raw-manifest entry for this module, keyed by its :id — merged into
   discovery/builtin-index for schema composition (see ns docstring)."
  []
  (when-let [manifest (own-manifest)]
    {(:id manifest) {:coord {} :manifest manifest :path nil}}))

(defn- full-index []
  (merge (discovery/builtin-index) (own-module-index)))

(defn- config-refs
  "Backtick `config:<path>` references in `text`, skipping `<placeholder>`
   shapes (any reference whose path still contains an angle bracket)."
  [text]
  (->> (re-seq #"`config:([^`]+)`" text)
       (map second)
       (remove #(str/includes? % "<"))
       distinct))

(defn- cli-commands-mentioned
  "The word immediately following `isaac ` wherever it appears — inline
   code, fenced examples, or plain prose — for every top-level `isaac
   <command>` invocation in `text`."
  [text]
  (->> (re-seq #"isaac\s+([a-zA-Z][a-zA-Z0-9_-]*)" text)
       (map second)
       distinct))

(defn- known-cli-commands
  "Top-level command names contributed to the :isaac/cli berth by every
   module in `index` — read directly off each module's manifest rather
   than through isaac.module.berths, whose report helpers vary across
   pinned foundation shas."
  [index]
  (->> (vals index)
       (mapcat (fn [entry] (keys (get-in entry [:manifest :isaac/cli]))))
       (map name)
       set))

(describe "isaac-cli-server handbook chapter (isaac-erjv)"

  (around [example] (nexus/-with-nexus {:fs (fs/real-fs)} (example)))

  (it "manifest declares the handbook resource"
    (should= chapter-resource (:handbook (own-manifest))))

  (it "ships at the manifest's declared classpath resource"
    (should-not-be-nil (chapter-text)))

  (it "every `config:<path>` reference resolves against the composed config schema"
    (let [text        (chapter-text)
          root-schema (schema-compose/effective-root-schema (full-index))
          unresolved  (remove #(schema-resolve/schema-for-data-path root-schema %)
                              (config-refs text))]
      (should= [] unresolved)))

  (it "every `isaac <command>` invocation names a registered top-level CLI command"
    (let [text    (chapter-text)
          known   (known-cli-commands (full-index))
          unknown (remove known (cli-commands-mentioned text))]
      (should= [] unknown))))
