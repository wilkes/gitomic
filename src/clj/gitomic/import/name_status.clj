(ns gitomic.import.name-status
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as string]
            [gitomic.datomic :as gd]
            [datomic.api :as d :refer [q]]
            [clojure.pprint :refer [pprint]]
            [clojure.edn :as edn])
  (:import [java.io StringReader]))

(defn git-log-name-status [d]
  (-> (sh "git"
          "log"
          "--pretty=format:[\"%H\" #inst \"%ad\"]"
          "--date=iso-strict"
          "--name-status"
          "--no-merges"
          :dir d)
      :out
      (StringReader.)
      io/reader
      line-seq))

(def change-types
  {"A" :change.type/add
   "C" :change.type/copy
   "D" :change.type/delete
   "M" :change.type/modify
   "R" :change.type/rename})

(defn strip-empty-commits [c]
  (flatten (reverse (take 2 (reverse
                              (partition-by #(re-matches #"^\[" %) c))))))

(defn parse-commit [c]
  (let [[commit-info & files] (strip-empty-commits c)
        [sha commit-time] (edn/read-string commit-info)]
    (remove nil?
            (map (fn [f]
                   (let [[t path] (string/split f #"\t")
                         type (get change-types t)]
                     (if (nil? type)
                       (println sha path t)
                       {:commit/sha sha
                        :commit/time commit-time
                        :file/path path
                        :change/type (get change-types t)})))
                 files))))

(defn partition-log [repo-path]
  (->> (git-log-name-status repo-path)
       (partition-by empty?)
       (remove (comp empty? first))
       (map parse-commit)
       flatten))

(defn change-file-lookup [db]
  (reduce (fn [results [sha path change-id]]
            (assoc-in results [sha path] change-id))
          {}
       (q '[:find ?sha ?path ?ch
            :in $
            :where
            [?c :commit/sha ?sha]
            [?c :commit/changes ?ch]
            [?ch :change/file ?f]
            [?f :file/path ?path]]
          db)))

(defn change-type-facts [db change-lookup commit]
  (try
    (let [ch-id (get-in change-lookup [(:commit/sha commit) (:file/path commit)])]
      (if (nil? ch-id)
        (println (pr-str [(:commit/sha commit) (:file/path commit)]))
        [{:db/id ch-id
          :change/type (:change/type commit)}
         {:db/id (d/tempid :db.part/tx)
          :tx/sha (:commit/sha commit)
          :commit/time (-> commit :commit/time)}]))
    (catch Exception _
      (pprint commit))))

(defn run [repo-name repo-path]
  (let [db (gd/db repo-name)]
    (doseq [commit (partition-log repo-path)
            :let [commit-name-status (change-type-facts db (change-file-lookup db) commit)]]
      (when-not (nil? commit-name-status))
      (gd/tx (gd/connect repo-name) commit-name-status))
    (println "Finished name-status update")))