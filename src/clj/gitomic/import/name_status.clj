(ns gitomic.import.name-status
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as string]
            [gitomic.datomic :as gd]
            [datomic.api :as d :refer [q]]
            [clojure.pprint :refer [pprint]])
  (:import [java.io StringReader]))

(defn git-log-name-status [d]
  (-> (sh "git"
          "log"
          "--pretty=format:%H"
          "--name-status"
          "--no-merges"
          :dir d)
      :out
      (StringReader.)
      io/reader
      line-seq))

(def change-types
  {"A" :change/add
   "C" :change/copy
   "D" :change/delete
   "M" :change/modify
   "R" :change/rename})

(defn strip-empty-commits [c]
  (flatten (reverse (take 2 (reverse
                              (partition-by #(re-matches #"^[0-9a-f]+" %) c))))))

(defn parse-commit [c]
  (let [[sha & files] (strip-empty-commits c)]
    (remove nil?
            (map (fn [f]
                   (let [[t path] (string/split f #"\t")
                         type (get change-types t)]
                     (if (nil? type)
                       (println sha path t)
                       {:commit/sha sha
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

(defn change-type-facts [db change-lookup changes]
  (remove nil?
          (map (fn [ch]
                 (try
                   (let [ch-id (get-in change-lookup [(:commit/sha ch) (:file/path ch)])]
                     (if (nil? ch-id)
                       (println (pr-str [(:commit/sha ch) (:file/path ch)]))
                       {:db/id ch-id
                        :change/type (:change/type ch)}))
                   (catch Exception _
                     (pprint ch))))
               changes)))

(defn run [repo-name repo-path]
  (let [db (gd/db repo-name)]
    (gd/tx (gd/connect repo-name)
           (change-type-facts db (change-file-lookup db) (partition-log repo-path)))
    (println "Finished name-status update")))