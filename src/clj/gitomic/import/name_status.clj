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

(defn parse-commit [[sha & files]]
  (map (fn [f]
         (let [[t path] (string/split f #"\t")]
           {:commit/sha sha
            :file/path path
            :change/type (get change-types t)}))
       files))

(defn partition-log [repo-path]
  (->> (git-log-name-status repo-path)
       (remove empty?)
       (partition-by #(re-matches #"^[0-9a-f]+" %))
       (partition 2)
       (map flatten)
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
                     {:db/id ch-id
                      :change/type (:change/type ch)})
                   (catch Exception _
                     (pprint ch))))
               changes)))

(defn run [repo-name repo-path]
  (let [db (gd/db repo-name)]
    (gd/tx (gd/connect repo-name)
           (change-type-facts db (change-file-lookup db) (partition-log repo-path)))
    (println "Finished name-status update")))