(ns gitomic.query
  (:require [datomic.api :as d :refer [q]]
            [clojure.pprint :refer [pprint]]
            [gitomic.stats :as stats]
            [gitomic.datomic :as gd]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]

            ))

(defn count-commits [db]
  (q '[:find (count ?c) .
       :in $
       :where [?c :commit/sha]]
     db))

(defn all-commits [db]
  (q '[:find [?c ...]
       :in $
       :where [?c :commit/sha]]
     db))

(defn count-files [db]
  (q '[:find (count ?f) .
       :in $
       :where [?f :file/path]]
     db))

(defn all-files [db]
  (q '[:find [?f ...]
       :in $
       :where [?f :file/path]]
     db))

(defn count-authors [db]
  (q '[:find (count ?a) .
       :in $
       :where [?a :person/name]]
     db))

(defn all-authors [db]
  (q '[:find [?a ...]
       :in $
       :where [?a :person/name]]
     db))

(defn commits-by-path [db path]
  (q '[:find [?c ...]
       :in $ ?p
       :where
       [?f :file/path ?p]
       [?ch :change/file ?f]]
     db path))

(defn weight [max n]
  (float (/ n max)))

(defn weighted-tuple [xs pos max]
  (mapv (fn [x]
          (concat x (weight max (nth x pos))))
        xs))

(defn churn [db]
  (q '[:find ?p (count ?ch)
       :in $
       :where
       [?f :file/path ?p]
       [?ch :change/file ?f]]
     db))

(defn files-per-commit-stats [db]
  (stats/basic
    (filter #(< % 10)
            (map second
                 (q '[:find ?c (count ?ch)
                      :in $
                      :where
                      [?c :commit/changes ?ch]]
                    db)))))

(defn commits-by-author [db]
  (reverse (sort-by second (q '[:find ?n (count ?c)
                                :in $
                                :where
                                [?c :commit/author ?a]
                                [?a :person/name ?n]]
                              db))))

(defn non-merge-commits [db]
  (q '[:find [?c ...]
       :in $
       :where
       [?c :commit/merge? false]]
     db))

(defn change-maps [db]
  (let [results (q '[:find ?sha ?time ?an ?ae ?subject ?p ?added ?deleted ?lines
                     :in $
                     :where
                     [?c :commit/sha ?sha]
                     [?c :commit/time ?time]
                     [?c :commit/subject ?subject]
                     [?c :commit/author ?a]
                     [?a :person/name ?an]
                     [?a :person/email ?ae]
                     [?c :commit/changes ?ch]
                     [(get-else $ ?ch :change/added 0) ?added]
                     [(get-else $ ?ch :change/deleted 0) ?deleted]
                     [(- ?added ?deleted) ?lines]
                     [?ch :change/file ?f]
                     [?f :file/path ?p]]
                   db)
        mapify-result (fn [[sha time an ae subject p added deleted lines]]
                        {:commit/sha sha
                         :commit/time time
                         :person/name an
                         :person/email ae
                         :commit/subject subject
                         :file/path p
                         :change/added added
                         :change/deleted deleted
                         :change/lines lines})]
    (map mapify-result results)))

(defn authors-contribution [db]
  (q '[:find ?an (sum ?added) (sum ?deleted) (sum ?total)
       :in $
       :where
       [?c :commit/author ?a]
       [?a :person/name ?an]
       [?c :commit/changes ?ch]
       [(get-else $ ?ch :change/added 0) ?added]
       [(get-else $ ?ch :change/deleted 0) ?deleted]
       [(- ?added ?deleted) ?total]
       [?ch :change/file ?f]
       [?f :file/path ?p]
       (or
         [(.startsWith ?p "app/")]
         [(.startsWith ?p "lib/")])]
     db))

(defn file-age [db]
  (q '[:find ?p (max ?t)
       :where
       [?f :loc/language _]
       [?f :file/path ?p]
       [?ch :change/file ?f]
       [?c :commit/changes ?ch]
       [?c :commit/time ?t]]
     db))

;; Left for example purposes
(defn change-maps-pull [db]
  ;?sha ?time ?an ?ae ?subject ?p ?added ?deleted ?lines
  (q '[:find [(pull ?ch [{:commit/_changes [{:commit/author [:person/name :person/email]}
                                            :commit/sha
                                            :commit/time
                                            :commit/subject]}
                         (default :change/added 0)
                         (default :change/deleted 0)
                         {:change/file [:file/path]}])
              ...]
       :in $
       :where
       [?c :commit/changes ?ch]]
     db))

;; for query example ONLY.  Could run FOREVER on big commits
(defn file-commit-pair [db file-path]
  (q '[:find ?p2 (count ?c)
       :in $ ?p
       :where
       [?f1 :file/path ?p]
       [?ch1 :change/file ?f1]
       [?c :commit/changes ?ch1]
       [?c :commit/changes ?ch2]
       [(!= ?ch1 ?ch2)]
       [?ch2 :change/file ?f2]
       [?f2 :file/path ?p2]]
     db file-path))

(defn loc [db]
  (q '[:find [(pull ?f [*]) ...]
       :in $
       :where
       [?f :loc/code]]
     db))

(defn count-loc-entities [db]
  (q '[:find (count ?loc) .
       :in $
       :where [?loc :loc/code]]
     db))

(defn count-change-entities [db]
  (q '[:find (count ?loc) .
       :in $
       :where [?loc :change/upsert-id]]
     db))

(defn summary [db]
  {:commits (count-commits db)
   :changes (count-change-entities db)
   :files (count-files db)
   :authors (count-authors db)})

(defn tx-for-sha [db sha]
  (q '[:find ?tx .
       :in $ ?sha
       :where
       [?tx :tx/sha ?sha]]
     db sha))