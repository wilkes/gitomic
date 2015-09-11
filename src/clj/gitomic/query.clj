(ns gitomic.query
  (:require [datomic.api :as d :refer [q]]
            [clojure.pprint :refer [pprint]]
            [gitomic.stats :as stats]))

(defn repo-by-name [db n]
  (:db/id (d/entity db [:repo/name n])))

(defn count-commits [db r]
  (q '[:find (count ?c) .
       :in $ ?r
       :where [?r :repo/commits ?c]]
     db r))

(defn commit-by-sha [db sha]
  (q '[:find ?c .
       :in $ ?sha
       :where [?c :commit/sha ?sha]]
     db sha))

(defn commits-by-path [db r path]
  (q '[:find [?c ...]
       :in $ ?r ?p
       :where
       [?f :file/path ?p]
       [?r :repo/files ?f]
       [?ch :change/file ?f]]
     db r path))

(defn files-in-repo [db repo]
  (q '[:find [?f ...]
       :in $ ?r
       :where
       [?r :repo/files ?f]]
     db repo))

(defn weight [max n]
  (float (/ n max)))

(defn weighted-tuple [xs pos max]
  (mapv (fn [x]
          (concat x (weight max (nth x pos))))
        xs))

(defn churn [db r]
  (q '[:find ?p (count ?ch)
       :in $ ?r
       :where
       [?r :repo/files ?f]
       [?f :file/path ?p]
       [?ch :change/file ?f]]
     db r))

(defn files-per-commit-stats [db repo-name]
  (stats/basic-stats
    (filter #(< % 10)
            (map second
                 (q '[:find ?c (count ?ch)
                      :in $ ?r-name
                      :where
                      [?r :repo/name ?r-name]
                      [?r :repo/commits ?c]
                      [?c :commit/changes ?ch]]
                    db repo-name)))))

(defn commits-by-author [db repo-name]
  (reverse (sort-by second (q '[:find ?n (count ?c)
                                :in $ ?r-name
                                :where
                                [?r :repo/name ?r-name]
                                [?r :repo/commits ?c]
                                [?c :commit/author ?a]
                                [?a :person/name ?n]]
                              db repo-name))))

(defn change-maps [db repo-name]
  (let [results (q '[:find ?sha ?time ?an ?ae ?subject ?p ?added ?deleted ?lines
                     :in $ ?r-name
                     :where
                     [?r :repo/name ?r-name]
                     [?r :repo/commits ?c]
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
                   db repo-name)
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

;; Left for example purposes
(defn change-maps-pull [db repo-name]
  ;?sha ?time ?an ?ae ?subject ?p ?added ?deleted ?lines
  (q '[:find [(pull ?ch [{:commit/_changes [{:commit/author [:person/name :person/email]}
                                            :commit/sha
                                            :commit/time
                                            :commit/subject]}
                         (default :change/added 0)
                         (default :change/deleted 0)
                         {:change/file [:file/path]}])
              ...]
       :in $ ?r-name
       :where
       [?r :repo/name ?r-name]
       [?r :repo/commits ?c]
       [?c :commit/changes ?ch]]
     db repo-name))

;; for query example ONLY.  Could run FOREVER on big commits
(defn file-commit-pair [db r file-path]
  (q '[:find ?p2 (count ?c)
       :in $ ?r ?p
       :where
       [?f1 :file/path ?p]
       [?ch1 :change/file ?f1]
       [?c :commit/changes ?ch1]
       [?r :repo/commits ?c]
       [?c :commit/changes ?ch2]
       [(!= ?ch1 ?ch2)]
       [?ch2 :change/file ?f2]
       [?f2 :file/path ?p2]]
     db r file-path))

(defn loc-for-repo [db repo-name]
  (map (fn [m] (merge {:file/path (:file/path m)}
                      (-> m :loc/_file first)))
       (q '[:find [(pull ?f [:file/path {:loc/_file [:loc/comment
                                                     :loc/language
                                                     :loc/blank
                                                     :loc/code]}])
                   ...]
            :in $ ?repo-name
            :where
            [?r :repo/name ?repo-name]
            [?r :repo/files ?f]
            [_ :loc/file ?f]]
          db repo-name)))