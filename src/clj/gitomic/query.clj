(ns gitomic.query
  (:require [datomic.api :as d :refer [q]]
            [gitomic.datomic :as dtm]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]))

(defn repo-by-name [db n]
  (q '[:find ?r .
       :in $ ?n
       :where [?r :repo/name ?n]]
     db n))

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