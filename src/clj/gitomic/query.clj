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

(defn file-by-path [db p]
  (q '[:find ?f .
       :in $ ?p
       :where [?f :file/path ?p]]
     db p))

(defn commits-by-file-path [db r path]
  (q '[:find [?c ...]
       :in $ ?r ?p
       :where
       [?f :file/path ?p]
       [?c :commit/files ?f]
       [?c :commit/merge? false]]
     db r path))

(defn files-in-repo [db repo]
  (q '[:find [?f ...]
       :in $ ?r
       :where
       [?r :repo/commits ?c]
       [?c :commit/files ?f]]
     db repo))

(defn churn-on-path [db path]
  (q '[:find [?c ...]
       :in $ ?p
       :where
       [?f :file/path ?p]
       [?c :commit/files ?f]
       [?c :commit/merge? false]]
     db path))

(defn churn [db r]
  (q '[:find ?n (count ?c)
       :in $ ?r
       :where
       [?r :repo/commits ?c]
       [?c :commit/files ?f]
       [?c :commit/merge? false]
       [?f :file/path ?n]]
     db r))