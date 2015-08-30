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
       [?ch :change/path ?f]
       [?c :commit/changes ?ch]
       [?c :commit/merge? false]
       [?r :repo/commits ?c]]
     db r path))

(defn files-in-repo [db repo]
  (q '[:find [?f ...]
       :in $ ?r
       :where
       [?r :repo/commits ?c]
       [?c :commit/changes ?ch]
       [?ch :change/path ?f]]
     db repo))

(defn weight [max n]
  (float (/ n max)))

(defn churn [db r]
  (let [pairs (reverse
                (sort-by second
                         (q '[:find ?p (count ?c)
                              :in $ ?r
                              :where
                              [?r :repo/commits ?c]
                              [?c :commit/merge? false]
                              [?c :commit/changes ?ch]
                              [?ch :change/path ?f]
                              [?f :file/path ?p]
                              ;[(.endsWith ?p ".rb")]
                              ]
                            db r)))
        m (-> pairs first second)]
    (mapv (fn [[p n]]
            [p n (weight m n)])
          pairs)))

(defn file-commit-pair [db r file-path]
  (reverse
    (sort-by second
                     (q '[:find ?p2 (count ?c)
                                 :in $ ?r ?p
                                 :where
                                 [?f1 :file/path ?p]
                                 [?ch1 :change/path ?f1]x
                                 [?c :commit/changes ?ch1]
                                 [?r :repo/commits ?c]
                                 [?c :commit/changes ?ch2]
                                 [(!= ?ch1 ?ch2)]
                                 [?ch2 :change/path ?f2]
                                 [?f2 :file/path ?p2]
                          ;[(.endsWith ?p2 ".rb")]
                                 ]
                               db r file-path))))