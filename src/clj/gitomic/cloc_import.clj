(ns gitomic.cloc-import
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as string]
            [datomic.api :as d]
            [gitomic.datomic :as gd]))

(defn file-ref [db repo-id path]
  (d/q '[:find ?f .
         :in $ ?r ?p
         :where
         [?f :file/path ?p]
         [?r :repo/files ?f]]
       db repo-id path))

(defn line->map [s]
  (let [[lang path blank comment code] (take 5 (string/split s #","))]
    {:loc/language lang
     :loc/file (string/replace path #"\./" "")
     :loc/comment (Integer/parseInt comment)
     :loc/blank (Integer/parseInt blank)
     :loc/code (Integer/parseInt code)}))

(defn cloc [d]
  (->> (sh "cloc"
           "./"
           "--by-file"
           "--csv"
           "--quiet"
           :dir d)
       :out
       (java.io.StringReader.)
       io/reader
       line-seq
       (drop 2)
       (map line->map)))

(defn loc-facts [repo-name locs]
  (let [repo-id (:db/id (gd/ent [:repo/name repo-name]))
        db (gd/db)
        loc-fact (fn [loc]
                   (let [f (file-ref db repo-id (str repo-name "/" (:loc/file loc)))]
                     (when f
                       (assoc loc :loc/file f
                                  :db/id (gd/tid)))))]
    (remove nil? (map loc-fact locs))))

(defn run [repo-name repo-dir]
  (gd/ensure-schema (gd/connect))
  (gd/tx (gd/connect) (loc-facts repo-name (cloc repo-dir))))
