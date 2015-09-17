(ns gitomic.import.cloc
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as string]
            [datomic.api :as d]
            [gitomic.datomic :as gd])
  (:import [java.io StringReader]))

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
       (StringReader.)
       io/reader
       line-seq
       (drop 2)
       (map line->map)))

(defn loc-facts [repo-name locs]
  (let [db (gd/db repo-name)
        loc-fact (fn [loc]
                   (let [f (:db/id (gd/ent db [:file/path (:loc/file loc)]))]
                     (when f
                       (-> loc
                           (assoc :db/id f)
                           (dissoc :loc/file)))))]
    (remove nil? (map loc-fact locs))))

(defn run [repo-name repo-dir]
  (gd/ensure-schema (gd/connect repo-name))
  (gd/tx (gd/connect repo-name) (loc-facts repo-name (cloc repo-dir)))
  (println "Done"))
