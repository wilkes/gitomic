(ns gitomic.import
  (:require [clojure.pprint :refer [pprint]]
            [datomic.api :as d]
            [gitomic.datomic :as dtm]
            [gitomic.git :as git]))

(defn repo-fact [name path]
  {:db/id (dtm/tid)
   :repo/name name
   :repo/path path})

(defn person-fact [cid c-ref p]
  (assoc p
    :db/id (dtm/tid)
    c-ref cid))

(defn file-fact [path]
  {:db/id (dtm/tid)
   :file/path path})

(defn diff-facts [cid d]
  (let [new-path (file-fact (:diff/new-path d))
        old-path (file-fact (:diff/old-path d))]
    [new-path
     old-path
     (assoc d :db/id (dtm/tid)
              :commit/_diffs cid
              :diff/new-path (:db/id new-path)
              :diff/old-path (:db/id old-path))]))

(defn parent-fact [cid sha]
  {:db/id (dtm/tid)
   :commit/sha sha
   :commit/_parents cid})

(defn commit-facts [rid c]
  (let [cid (dtm/tid)]
    (-> [(-> c
               (assoc :repo/_commits rid
                      :db/id cid)
               (dissoc :commit/author :commit/parents :commit/committer :commit/diffs))
         ;(person-fact cid :commit/_author (:commit/author c))
           (person-fact cid :commit/_committer (:commit/committer c))]
        (into (map (partial parent-fact cid) (:commit/parents c)))
        (into (mapcat (partial diff-facts cid) (:commit/diffs c))))))

(defn create-repo [name path]
  (let [r-fact (repo-fact name path)
        {:keys [db-after tempids]} (dtm/tx (dtm/connect) [r-fact])]
    (d/resolve-tempid db-after tempids (:db/id r-fact))))

(defn main [repo-name repo-path]
  (println "Creating db: " (dtm/create-db))
  (dtm/ensure-schema (dtm/connect))
  (let [repo (git/open-repo repo-path)
        n (atom 0)
        repo-id (create-repo repo-name repo-path)]
    (doseq [c (git/commits repo (git/git-log repo))]
      (swap! n inc)
      (dtm/tx (dtm/connect)(commit-facts repo-id c))
      (when (= 0 (rem @n 100))
        (print ".")
        (flush)))
    (println)
    (println "Imported" @n "commits.")))