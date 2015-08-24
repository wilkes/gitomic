(ns gitomic.git
  (:require[clojure.pprint :refer [pprint]]
            [clojure.java.io :as io])
  (:import
    [org.eclipse.jgit.lib Repository PersonIdent ObjectId]
    [org.eclipse.jgit.storage.file FileRepositoryBuilder]
    [org.eclipse.jgit.api Git]
    [org.eclipse.jgit.revwalk RevCommit
                              RevTree]
    [java.util Date]
    [org.eclipse.jgit.diff DiffFormatter RawTextComparator DiffEntry]
    [org.eclipse.jgit.util.io DisabledOutputStream]))

(defn diff-tree [^Repository r ^RevTree t ^RevTree p]
  (let [df (doto (DiffFormatter. DisabledOutputStream/INSTANCE)
             (.setRepository r)
             (.setDiffComparator RawTextComparator/WS_IGNORE_ALL))]
    (map (fn [^DiffEntry d]
           (let []
             {:change-type (-> (.getChangeType d)
                               .toString .toLowerCase keyword)
              :new-path (.getNewPath d)
              :old-path (.getOldPath d)
              :new-sha (ObjectId/toString (.toObjectId (.getNewId d)))
              :old-sha (ObjectId/toString (.toObjectId (.getOldId d)))}))
         (.scan df p t))))

(defn person [^PersonIdent pi]
  {:name (.getName pi)
   :email (.getEmailAddress pi)})

(defn make-commit [^Repository r ^RevCommit c]
  (let [diffs (diff-tree r
                         (.getTree c)
                         (some-> c .getParents first .getTree))]
    {:sha (ObjectId/toString c)
     :committer (-> c .getCommitterIdent person)
     :author (-> c .getAuthorIdent person)
     :message (.getFullMessage c)
     :parents (into [] (map #(ObjectId/toString %) (.getParents c)))
     :tree (-> c .getTree ObjectId/toString)
     :time (Date. (int (* 1000 (.getCommitTime c))))
     :diffs diffs
     :diffs-count (count diffs)}))

(defn open-repo [path]
  (FileRepositoryBuilder/create (io/file path ".git")))

(defn git-log [^Repository repo]
  (-> repo Git. .log .all .call seq reverse))

(defn commits [^Repository repo l]
  (map (partial make-commit repo) l))

(comment
  (time (do
          (def repo-path "path-to-repo")
          (def r (open-repo repo-path))
          (def n (atom 0))
          (def b (atom 0))
          (def l (git-log r))
          (def c (rand-nth (into [] l))))
        )

  (time (doseq [c (commits r l)]
          (swap! n inc)
          (reset! b (count (:diffs c)))
          (when (= 0 (rem @n 100))
            (println @n ":" @b)
            (pprint c))))
  )