(ns gitomic.log-import
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [datomic.api :as d]
            [gitomic.datomic :as gd]
            ))

;

(defn git-log-commits [d]
  (-> (sh "git"
          "log"
          "--pretty=format:GITOMIC%n[\"%H\" \"%P\" \"%an\" \"%ae\" #inst \"%ad\"]%n%s%n--%n%b%n--"
          "--date=iso-strict"
          "--numstat"
          "--reverse"
          :dir d)
      :out
      (java.io.StringReader.)
      io/reader
      line-seq))

(defn remove-empty-vals [m]
  (reduce (fn [x [k v]]
            (if (or (nil? v)
                    (and (or (string? v) (coll? v)) (empty? v)))
              (dissoc x k)
              x))
          m m))

(defn parse-numstat [s]
  (when-not (empty? s)
    (let [[added deleted path] (string/split s #"\t")
          as-num #(when-not (= "-" %) (edn/read-string %))]
      (remove-empty-vals
        {:db/id (gd/tid)
         :change/file {:db/id (gd/tid)
                       :file/path path}
         :change/added (as-num added)
         :change/deleted (as-num deleted)}))))

(defn parse-parents [s]
  (let [[one two] (string/split s #"\s")]
    (remove nil? [(when (not-empty one)
                    {:db/id (gd/tid) :commit/sha one}
                    (if two
                      {:db/id (gd/tid) :commit/sha two}))])))

(defn parse-commit [raw-commit]
  (try
    (let [cid (gd/tid)
          [sha parents author-name author-email committed-at] (edn/read-string (first raw-commit))
          subject (second raw-commit)
          [_ b _ changes] (partition-by (partial = "--") (drop 2 raw-commit))
          body (string/trim (string/join "\n" b))]
      (remove-empty-vals
        {:db/id cid
         :commit/sha sha
         :commit/short-sha (.substring sha 0 7)
         :commit/parents (parse-parents parents)
         :commit/author {:db/id (gd/tid)
                         :person/name author-name
                         :person/email author-email}
         :commit/time committed-at
         :commit/merge? (> 1 (count parents))
         :commit/subject subject
         :commit/body (if-not (empty? body) body)
         :commit/changes (into [] (remove nil? (map parse-numstat changes)))}))
    (catch Exception e
      (pprint raw-commit)
      (throw e))))

(defn split-commits [lines]
  (->> lines
       (partition-by #(.startsWith % "GITOMIC") )
       (partition 2)
       (map (comp (partial drop 1) vec flatten))))

(defn parse-lines [lines]
  (map parse-commit (split-commits lines)))

(defn read-log [f]
  (with-open [r (io/reader f)]
    (-> r line-seq parse-lines doall)))

(defn commit-facts [repo-id commit]
  (let [changes (mapcat (fn [change]
                          [(assoc (:change/file change)
                             :repo/_files repo-id)
                           (assoc change :change/file (-> change :change/file :db/id)
                                         :commit/_changes (:db/id commit))])
                        (:commit/changes commit))
        parents (map (fn [p]
                       (assoc p :commit/_parents (:db/id commit)))
                     (:commit/parents commit))]
    (concat [(-> commit
                 (assoc :commit/author (-> commit :commit/author :db/id)
                        :repo/_commits repo-id)
                 (dissoc :commit/parents :commit/changes))]
            parents
             [(assoc (:commit/author commit)
                :repo/_authors repo-id)]
            changes)))

(defn create-repo [name path]
  (let [id (gd/tid)
        tx-result (gd/tx (gd/connect) [{:db/id id :repo/name name :repo/path path}])]
    (d/resolve-tempid (:db-after tx-result) (:tempids tx-result) id)))

(defn run-import [repo-name repo-path commits]
  (println "Creating db: " (gd/create-db))
  (gd/ensure-schema (gd/connect))
  (let [n (atom 0)
        repo-id (create-repo repo-name repo-path)]
    (doseq [c commits]
      (swap! n inc)
      (try
        (gd/tx (gd/connect) (commit-facts repo-id c))
        (catch Exception e
          (pprint (commit-facts repo-id c))
          (throw e)))
      (when (= 0 (rem @n 100))
        (print ".")
        (flush)))
    (println)
    (println "Imported" @n "commits.")))

(defn import-log-file [repo-name repo-path log-path]
  (run-import repo-name repo-path (read-log log-path)))

(defn import-local-repo [repo-name repo-path]
  (run-import repo-name
              repo-path
              (-> repo-path git-log-commits parse-lines)))