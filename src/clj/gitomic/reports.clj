(ns gitomic.reports
  (:require [datomic.api :as d :refer [q]]
            [gitomic.datomic :as gd]
            [gitomic.query :as query]
            [clojure.pprint :as pretty :refer [pprint]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(defn all-pairs [xs]
  (loop [pairs [] x (first xs) xs (rest xs)]
    (if-not x
      pairs
      (recur (concat pairs (map (fn [y] #{x y}) xs))
             (first xs)
             (rest xs)))))

(defn commit-pairs [commit]
  (let [changes (:commit/changes commit)]
    (when (and (> (count changes) 1) (< (count changes) 50))
      (all-pairs
        (map (comp :file/path :change/file) changes)))))

(defn avg [xs]
  (/ (reduce + xs)
     (count xs)))

(defn std-dev [xs]
  (let [mean (avg xs)
        square #(* % %)
        difference #(- mean %)]
    (avg (map (comp square difference) xs))))

(defn pairs->map [pairs]
  (reduce (fn [m [k v]]
            (assoc m k v))
          {}
          pairs))

(defn write-csv [fname data]
  (with-open [out-file (io/writer fname)]
    (csv/write-csv out-file data)))

(defn maps->table [keys ms]
  (concat [keys]
          (map #((apply juxt keys) %) ms)))

(defn churn-buddies [db repo-name]
  (let [repo (gd/ent db [:repo/name repo-name])
        commits (filter #(not (:commit/merge? %)) (:repo/commits repo))]
    (reduce (fn [m pair]
              (merge-with + m {pair 1}))
            {}
            (remove nil? (mapcat commit-pairs commits)))))

(defn pair-details [churn-report [pair pair-churn]]
  (let [[f1 f2] (vec pair)
        f1-churn (get churn-report f1)
        f2-churn (get churn-report f2)]
    {:f1 f1
     :f1-churn f1-churn
     :f2 f2
     :f2-churn f2-churn
     :coupling pair-churn
     :f1-percentage (Math/round (float (* 100 (/ pair-churn f1-churn))))
     :f2-percentage (Math/round (float (* 100 (/ pair-churn f2-churn))))}))

(defn temporal-coupling [db repo-name]
  (let [repo (gd/ent db [:repo/name repo-name])
        buddies (pairs->map (churn-buddies db repo-name))
        churn (pairs->map (query/churn db (:db/id repo)))
        counts (vals buddies)
        stats {:max (reduce max counts)
               :mean (float (avg counts))
               :std-dev (float (std-dev counts))}]
    {:stats stats
     :pairs (map (partial pair-details churn)
                 buddies)}))

(defn coupling->csv [db fname repo]
  (let [tc (temporal-coupling db repo)]
    (write-csv fname
               (maps->table [:f1 :f1-churn :f2 :f2-churn :coupling :f1-percentage :f2-percentage]
                            (:pairs tc)))))