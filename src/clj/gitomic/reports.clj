(ns gitomic.reports
  (:require
    [clojure.pprint :refer [pprint]]
    [gitomic.datomic :as gd]
    [gitomic.query :as query]
    [gitomic.stats :as stats]
    [incanter.core :as i]
    [incanter.datasets :as ds]
    [incanter.stats :as istats]
    [datomic.api :as d]))

(defn all-pairs [xs]
  (loop [pairs [] x (first xs) xs (rest xs)]
    (if-not x
      pairs
      (recur (concat pairs (map (fn [y] #{x y}) xs))
             (first xs)
             (rest xs)))))

(defn commit-pairs [commit  {:keys [file-filter min-change-size max-change-size]
                              :or {file-filter identity
                                   min-change-size 1
                                   max-change-size 50}}]
  (let [changes (:commit/changes commit)]
    (when (and (> (count changes) min-change-size)
               (< (count changes) max-change-size))
      (all-pairs
        (filter file-filter
                (map (comp :file/path :change/file) changes))))))

(defn pairs->map [pairs]
  (reduce (fn [m [k v]]
            (assoc m k v))
          {}
          pairs))

(defn change-pairs [db repo-name opts]
  (let [repo (gd/ent db [:repo/name repo-name])
        commits (filter #(not (:commit/merge? %)) (:repo/commits repo))]
    (reduce (fn [m pair]
              (merge-with + m {pair 1}))
            {}
            (remove nil? (mapcat #(commit-pairs % opts) commits)))))


(defn pair-details [churn-report [pair pair-churn]]
  (let [[f1 f2] (vec pair)
        f1-churn (get churn-report f1)
        f2-churn (get churn-report f2)
        make-map (fn [f1 f2 f1-churn f2-churn]
                   {:f1 f1
                    :f2 f2
                    :churn f1-churn
                    :coupling pair-churn
                    :percentage-of-f1 (Math/round (float (* 100 (/ pair-churn f1-churn))))
                    :percentage-of-f2 (Math/round (float (* 100 (/ pair-churn f2-churn))))})]
    [(make-map f1 f2 f1-churn f2-churn)
     (make-map f2 f1 f2-churn f1-churn)]))

(def rails-code-filter #(or (.startsWith % "app/") (.startsWith % "lib/")))

(defn temporal-coupling [db repo-name opts]
  (let [r (:db/id (gd/ent db [:repo/name repo-name]))
        churn (pairs->map (query/churn db r))]
    (->> (change-pairs db repo-name opts)
         pairs->map
         (mapcat (partial pair-details churn))
         i/to-dataset)))

(defn coupling-stats [ds]
  {:churn (stats/basic-stats (i/$ :churn ds))
   :coupling (stats/basic-stats (i/$ :coupling ds))
   :percentages (stats/basic-stats (i/$ :percentage-of-f1 ds))})

(defn coupling->csv [db repo fname & opts]
  (let [tc (temporal-coupling db repo opts)
        pairs (i/$order [:churn :f1 :percentage] :desc tc)]
    (i/save pairs fname)))