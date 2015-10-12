(ns gitomic.reports
  (:require
    [clojure.pprint :refer [pprint]]
    [gitomic.datomic :as gd]
    [gitomic.query :as query]
    [gitomic.stats :as stats]
    [incanter.core :as i]
    [incanter.datasets :as ds]
    [incanter.stats :as istats]
    [datomic.api :as d]
    [clj-time.core :as t]
    [clj-time.coerce :as tc]))

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
        (filter file-filter (map (comp :file/path :change/file) changes))))))

(defn pairs->map [pairs]
  (reduce (fn [m [k v]]
            (assoc m k v))
          {}
          pairs))

(defn change-pairs [db opts]
  (reduce (fn [m pair]
            (merge-with + m {pair 1}))
          {}
          (remove nil? (mapcat #(commit-pairs (gd/ent db %) opts) (query/non-merge-commits db)))))


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

(def rails-code-filter #(or (.startsWith ^String % "app/") (.startsWith ^String % "lib/")))

(defn commit-size-filter [min-size max-size]
  (fn [commit]
    (let [size (-> commit :commit/changes count)]
      (and (>= size min-size)
           (<= size max-size)))))

(defn temporal-coupling [db opts]
  (let [churn (pairs->map (query/churn db))]
    (->> (change-pairs db opts)
         pairs->map
         (mapcat (partial pair-details churn))
         i/to-dataset)))

(defn commits-dataset [db & {:keys [file-filter] :or {file-filter identity}}]
  (i/to-dataset (filter (comp file-filter :file/path) (query/change-maps db))))

(defn coupling-stats [ds]
  {:churn (stats/basic (i/$ :churn ds))
   :coupling (stats/basic (i/$ :coupling ds))
   :percentages (stats/basic (i/$ :percentage-of-f1 ds))})

(defn coupling->csv [db fname & opts]
  (let [tc (temporal-coupling db opts)
        pairs (i/$order [:churn :f1 :percentage] :desc tc)]
    (i/save pairs fname)))

(defn path-ownership [ds]
  (let [added-total (i/rename-cols {:change/added :change/added-total}
                                   (i/$rollup :sum :change/added :file/path ds))
        deleted-total (i/rename-cols {:change/deleted :change/deleted-total}
                                     (i/$rollup :sum :change/deleted :file/path ds))
        added-author (i/$rollup :sum :change/deleted [:person/name :file/path] ds)
        deleted-author (i/$rollup :sum :change/added [:person/name :file/path] ds)
        joined (i/$join [:file/path :file/path]
                        (i/$join [:file/path :file/path] added-total deleted-total)
                        (i/$join [:file/path :file/path] added-author deleted-author))]
    (as-> joined x
          (i/add-derived-column
            :ownership [:change/added :change/added-total]
            (fn [n d] (if (pos? d)
                        (* 100.0 (/ n d))
                        -1.0))
            x)
          (i/$order [:change/added-total :ownership] :desc x)
          (i/reorder-columns x [:file/path :person/name	:ownership
                                :change/added :change/added-total
                                :change/deleted :change/deleted-total]))))

(defn path-effort [ds]
  (let [joined
        (i/$join [:file/path :file/path]
                 (i/rename-cols {:commit/sha :total-commits}
                                       (i/$rollup :count :commit/sha :file/path ds))
                 (i/rename-cols {:commit/sha :author-commits}
                                (i/$rollup :count :commit/sha [:file/path :person/name] ds)))]
    (as-> joined x
          (i/add-derived-column
            :author-percentage [:author-commits :total-commits]
            (fn [ac tc] (* 100.0 (/ ac tc)))
            x)
          (i/$order [:total-commits :file/path :author-commits] :desc x)
          (i/reorder-columns x [:file/path :person/name
                                :author-commits :author-percentage :total-commits]))))

(defn authors-contribution [repo-name]
  (let [db (gd/db repo-name)
        commits (commits-dataset db)]
    (as-> (i/$join [:person/name :person/name]
                   (i/$rollup :sum :change/added :person/name commits)
                   (i/$rollup :sum :change/deleted :person/name commits))
          ds
          (i/add-derived-column :changes/total [:change/added :change/deleted] -
                                ds)
          (i/reorder-columns ds [:person/name :change/added :change/deleted :changes/total])
          (i/$order [:change/added :change/deleted] :desc ds))))

(defn file-age [db]
  (->> (map (fn [[p ts]]
              {:file/path p
               :last-commit (t/in-days (t/interval (tc/from-date ts) (t/now)))})
            (query/file-age db))
       i/to-dataset
       (i/$order [:last-commit] :desc)))