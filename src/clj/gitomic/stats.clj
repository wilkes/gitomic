(ns gitomic.stats
  (:require [incanter.stats :as ist]
            [incanter.core :as i]))

(defn basic [xs]
  (let [mean (ist/mean xs)]
    {:max (reduce max xs)
     :min (reduce min xs)
     :mean (float mean)
     :median (ist/median xs)
     :sample-std-dev (ist/sd xs)}))

(defn for-column [col ds]
  (basic (i/$ col ds)))

(defn quantile [xs p]
  (nth (vec (sort xs))
       (Math/round (* p (count xs)))))

(comment
  (def num-friends [100,49,41,40,25,21,21,19,19,18,18,16,15,15,15,15,14,14,13,13,13,13,12,12,11,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,8,8,8,8,8,8,8,8,8,8,8,8,8,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1])
  (quantile num-friends 0.89))
