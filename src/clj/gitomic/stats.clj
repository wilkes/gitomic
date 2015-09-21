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

