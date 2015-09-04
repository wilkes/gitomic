(ns gitomic.stats
  (:require [incanter.stats :as ist]))

(defn basic-stats [xs]
  (let [mean (ist/mean xs)]
    {:max (reduce max xs)
     :min (reduce min xs)
     :mean (float mean)
     :median (ist/median xs)
     :sample-std-dev (ist/sd xs)}))

