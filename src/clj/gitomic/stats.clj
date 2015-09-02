(ns gitomic.stats)

(defn avg [xs]
  (/ (reduce + xs)
     (count xs)))

(defn square [x]
  (* x x))

(defn std-dev
  ([xs]
   (std-dev xs (avg xs)))
  ([xs mean]
   (let [difference #(- mean %)]
     (avg (map (comp square difference) xs)))))

(defn basic-stats [xs]
  (let [mean (avg xs)
        sd (std-dev xs mean)]
    {:max (reduce max xs)
     :min (reduce min xs)
     :mean (float mean)
     :std-dev (float sd)}))

