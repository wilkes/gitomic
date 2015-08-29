(ns gitomic.log-import
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]))

; git log --pretty=format:'GITOMIC%n["%H" "%P" "%an" "%ae" #inst "%ad"]%n%s%n--%n%b%n--' --date=iso-strict --numstat --reverse

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
        {:change/path {:file/path path}
         :change/added (edn/read-string added)
         :change/deleted (edn/read-string deleted)}))))

(defn parse-commit [raw-commit]
  (try
    (let [[sha parents author-name author-email committed-at] (edn/read-string (first raw-commit))
          parents (string/split parents #"\s")
          subject (second raw-commit)
          [_ b _ changes] (partition-by (partial = "--")
                                        (drop 2 raw-commit))
          body (string/trim (string/join "\n" b))]
      (remove-empty-vals
        {:commit/sha sha
         :commit/parents (if-not (= [""] parents) parents)
         :commit/author {:person/name author-name
                         :person/email author-email}
         :commit/committed-at committed-at
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

(defn read-log [f]
  (with-open [r (io/reader f)]
    (->> r
         line-seq
         split-commits
         (map parse-commit)
         doall)))
