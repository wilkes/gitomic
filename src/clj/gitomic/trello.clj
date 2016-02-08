(ns gitomic.trello
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as string]))

(defn load-board [json-file]
  (json/parse-stream (io/reader json-file) true))

(def common-words
  ["" "to" "the" "not" "i" "in" "a" "on" "is" "and" "t" "for"
   "of" "when" "are" "it" "this" "up" "but" "have" "from" "with"
   "error" "an" "as" "that" "can" "my" "be" "s" "being"
   "you" "has" "or" "-" "if" "does"])

(defn words [strs]
  (apply dissoc
         (reduce (fn [results n]
                   (let [ws (apply hash-map (interleave (string/split (string/lower-case n) #" ")
                                                        (repeat 1)))]
                     (merge-with + results ws)))
                 {}
                 strs)
         common-words))

(defn word-cloud [word-counts]
  (string/join " " (mapcat (fn [[x n]] (repeat n x)) word-counts)))

(defn filtered-weighted [strs remove top-n]
  (as-> strs xs
        (words xs)
        (apply dissoc xs remove)
        (vec (reverse (sort-by second xs)))
        (take (or top-n (count xs)) xs)))

[{:subscribed [:subscribed
               :labels
               :closed
               :manualCoverAttachment
               :email
               :idChecklists
               :desc
               :dateLastActivity
               :name
               :shortLink
               :attachments
               :idList
               :pos
               :descData
               :idLabels
               :due
               :id
               :badges
               :idMembersVoted
               :url
               :idBoard
               :checkItemStates
               :idAttachmentCover
               :idMembers
               :shortUrl
               :idShort] }
 {:labels [:id :idBoard :name :color :uses]}
 {:lists [:id :name :closed :idBoard :pos :subscribed]}
 :name
 :shortLink
 :descData
 :id
 :idOrganization
 {:cards [:subscribed
          :labels
          :closed
          :manualCoverAttachment
          :email
          :idChecklists
          :desc
          :dateLastActivity
          :name
          :shortLink
          :attachments
          :idList
          :pos
          :descData
          :idLabels
          :due
          :id
          :badges
          :idMembersVoted
          :url
          :idBoard
          :checkItemStates
          :idAttachmentCover
          :idMembers
          :shortUrl
          :idShort]}
 :url
 :shortUrl]