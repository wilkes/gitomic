(ns gitomic.datomic
  (:require [datomic.api :as d :refer [q]]
            [environ.core :refer [env]]))

(def schema
  [
   {:db/id #db/id[:db.part/db]
    :db/ident :repo/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/fulltext true
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :repo/path
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/fulltext true
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :repo/files
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :repo/commits
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :repo/authors
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/sha
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/short-sha
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/time
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/author
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/parents
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/merge?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/subject
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/fulltext true
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/body
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/fulltext true
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :commit/changes
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :change/file
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :change/added
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :change/deleted
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :change/type
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/user] :db/ident :change/modify}
   {:db/id #db/id[:db.part/user] :db/ident :change/add}
   {:db/id #db/id[:db.part/user] :db/ident :change/delete}
   {:db/id #db/id[:db.part/user] :db/ident :change/copy}
   {:db/id #db/id[:db.part/user] :db/ident :change/rename}

   {:db/id #db/id[:db.part/db]
    :db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/fulltext true
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :person/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/fulltext true
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :file/path
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/fulltext true
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :loc/file
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :loc/language
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :loc/blank
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :loc/comment
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :loc/code
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ])

(defn connect []
  (d/connect (env :datomic-uri)))

(defn db []
  (d/db (connect)))

(defn tid []
  (d/tempid :db.part/user))

(defn tx [conn facts]
  @(d/transact-async conn facts))

(defn ent
  ([id] (ent (db) id))
  ([db id] (d/entity db id)))


(defn create-db []
  (d/create-database (env :datomic-uri)))

(defn delete-db []
  (d/delete-database (env :datomic-uri)))

(defn ensure-schema [conn]
  @(d/transact conn schema))

