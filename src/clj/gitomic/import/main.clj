(ns gitomic.import.main
  (:require [gitomic.import.cloc :as cloc]
            [gitomic.import.log :as log]
            [gitomic.import.name-status :as name-status]))

(defn run [repo-name repo-path]
  (log/import-local-repo repo-name repo-path)
  (cloc/run repo-name repo-path)
  (name-status/run repo-name repo-path))
