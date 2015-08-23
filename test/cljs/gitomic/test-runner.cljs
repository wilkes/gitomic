(ns gitomic.test-runner
  (:require
   [cljs.test :refer-macros [run-tests]]
   [gitomic.core-test]))

(enable-console-print!)

(defn runner []
  (if (cljs.test/successful?
       (run-tests
        'gitomic.core-test))
    0
    1))
