(ns echo.dataserver.config
  (:use echo.dataserver.utils))
(set! *warn-on-reflection* true)

(def ^:dynamic *debug* true)

(defn load-config [name]
  (read-string (slurp name)))