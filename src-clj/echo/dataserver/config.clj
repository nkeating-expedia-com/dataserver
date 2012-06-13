(ns echo.dataserver.config
  (:use echo.dataserver.utils))
(set! *warn-on-reflection* true)

(def ^:dynamic *debug* false)

(defn load-config [name]
  (read-string (slurp name)))