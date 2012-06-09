(ns echo.dataserver.utils)
(set! *warn-on-reflection* true)

(defmacro defthreadlocal [name & body]
 `(def ~name
    (proxy [ThreadLocal] []
      (initialValue []
        ~@body))))

(defn now []
  (java.util.Date.))

(defn env [v]
  (-> (System/getenv) (.get v)))

(defn mapmap [f m]
  (into {} (mapv (fn [[k v]] (f k v)) m)))

(defn str-head [s n]
  (subs s 0 (min n (count s))))