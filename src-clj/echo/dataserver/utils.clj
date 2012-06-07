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