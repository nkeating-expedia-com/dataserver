(ns echo.dataserver.utils)

(defmacro defthreadlocal [name & body]
 `(def ~name
    (proxy [ThreadLocal] []
      (initialValue []
        ~@body))))

(defn now []
  (java.util.Date.))