(ns echo.dataserver.utils)

(defmacro threadlocal [& body]
 `(proxy ^ThreadLocal [ThreadLocal] []
    (initialValue []
      ~@body)))
