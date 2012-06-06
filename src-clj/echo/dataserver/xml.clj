(ns echo.dataserver.xml
  (:require [clojure.string :as str]))

(defn- escape [s]
  (str/escape s {\< "&lt;"
             \> "&gt;"
             \& "&amp;"
             \' "&apos;"
             \" "&quot;"}))

(defn tostr [obj]
  (cond
    (string? obj) obj
    (nil? obj) nil
    :else (name obj)))

(defn qname [tag attrs]
  (if-let [xmlns (tostr (:ns attrs))]
    (str xmlns ":" (tostr tag))
    (tostr tag)))

(defn print-attrs [attrs]
  (binding [*indent?* (> (count attrs) 1)]
    (doseq [[k v] (dissoc attrs :ns)]
      (print-indent)
      (print (str " " (tostr k) "=\"" (escape (tostr v)) "\"")))))

(def ^:dynamic *depth* 0)
(def ^:dynamic *indent?* true)
(defn print-indent []
  (when *indent?*
    (print "\n")
    (dotimes [_ *depth*] (print "  "))))

(defn xml [arg]
  (cond
    (string? arg) 
      (do
        (print-indent)
        (print (escape arg)))
    (nil? arg) :ok
    :else
      (let [[tag & children] arg]
        (cond
          (= :decl tag)
            (let [[tag attrs & children] children]
              (print-indent)
              (print "<?")
              (print tag)
              (print-attrs attrs)
              (print "?>\n")
              (doseq [child children]
                (xml child)))
          (nil? children)
            (do
              (print-indent)
              (print (str "<" (qname tag {}) " />")))
          (map? (first children))
            (let [attrs (first children)
                  children (next children)
                  simple-child (and (= 1 (count children)) (not (coll? (first children))) (<= (count (dissoc attrs :ns)) 0))]
              (print-indent)
              (print "<")
              (print (qname tag attrs))
              (print-attrs attrs)
              (print ">")
              (binding [*depth* (inc *depth*)
                        *indent?* (not simple-child)]
                (doseq [child children]
                  (xml child)))
              (when (not simple-child)
                (print-indent))
              (print (str "</" (qname tag attrs) ">")))
          :else
            (xml (concat [tag {}] children))))))