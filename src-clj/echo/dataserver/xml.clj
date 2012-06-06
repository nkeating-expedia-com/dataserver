(ns echo.dataserver.xml
  (:require [clojure.string :as str]))
(set! *warn-on-reflection* true)

(def ^:dynamic *depth* 0)
(def ^:dynamic *indent?* true)

(defn- escape-quotes [s]
  (str/escape s {
    \' "&apos;" 
    \" "&quot;"}))

(defn- escape-body [s]
  (str/escape s {\< "&lt;"
             \> "&gt;"
             \& "&amp;"
             \' "&apos;"
             \" "&quot;"}))

(defn tostr [obj]
  (cond
    (string? obj) obj
    (nil? obj) ""
    :else (name obj)))

(defn qname [tag attrs]
  (if-let [xmlns (:ns attrs)]
    (str (tostr xmlns) ":" (tostr tag))
    (tostr tag)))

(defn print-indent []
  (when *indent?*
    (print "\n")
    (dotimes [_ *depth*] (print "  "))))

(defn print-attrs [attrs]
  (binding [*indent?* (> (count attrs) 1)]
    (doseq [[k v] (dissoc attrs :ns)]
      (print-indent)
      (print (str " " (tostr k) "=\"" (escape-quotes (tostr v)) "\"")))))


(defn xml [arg]
  (cond
    (string? arg) 
      (do
        (print-indent)
        (print (escape-body arg)))
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
          (map? (first children))
            (let [attrs (first children)
                  children (next children)
                  no-children (empty? children)
                  simple-child (and (= 1 (count children)) (not (coll? (first children))) (<= (count (dissoc attrs :ns)) 0))]
              (print-indent)
              (print "<")
              (print (qname tag attrs))
              (print-attrs attrs)
              (if (empty? children)
                (print " />")
                (do
                  (print ">")
                  (binding [*depth* (inc *depth*)
                        *indent?* (not simple-child)]
                    (doseq [child children]
                      (xml child)))
                  (when (not simple-child)
                    (print-indent))
                  (print (str "</" (qname tag attrs) ">")))))
          :else
            (xml (concat [tag {}] children))))))