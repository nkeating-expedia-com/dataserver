(ns echo.dataserver.activitystream
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [echo.dataserver.xml :as xml]
            [echo.dataserver.config :as config])
  (:use     [backtype.storm clojure]
             echo.dataserver.utils)
  (:import  [java.text SimpleDateFormat])
  (:gen-class))
(set! *warn-on-reflection* true)

(defthreadlocal date-formatter
  (doto
    (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")
    (.setTimeZone (java.util.TimeZone/getTimeZone "GMT")))) ; "2010-03-22T11:18:21Z"

(defn format-date [^java.util.Date date]
  (.format ^SimpleDateFormat (.get ^ThreadLocal date-formatter) date))

(defn target [t]
  [:target {:ns :activity}
    [:id (:url t)]])

(defn entry [record]
  (let [item (-> (:item record)
                 (update-in [:verb] #(or % "post"))
                 (update-in [:object :object-type] #(or % "note"))
                 (update-in [:object :source] #(or % {:name "DataServer source"
                                                      :icon "http://cdn.js-kit.com/images/echo.png"
                                                      :uri  "http://aboutecho.com/"}))
                 (update-in [:actor :object-type] #(or % "person")))]
    (concat
      [:entry
        [:published (format-date (or (:published item) (now)))]
        [:updated   (format-date (or (:updated item)   (now)))]
        [:verb {:ns :activity} (:verb item)]
        [:source 
          [:provider {:ns "service"}
            [:name (get-in item [:source :name])]
            [:uri  (get-in item [:source :url])]
            [:icon (get-in item [:source :icon])]]]
        [:object {:ns :activity}
          [:object-type {:ns :activity} (get-in item [:object :object-type])]
          [:id          {:ns :activity} (get-in item [:object :url])]
          [:content     {"type" "html"} (get-in item  [:object :content])]
          [:link        {"rel" "alternate" "type" "text/html" "href" (get-in item [:object :url])}]
          [:source      [:title {"type" "html"} (get-in item [:object :source])]]]
        [:actor {:ns :activity}
          [:object-type {:ns :activity} (get-in item [:actor :object-type])]
          [:id          (get-in item [:actor :url])]
          [:title       (get-in item [:actor :displayName])]
          [:link {"rel" "avatar"    "type" "image/jpeg" "href" (get-in item [:actor :avatar])}]
          [:link {"rel" "alternate" "type" "text/html"  "href" (get-in item [:actor :url])}]
        ]]
      (mapv target (:targets item)))
    ))

(defn feed [entries]
  [:decl "xml" (array-map "version" "1.0" "encoding" "UTF-8")
    (concat 
      [:feed {"xml:lang"       "en-US"
              "xmlns"          "http://www.w3.org/2005/Atom"
              "xmlns:activity" "http://activitystrea.ms/spec/1.0/"
              "xmlns:thr"      "http://purl.org/syndication/thread/1.0"
              "xmlns:media"    "http://purl.org/syndication/atommedia"
              "xmlns:service"  "http://activitystrea.ms/service-provider"}
        [:updated (format-date (now))]
        [:generator {"uri" "http://aboutecho.com/"} "DataServer (c) JackNyfe, 2012"]
        [:provider {:ns "service"}
          [:name "DataServer"]
          [:uri "http://aboutecho.com/"]
          [:icon "http://cdn.js-kit.com/images/echo.png"]]]
      entries)])

(defn as->xml [record]
  (let [_entries [(entry record)]
        _feed    (feed _entries)]
    (with-out-str
      (xml/emit _feed))))

(defbolt as->payload ["payload"] [tuple collector] 
  (let [record  (read-string (.getString tuple 0))
        _xml    (as->xml record)
        record  (-> record 
                    (dissoc :item)
                    (assoc  :xml _xml))
        payload (json/json-str record)]
    (emit-bolt! collector [payload] :anchor tuple)
    (ack! collector tuple)))
