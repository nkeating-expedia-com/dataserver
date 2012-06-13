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

(def default-object
  {:object-type "note"
   :source {:name "DataServer source"
            :icon "http://cdn.js-kit.com/images/echo.png"
            :uri  "http://aboutecho.com/"}})
(def default-actor 
  {:object-type "person"})
(def default-entry 
  {:verb "post"})

(defn target [t]
  [:target {:ns :activity}
    [:id (:url t)]])

(defn entry [record]
  (let [item   (:item record)
        {:keys [object actor targets source]} item
        object (merge default-object object)
        actor  (merge default-actor  actor)
        entry  (merge default-entry {:object object, :actor actor, :targets targets, :source source})
        content (get-in entry [:object :content])
        content (if config/*debug* 
                  (str "source: " (get-in record [:source :name]) "\n"
                       "rule: "   (get-in record [:rule :name])   "\n"
                       "user: "   (get-in item   [:actor :name])  "\n\n"
                       content) 
                  content)]
    (concat
      [:entry
        [:published (format-date (or (:published entry) (now)))]
        [:updated   (format-date (or (:updated entry)   (now)))]
        [:verb {:ns :activity} (:verb entry)]
        [:source 
          [:provider {:ns "service"}
            [:name (get-in entry [:source :name])]
            [:uri  (get-in entry [:source :url])]
            [:icon (get-in entry [:source :icon])]]]
        [:object {:ns :activity}
          [:object-type {:ns :activity} (get-in entry [:object :object-type])]
          [:id          {:ns :activity} (get-in entry [:object :url])]
          [:content     {"type" "html"} content]
          [:link        {"rel" "alternate" "type" "text/html" "href" (get-in entry [:object :url])}]
          [:source      {"type" "html"} (get-in entry [:object :source])]]
        [:actor {:ns :activity}
          [:object-type {:ns :activity} (get-in entry [:actor :object-type])]
          [:id          (get-in entry [:actor :url])]
          [:title       (get-in entry [:actor :displayName])]
          [:link {"rel" "avatar"    "type" "image/jpeg" "href" (get-in entry [:actor :avatar])}]
          [:link {"rel" "alternate" "type" "text/html"  "href" (get-in entry [:actor :url])}]
        ]]
      (mapv target (:targets entry)))
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

(defn json->xml [record]
  (let [_entries [(entry record)]
        _feed    (feed _entries)]
    (with-out-str
      (xml/emit _feed))))

(defbolt json->payload ["payload"] [tuple collector] 
  (let [record  (read-string (.getString tuple 0))
        _xml    (json->xml record)
        record  (-> record (dissoc :item) (assoc :xml _xml))
        payload (json/json-str record)]
    (emit-bolt! collector [payload] :anchor tuple)
    (ack! collector tuple)))
