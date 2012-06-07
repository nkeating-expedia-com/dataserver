(ns echo.dataserver.activitystream
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [echo.dataserver.xml :as xml])
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

(def namespaces
  {"activity" "http://activitystrea.ms/spec/1.0/"
   "thr" "http://purl.org/syndication/thread/1.0"
   "media" "http://purl.org/syndication/atommedia"})

(def default-object
  {:object-type "http://activitystrea.ms/schema/1.0/note"})
(def default-actor 
  {:object-type "http://activitystrea.ms/schema/1.0/person"})
(def default-entry 
  {:verb "http://activitystrea.ms/schema/1.0/post"})

(defn entry [item]
  (let [{:keys [object actor author]} item
        object (merge default-object object)
        actor  (merge default-actor  actor)
        entry  (merge default-entry {:object object, :actor actor})]
    [:entry
      [:published (format-date (or (:published entry) (now)))]
      [:updated   (format-date (or (:updated entry)   (now)))]
      [:verb {:ns :activity} (:verb entry)]
      [:source 
        [:provider {:ns "service"}
          [:name "Twitter"]
          [:uri (get-in entry [:object :id])]
          [:icon "http://cdn.js-kit.com/images/favicons/twitter.png"]]]
      [:object {:ns :activity}
        [:object-type {:ns :activity} (get-in entry [:object :object-type])]
        [:id          {:ns :activity} (get-in entry [:object :id])]
        [:content     {"type" "html"} (get-in entry [:object :content])]
        [:link        {"rel" "alternate" "type" "text/html" "href" (get-in entry [:object :id])}]
        [:source      {"type" "html"} (get-in entry [:object :source])]]
      [:actor {:ns :activity}
        [:object-type {:ns :activity} (get-in entry [:actor :object-type])]
        [:id          (get-in entry [:actor :id])]
        [:title       (get-in entry [:actor :title])]
        [:link {"rel" "avatar"    "type" "image/jpeg" "href" (get-in entry [:actor :avatar])}]
        [:link {"rel" "alternate" "type" "text/html"  "href" (get-in entry [:actor :id])}]
      ]]
    ))

(defn feed [entries]
  [:decl "xml" {"version" "1.0" "encoding" "UTF-8"}
    (concat 
      [:feed {"xml:lang"       "en-US"
              "xmlns"          "http://www.w3.org/2005/Atom"
              "xmlns:activity" "http://activitystrea.ms/spec/1.0/"
              "xmlns:thr"      "http://purl.org/syndication/thread/1.0"
              "xmlns:media"    "http://purl.org/syndication/atommedia"
              "xmlns:service"  "http://activitystrea.ms/service-provider"}
        [:id "tag:twitter.com,2007:Status"]
        [:title {"type" "text"} "Twitter status updates"]
        [:updated (format-date (now))]
        [:generator {"uri" "http://aboutecho.com/"} "DataServer (c) JackNyfe, 2012"]
        [:provider {:ns "service"}
          [:name "DataServer"]
          [:uri "http://aboutecho.com/"]
          [:icon "http://cdn.js-kit.com/images/echo.png"]]]
      entries)])

(defn json->xml [item]
  (let [_entries [(entry item)]
        _feed    (feed _entries)]
    (with-out-str
      (xml/emit-indented _feed))))

(defbolt json->payload ["payload"] [tuple collector] 
  (let [item  (read-string (.getString tuple 0))
        _xml  (json->xml item)
        submit-tokens (read-string (.getString tuple 1))
        payload ^String (json/json-str {:xml _xml :submit-tokens submit-tokens})]
    (emit-bolt! collector [(.getBytes payload)] :anchor tuple)
    (ack! collector tuple)))
