(ns echo.dataserver.activitystream
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.data.xml :as xml])
  (:use      echo.dataserver.utils)
  (:import  [java.text SimpleDateFormat])
  (:gen-class))

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
  {:object-type "http://activitystrea.ms/schema/1.0/comment"
   :title ""})
(def default-actor 
  {:object-type "http://activitystrea.ms/schema/1.0/person"})
(def default-author {})
(def default-entry 
  {:verb "http://activitystrea.ms/schema/1.0/post"})

(defn entry [entry]
  (let [{:keys [object actor author]} entry
        object (merge default-object object)
        actor  (merge default-actor  actor)
        author (merge default-author author)
        entry  (merge default-entry {:object object, :actor actor, :author author})]
    [:entry
      [:published (format-date (or (:published entry) (now)))]
      [:updated   (format-date (or (:updated entry)   (now)))]
      [:verb {:ns :activity} (:verb entry)]
      [:object {:ns :activity}
        [:object-type {:ns :activity} (get-in entry [:object :object-type])]
        [:id          {:ns :activity} (get-in entry [:object :id])]
        [:title       {:ns :activity} (get-in entry [:object :title])]]]
    ))

(defn feed [entries]
  [:decl "xml" {"version" "1.0" "encoding" "UTF-8"}
    (concat 
      [:feed {"xml:lang"       "en-US"
              "xmlns"          "http://www.w3.org/2005/Atom"
              "xmlns:activity" "http://activitystrea.ms/spec/1.0/"
              "xmlns:thr"      "http://purl.org/syndication/thread/1.0"
              "xmlns:media"    "http://purl.org/syndication/atommedia"}
        [:updated (format-date (now))]
        [:generator {"uri" "http://aboutecho.com/"} "DataServer (c) JackNyfe, 2012"]]
      entries)])