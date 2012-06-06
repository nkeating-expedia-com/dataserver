(ns echo.dataserver.twitter
  (:use [backtype.storm clojure log])
  (:import [java.text SimpleDateFormat])
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [echo.dataserver.utils :as utils])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn uri-of-tweet [name id]
  (str "https://twitter.com/" name "/status/" id))

(defn uri-of-twitterer [name]
  (str "https://twitter.com/" name))

(def date-parser (utils/threadlocal ; "Mon Jun 04 19:04:12 +0000 2012"
  (SimpleDateFormat. "E M d HH:mm:ss Z yyyy")))

(def date-formatter (utils/threadlocal  ; "2010-03-22T11:18:21Z"
  (doto
    (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")
    (.setTimeZone (java.util.TimeZone/getTimeZone "GMT")))))

(defn parse-date [str]
  (.parse ^SimpleDateFormat (.get date-parser) str))

(defn format-date [date]
  (.format ^SimpleDateFormat (.get date-formatter) date))

(defn tweet->as [tweet]
  (let [{:keys [text id in_reply_to_status_id user created_at]}  tweet
        {:keys [name profile_image_url]}  user
        published (format-date (parse-date created_at))
        uri (uri-of-tweet name id)]
    {:object {:content text
              :id uri}
     :author {:uri (uri-of-twitterer name)
              :name name}
     :actor  {:id (uri-of-twitterer name)
              :title name
              :avatar profile_image_url}
     :published published
     :updated   published
     :id        uri}))

(defspout twitter-spout ["tweet"] {:params [in out] :prepared true}
  [conf context collector]
  (let [rdr (atom (clojure.java.io/reader in))]
    (spout
      (nextTuple []
        (if @rdr
          (if-let [l (binding [*in* @rdr] (read-line))]
            (emit-spout! collector [l] :id (+ 100000 (rand-int 899999)))
            (with-open [^java.io.BufferedReader _ @rdr]
              (log-message "DONE READING\n")
              (reset! rdr nil)))
          (Thread/sleep 100)))
      (ack [id]
        (log-message "ACKED: " id "\n")
        (spit out (str "Processed " id "\n") :append true)))))

(defbolt tw-parse ["as"] [tuple collector]
  (let [tweet (json/read-json (.getString tuple 0))]
    (when (tweet :text)
      (emit-bolt! collector [(pr-str (tweet->as tweet))] :anchor tuple)))
    (ack! collector tuple))

(defbolt as-persist [] {:params [out]} [tuple collector] 
  (let [as-entry (read-string (.getString tuple 0))]
    (spit out (str (pr-str as-entry) "\n") :append true)
    (ack! collector tuple)))

(defn mk-topology []
  (topology
   {"1" (spout-spec (twitter-spout "tweets.json" "log-processed.txt"))}
   {"2" (bolt-spec {"1" :shuffle} tw-parse :p 6)
    "3" (bolt-spec {"2" :shuffle} (as-persist "log-persisted.txt") :p 6)}))
