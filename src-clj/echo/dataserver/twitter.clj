(ns echo.dataserver.twitter
  (:use [backtype.storm clojure log]
        [echo.dataserver utils])
  (:import [java.text SimpleDateFormat]
           [backtype.storm.utils Utils])
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [http.async.client :as httpc])
  (:gen-class))
(set! *warn-on-reflection* true)

(defn uri-of-tweet [name id]
  (str "https://twitter.com/" name "/status/" id))

(defn uri-of-twitterer [name]
  (str "https://twitter.com/" name))

(defthreadlocal date-parser 
  (doto
    (SimpleDateFormat. "E MMM d HH:mm:ss Z yyyy" (java.util.Locale. "en"))
    (.setTimeZone (java.util.TimeZone/getTimeZone "GMT")))) ; "Mon Jun 04 19:04:12 +0000 2012"

(defn parse-date [str]
  (.parse ^SimpleDateFormat (.get ^ThreadLocal date-parser) str))

(defn maybe-populate-reply [item tweet]
  (let [{:keys [in_reply_to_screen_name in_reply_to_status_id]} tweet]
    (if in_reply_to_status_id
      (merge-with concat item
        {:targets [
          {:id (uri-of-tweet in_reply_to_screen_name in_reply_to_status_id)}]})
      item)))

(defn tweet->item [tweet]
  (let [{:keys [text id  user created_at source] :or {source "web"}}  tweet
        {:keys [name screen_name profile_image_url]}  user
        published (parse-date created_at)
        uri (uri-of-tweet screen_name id)]
    (->
      {:object {:content text
                :id uri
                :source source}
       :actor  {:id (uri-of-twitterer screen_name)
                :title name
                :avatar profile_image_url}
       :published published
       :updated   published
       :id        uri}
       (maybe-populate-reply tweet))))


(def tweets (ref []))
(def listeners (atom {}))

(defn read-stream [source-config callback]
  (let [{:keys [login passwd endpoint params]} source-config
        auth   {:type :basic :user login :password passwd :preemptive true}]
    (with-open [client (httpc/create-client :request-timeout -1 :auth auth)]
      (doseq [s (httpc/string (httpc/stream-seq client :post endpoint :body params))]
        (if (= 0 (rand-int (get source-config :sieve 1)))
          (callback s))))))

(defn tweet->record [tweet source-config]
  (let [tweet (json/read-json tweet)]
    (if (:text tweet)
      {:record-id (:id tweet)
       :source    {:type "twitter", :name (:name source-config)}
       :item      (tweet->item tweet)}
      nil)))

(defn on-tweet [content]
  (log-debug "Tweet DRINKED: " (subs content 0 100) "...")
  (if (not (str/blank? content))
    (dosync (alter tweets conj content))))

(defspout from-endpoint ["record"] {:params [source-config] :prepared true}
  [conf context collector]
  (let [source-name (:name source-config)]

    (->> (Thread. #(read-stream source-config #'on-tweet))
      (.start)
      (swap! listeners assoc source-name))

    (spout
      (nextTuple []
        (dosync
          (if-let [t (first @tweets)]
            (when-let [record (tweet->record t source-config)]
              (emit-spout! collector [(pr-str record)] :id (:record-id record))
              (alter tweets rest))
            (Utils/sleep 100))))
      (ack [id]
        (log-message "Tweet ACKED: " id)))))


(defspout from-file ["record"] {:params [source-config] :prepare true}
  [conf context collector]
  (let [file (:file source-config)
        rdr  (atom (clojure.java.io/reader file))]
    (spout
      (nextTuple []
        (if @rdr
          (if-let [l (binding [*in* @rdr] (read-line))]
            (when-let [record (tweet->record l source-config)]
              (emit-spout! collector [(pr-str record)] :id (:record-id record)))
            (with-open [^java.io.BufferedReader _ @rdr] ; closing @rdr
              (log-message "DONE READING\n")
              (reset! rdr nil)))
          (Utils/sleep 100)))
      (ack [id]
        (log-message "Tweet ACKED: " id)))))