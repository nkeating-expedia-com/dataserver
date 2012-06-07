(ns echo.dataserver.twitter
  (:use [backtype.storm clojure log]
        [echo.dataserver utils])
  (:import [java.text SimpleDateFormat])
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

(defspout from-file ["tweet"] {:params [in] :prepared true}
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
        (log-message "ACKED: " id "\n")))))

(defbolt tw-parse ["item"] [tuple collector]
  (let [tweet (json/read-json (.getString tuple 0))]
    (when (tweet :text)
      (emit-bolt! collector [(pr-str (tweet->item tweet))] :anchor tuple)))
    (ack! collector tuple))

(def tweets (ref []))
(def listeners (atom {}))

(defn on-tweet [content]
  (log-message (str "Tweet received:" content))
  (if (not (str/blank? content))
    (dosync (alter tweets conj content))))

(defn read-stream [endpoint-conf]
  (let [{:keys [login passwd endpoint params]} endpoint-conf
        auth   {:type :basic :user login :password passwd :preemptive true}
        client (httpc/create-client :auth auth)
        resp   (httpc/stream-seq client :post endpoint :body params)]
    (doseq [s (httpc/string resp)]
      (on-tweet s))))

(defspout from-endpoint ["tweet"] {:params [name endpoint-conf] :prepared true}
  [conf context collector]

  (let [t (Thread. #(read-stream endpoint-conf))]
    (.start t)
    (swap! listeners assoc name t))

  (spout
    (nextTuple []
      (dosync
        (if-let [t (first @tweets)]
          (do
            (emit-spout! collector [t] :id (+ 100000 (rand-int 899999)))
            (alter tweets rest))
          (Thread/sleep 100))))
    (ack [id]
      (log-message "ACKED: " id "\n"))))