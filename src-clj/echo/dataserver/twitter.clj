(ns echo.dataserver.twitter
  (:use     [backtype.storm clojure log]
            [echo.dataserver utils])
  (:import  [java.text SimpleDateFormat]
            [backtype.storm.utils Utils])
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [http.async.client :as httpc]
            [echo.dataserver.config :as config])
  (:gen-class))
(set! *warn-on-reflection* true)

(defn tweet-url [name id]
  (str "https://twitter.com/" name "/status/" id))

(defn user-url [name]
  (str "https://twitter.com/" name))

(defthreadlocal date-parser 
  (doto
    (SimpleDateFormat. "E MMM d HH:mm:ss Z yyyy" (java.util.Locale. "en"))
    (.setTimeZone (java.util.TimeZone/getTimeZone "GMT")))) ; "Mon Jun 04 19:04:12 +0000 2012"

(defn parse-date [str]
  (.parse ^SimpleDateFormat (.get ^ThreadLocal date-parser) str))

(defn maybe-populate-reply [item tweet]
  (let [{parent-user :in_reply_to_screen_name
         parent-id   :in_reply_to_status_id  } tweet]
    (if parent-id
      (update-in item [:targets] conj {:url (tweet-url parent-user parent-id)
                                       :id  parent-id})
      item)))

(defn expand-tco [tweet]
  (let [{{:keys [urls hashtags user_mentions media]} :entities} tweet
        replacements 
          (concat
            (for [{href :expanded_url, text :display_url, [f t] :indices} urls]
                 [f t (str "<a href='" href "'>" text "</a>")])
            (for [{href :expanded_url, src :media_url_https, {{h :h w :w} :small} :sizes, [f t] :indices} media]
                 [f t (str "\n<a href='" href "'><img src='" src "' width='" w "' height='" h "'/></a>\n")]))]
    (reduce (fn [s [f t r]] (str (subs s 0 f) r (subs s t)))
            (:text tweet)
            (reverse (sort replacements)))))

(defn tweet-content [tweet]
  (if-let [source-tweet (:retweeted_status tweet)]
    (let [author (get-in source-tweet [:user :screen_name])
          text   (expand-tco source-tweet)]
      (str "RT @" author ": " text))
  (expand-tco tweet)))

(defn tweet->item [tweet]
  (let [{:keys [text id user created_at source] :or {source "web"}} tweet
        {:keys [name screen_name profile_image_url]}  user
        published (parse-date created_at)
        url (tweet-url screen_name id)]
    (->
      {:object {:content (tweet-content tweet)
                :url url
                :id  id
                :source source}
       :actor  {:id  (:id user) 
                :url (user-url screen_name)
                :name screen_name
                :displayName name
                :avatar profile_image_url}
       :source  {:name "Twitter"
                 :icon "http://cdn.js-kit.com/images/favicons/twitter.png"
                 :url  url}
       :published published
       :updated   published}
       (maybe-populate-reply tweet))))

(defn tweet->items [tweet]
  (if (not (:text tweet))
    []
    [(tweet->item tweet)]))

;     (if-let [source-tweet (:retweeted_status tweet)]
;       (let  [source-item  (tweet->item source-tweet)
;              {{source-user :name} :actor 
;               {source-id :id 
;                source-url :url}   :object} source-item 
;              item (-> (tweet->item tweet)
;                       (assoc-in [:targets] [{:id  source-id
;                       :url source-url}]))]
;         [source-item item])

(defn read-stream [source-config callback]
  (let [{:keys [login passwd endpoint params]} source-config
        auth   {:type :basic :user login :password passwd :preemptive true}]
    (with-open [client (httpc/create-client :request-timeout -1 :auth auth)]
      (doseq [content (httpc/string (httpc/stream-seq client :post endpoint :body params))]
        (when (and (not (str/blank? content))
                   (= 0 (rand-int (get source-config :sieve 1))))
          (log-debug "Tweet DRINKED: " (str-head content 100) "...")
          (when config/*debug*
            (spit "log/tweets.log" content :append true))
          (callback content))))))

(defn tweet->records [tweet source-config]
  (let [tweet (json/read-json tweet)]
    (for [item (tweet->items tweet)]
      {:record-id  (get-in item [:object :id])
       :source     {:type "twitter", :name (:name source-config)}
       :item       item
       :timestamps {:source-out (now-ts)}})))

(def reader-threads (atom {}))

(defspout from-endpoint ["record"] {:params [source-config] :prepared true}
  [conf context collector]
  (let [source-name (:name source-config)
        queue       (ref [])
        queue-put   (fn [t] (dosync (alter queue conj t)))]

    (->> (Thread. #(read-stream source-config queue-put))
      (.start)
      (swap! reader-threads assoc source-name))

    (spout
      (nextTuple []
        (if-let [t (first @queue)]
          (dosync
            (doseq [record (tweet->records t source-config)]
              (emit-spout! collector [(pr-str record)] :id (:record-id record))
              (alter queue rest)))
          (Utils/sleep 100)))
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
            (doseq [record (tweet->records l source-config)]
              (emit-spout! collector [(pr-str record)] :id (:record-id record)))
            (with-open [^java.io.BufferedReader _ @rdr] ; closing @rdr
              (log-message "DONE READING\n")
              (reset! rdr nil)))
          (Utils/sleep 100)))
      (ack [id]
        (log-message "Tweet ACKED: " id)))))
