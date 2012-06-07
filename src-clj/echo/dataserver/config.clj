(ns echo.dataserver.config
  (:use echo.dataserver.utils))
(set! *warn-on-reflection* true)

(defn twitter-streams [] {
  "sample" {:login    (env "DATASERVER_TWITTER_LOGIN") 
            :passwd   (env "DATASERVER_TWITTER_PASSWD")
            :endpoint "https://stream.twitter.com/1/statuses/sample.json"}
  })