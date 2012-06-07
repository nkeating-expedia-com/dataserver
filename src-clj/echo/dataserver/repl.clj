(ns echo.dataserver.repl
  (:import
    [backtype.storm StormSubmitter LocalCluster Config]
    [echo.dataserver RMQSpout]
    [echo.dataserver ECHOBolt])
  (:require
    [echo.dataserver.twitter :as twitter]
    [clojure.string :as str]
    [clojure.data.json :as json]
    [clojure.data.xml :as xml])
  (:use
    [backtype.storm clojure config log]
    echo.dataserver.twitter
    echo.dataserver.activitystream
    echo.dataserver.utils)
  (:gen-class))
(set! *warn-on-reflection* true)

(defbolt logger [] {:params [out]} [tuple collector]
  (let [as-entry   (.getString tuple 0)
        submit-key (.getString tuple 1)]
    (spit out (str submit-key ": " (pr-str as-entry) "\n") :append true)
    (ack! collector tuple)))

(defn top-submit [host]
  (topology
    {"drink-twitter" (spout-spec (twitter/from-file "tweets.json"))
     "drink-submit"  (spout-spec (RMQSpout. "dataserver.submit" host (int 5672)))}

    {"echobolt" (bolt-spec (ECHOBolt. ))})

(comment
    {"parse-tweet"       (bolt-spec {"drink-twitter" :shuffle}     twitter/tw-parse :p 6)
     "apply-rules"       (bolt-spec {"parse-tweet" :shuffle}       rules/apply-rules :p 6)
     "to-activitystream" (bolt-spec {"apply-rules" :shuffle}       activitystream/json->xml :p 6)
     "to-submit-queue"   (bolt-spec {"to-activitystream" :shuffle} rmq/submit :p 6)
     "to-streamserver"   (bolt-spec {"drink-submit" :shuffle}      (logger "log-submitted.txt") :p 6}))
)

(defn run-local! [host]
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "dataserver" {TOPOLOGY-DEBUG false} (top-submit host))
    (Thread/sleep 60000)
    (.shutdown cluster)))

(defn -main
  ([] (run-local! "prokopov.ul.js-kit.com"))
  ([host] (run-local! host)))
