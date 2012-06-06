(ns echo.dataserver.repl
  (:import
    [backtype.storm StormSubmitter LocalCluster Config]
    [echo.dataserver RMQSpout])
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

(defn mk-topology []
  (topology
   {"drinker" (spout-spec (twitter/twitter-spout "tweets.json" "log-processed.txt"))}
   {"parser" (bolt-spec {"drinker" :shuffle} twitter/tw-parse :p 6)
    "persister" (bolt-spec {"parser" :shuffle} (twitter/as-persist "log-persisted.txt") :p 6)}))

(defn top-submit [host]
  (topology
    {"rmq" (spout-spec (RMQSpout. "dataserver.submit" host (int 5672)))}
    {}))

(defn run-local! [host]
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "dataserver" {TOPOLOGY-DEBUG false} (top-submit host))
    (Thread/sleep 60000)
    (.shutdown cluster)))

(defn -main 
  ([] (run-local! "prokopov.ul.js-kit.com"))
  ([host] (run-local! host)))