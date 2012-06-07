(ns echo.dataserver.repl
  (:import
    [backtype.storm StormSubmitter LocalCluster Config]
    [echo.dataserver RMQSpout]
    [echo.dataserver ECHOBolt])
  (:require
    [echo.dataserver.twitter :as twitter]
    [echo.dataserver.rules :as rules]
    [echo.dataserver.activitystream :as as]
    [echo.dataserver.rmq :as rmq]
    [clojure.string :as str]
    [clojure.data.json :as json]
    [clojure.data.xml :as xml])
  (:use
    [backtype.storm clojure config log]
    echo.dataserver.utils)
  (:gen-class))
(set! *warn-on-reflection* true)

(def ^:dynamic *rmq-host* "prokopov.ul.js-kit.com")
(def ^:dynamic *rmq-port* (int 5672))
(def ^:dynamic *rmq-submit-queue* "dataserver.submit")

(defbolt logger [] {:params [out]} [tuple collector] 
  (let [payload (.getBinary tuple 0)]
    (with-open [o (clojure.java.io/output-stream out :append true)]
      (.write o payload)
      (.write o (.getBytes "\n")))
    (ack! collector tuple)))

(defn top-submit []
  (let [f (clojure.java.io/file "log-submitted.txt")]
    (when (.exists f)
      (.delete f)))
  (topology
    {"drink-twitter" (spout-spec (twitter/from-file "tweets.json"))
     "drink-submit"  (spout-spec (RMQSpout. *rmq-submit-queue* *rmq-host* *rmq-port*))}

    {"parse-tweet"       (bolt-spec {"drink-twitter" :shuffle} twitter/tw-parse :p 6)
     "apply-rules"       (bolt-spec {"parse-tweet" :shuffle}   rules/apply-rules :p 6)
     "to-payload"        (bolt-spec {"apply-rules" :shuffle}   as/json->payload :p 6)
     "to-submit-queue"   (bolt-spec {"to-payload" :shuffle}    (rmq/poster {:host *rmq-host* :port *rmq-port* :queue *rmq-submit-queue*}) :p 6)
     "to-file"           (bolt-spec {"drink-submit" :shuffle}  (logger "log-submitted.txt") :p 6)
     "to-submit-api"     (bolt-spec {"drink-submit" :shuffle}  (ECHOBolt.) :p 6)}))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "dataserver" {TOPOLOGY-DEBUG false} (top-submit))))

(defn -main 
  ([] (run-local!))
  ([host] 
    (binding [*rmq-host* host]
      (run-local!))))

(use 'echo.dataserver.activitystream)
(use 'echo.dataserver.xml)
(use 'echo.dataserver.twitter)
(use 'echo.dataserver.rules)
