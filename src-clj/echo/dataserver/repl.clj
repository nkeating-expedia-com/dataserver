(ns echo.dataserver.repl
  (:import
    [backtype.storm StormSubmitter LocalCluster Config]
    [echo.dataserver RMQSpout EchoSubmitBolt])

  (:require
    [echo.dataserver.twitter :as twitter]
    [echo.dataserver.rules :as rules]
    [echo.dataserver.activitystream :as as]
    [echo.dataserver.rmq :as rmq]
    [echo.dataserver.config :as config]
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

(def type->spout-fn {
  "twitter"      #'twitter/from-endpoint
  "twitter-file" #'twitter/from-file
  })

(defn source->spout [source]
  (let [name     (str "drink/" (:type source) "/" (:name source))
        spout-fn (type->spout-fn (:type source))
        spec     (spout-spec (spout-fn source) :p (get source :p 1))] 
    [name spec]))

(defn top-pipeline [env]
  (let [config       (config/load-config (str "conf/" env ".config"))
        {:keys [sources rules]} config
        drinkers     (into {} (map source->spout sources))
        drinkers-out (into {} (map (fn [n] [n :shuffle]) (keys drinkers)))
        rmq-poster   (rmq/poster {:host *rmq-host* :port *rmq-port* :queue *rmq-submit-queue*})
        pipeline {
          "pipeline/apply-rules"     
            (bolt-spec drinkers-out (rules/apply-rules rules) :p 6)
          "pipeline/to-payload"      
            (bolt-spec {"pipeline/apply-rules" :shuffle} as/json->payload :p 6)
          "pipeline/to-submit-queue" 
            (bolt-spec {"pipeline/to-payload" :shuffle} rmq-poster :p 6)
        }]
    (log-message "Starting PIPELINE TOPOLOGY")
    (topology drinkers pipeline)))

(defn top-submit []
  (log-message "Starting SUBMIT TOPOLOGY")
  (topology
    {"submit/drink"   (spout-spec (RMQSpout. *rmq-submit-queue* *rmq-host* *rmq-port*))}
    {"submit/to-echo" (bolt-spec {"submit/drink" :shuffle} (EchoSubmitBolt.) :p 6)
     "submit/to-file" (bolt-spec {"submit/drink" :shuffle} (logger "log/submitted.log") :p 6)}))

(defn run-local! []
  (let [cluster (LocalCluster.)
        args    {TOPOLOGY-DEBUG false}]
    (.submitTopology cluster "ds-submit"   args (top-submit))
    (.submitTopology cluster "ds-pipeline" args (top-pipeline "local"))))

(defn -main
  ([] (run-local!))
  ([host]
    (binding [*rmq-host* host]
      (run-local!))))

; REPL stuff

(use 'echo.dataserver.activitystream)
(use 'echo.dataserver.xml)
(use 'echo.dataserver.twitter)
(use 'echo.dataserver.rules)
(use 'echo.dataserver.config)
