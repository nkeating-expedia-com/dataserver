(ns echo.dataserver.repl
  (:import
    [backtype.storm StormSubmitter LocalCluster Config]
    [echo.dataserver EchoSubmitBolt])

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
(def ^:dynamic *rmq-items-queue*  "dataserver.items")
(def ^:dynamic *rmq-submit-queue* "dataserver.submit")

(defbolt logger [] {:params [out]} [tuple collector] 
  (let [payload (.getString tuple 0)]
    (spit out payload :append true)
    (ack! collector tuple)))

(def type->spout-fn {
  "twitter"      #'twitter/from-endpoint
  "twitter-file" #'twitter/from-file
  })

(defn top-drinker [source]
  (let [p          (get source :p 1)
        prefix     (str "drink/" (:type source) "/" (:name source))
        spout-name (str prefix "/in")
        spout-fn   (type->spout-fn (:type source))
        spout      (spout-spec (spout-fn source) :p p)
        out-name   (str prefix "/out")
        out-bolt   (rmq/poster {:host *rmq-host* :port *rmq-port* :queue *rmq-items-queue*})
        out        (bolt-spec {spout-name :shuffle} out-bolt :p p)]
    (log-message "Starting " prefix " TOPOLOGY")
    (topology 
      {spout-name spout} 
      {out-name   out})))

(defn top-pipeline [config]
  (log-message "Starting PIPELINE TOPOLOGY")
  (let [rules    (:rules config)
        spout    (spout-spec (rmq/reader {:host *rmq-host* :port *rmq-port* :queue *rmq-items-queue*}))
        out-bolt (rmq/poster {:host *rmq-host* :port *rmq-port* :queue *rmq-submit-queue*})
        pipeline {
          "pipeline/apply-rules"     
            (bolt-spec {"pipeline/in" :shuffle} (rules/apply-rules rules) :p 6)
          "pipeline/to-payload"      
            (bolt-spec {"pipeline/apply-rules" :shuffle} as/json->payload :p 6)
          "pipeline/out" 
            (bolt-spec {"pipeline/to-payload" :shuffle} out-bolt :p 6)
        }]
    (topology
      {"pipeline/in" spout}
      pipeline)))

(defn top-submit [config]
  (log-message "Starting SUBMIT TOPOLOGY")
  (topology
    {"submit/in"       (spout-spec (rmq/reader {:host *rmq-host* :port *rmq-port* :queue *rmq-submit-queue*}))}
    {"submit/out-echo" (bolt-spec {"submit/in" :shuffle} (EchoSubmitBolt.) :p 6)
     "submit/out-file" (bolt-spec {"submit/in" :shuffle} (logger "log/submitted.log") :p 6)}))

(defn run-local! []
  (let [env     "local"
        cluster (LocalCluster.)
        args    {TOPOLOGY-DEBUG false}
        config  (config/load-config (str "conf/" env ".config"))]
    (doseq [source (:sources config)
            :let [name (str "ds-drink---" (:type source) "---" (:name source))]]
      (.submitTopology cluster name        args (top-drinker source)))
    (.submitTopology cluster "ds-submit"   args (top-submit config))
    (.submitTopology cluster "ds-pipeline" args (top-pipeline config))
))

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
