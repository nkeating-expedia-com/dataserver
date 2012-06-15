(ns echo.dataserver.repl
  (:import
    [backtype.storm StormSubmitter LocalCluster Config]
    [backtype.storm.generated TopologySummary Nimbus$Client KillOptions]
    [echo.dataserver EchoSubmitBolt])
  (:require
    [echo.dataserver.twitter :as twitter]
    [echo.dataserver.rules :as rules]
    [echo.dataserver.activitystream :as as]
    [echo.dataserver.rmq :as rmq]
    [echo.dataserver.config :as config]
    [clojure.string :as str]
    [clojure.set :as set]
    [clojure.data.json :as json]
    [clojure.data.xml :as xml])
  (:use
    [backtype.storm clojure config log thrift]
    echo.dataserver.utils)
  (:gen-class))
(set! *warn-on-reflection* true)

(def ^:dynamic *rmq-host* "prokopov.ul.js-kit.com")
(def ^:dynamic *rmq-port* (int 5672))
(def ^:dynamic *rmq-items-queue*  "dataserver.items")
(def ^:dynamic *rmq-submit-queue* "dataserver.submit")
(def ^:dynamic *kill-timeout-sec* 5)

(defbolt fs-logger [] {:params [out]} [tuple collector] 
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
        out        (bolt-spec {spout-name :shuffle} out-bolt :p 1)]
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
            (bolt-spec {"pipeline/in" :shuffle} (rules/apply-rules rules) :p 2)
          "pipeline/to-payload"      
            (bolt-spec {"pipeline/apply-rules" :shuffle} as/json->payload :p 2)
          "pipeline/out" 
            (bolt-spec {"pipeline/to-payload" :shuffle} out-bolt :p 1)
        }]
    (topology
      {"pipeline/in" spout}
      pipeline)))

(defn top-submit [config]
  (log-message "Starting SUBMIT TOPOLOGY")
  (topology
    {"submit/in"       (spout-spec (rmq/reader {:host *rmq-host* :port *rmq-port* :queue *rmq-submit-queue*}))}
    (merge
      {"submit/out-echo" (bolt-spec {"submit/in" :shuffle} (EchoSubmitBolt.) :p 10)}
      (if config/*debug*
        {"submit/out-file" (bolt-spec {"submit/in" :shuffle} (fs-logger "log/submitted.log") :p 1)}
        {}))))

(defn topologies [cfg]
  (let [config (config/load-config cfg)
        build  (config/build)]
    (into {} (merge
      (for [source (:sources config)]
        [(str build "__drink__" (:name source) "__" (hash source)) (top-drinker source)])
      [(str build "__submit") (top-submit config)]
      [(str build "__pipeline__" (hash (:rules config))) (top-pipeline config)]))))

(defn run-local! [cfg]
  (let [cluster (LocalCluster.)
        args    {TOPOLOGY-DEBUG false}]
    (doseq [[name top] (topologies cfg)]
      (.submitTopology cluster name args top))))

(defn -toplist [^Nimbus$Client nimbus]
  (let [cluster-info (.getClusterInfo nimbus)
        topologies   (.get_topologies cluster-info)]
    (into {} 
      (for [^TopologySummary t topologies]
           [(.get_name t) {:status  (keyword (.get_status t))
                           :tasks   (.get_num_tasks t)
                           :workers (.get_num_workers t)
                           :uptime  (.get_uptime_secs t)}]))))

(defn toplist []
  (with-configured-nimbus-connection nimbus
    (-toplist nimbus)))

(defn -kill [^Nimbus$Client nimbus name]
  (let [opts (KillOptions.)]
    (.set_wait_secs opts *kill-timeout-sec*)
    (.killTopologyWithOpts nimbus name opts)
    (log-message "Killed topology: " name)))

(defn kill
  ([]
    (with-configured-nimbus-connection nimbus
      (doseq [name (keys (-toplist nimbus))]
        (-kill nimbus name))))
  ([name]
    (with-configured-nimbus-connection nimbus
      (-kill nimbus name))))

(defn topsync [cfg]
  (with-configured-nimbus-connection nimbus
    (let [args {TOPOLOGY-DEBUG false
                Config/TOPOLOGY_MESSAGE_TIMEOUT_SECS *kill-timeout-sec*}
          tops    (topologies cfg)
          torun   (set (keys tops))
          running (set (keys (-toplist nimbus)))]
      (log-message "Killing topologies out of sync")
      (doseq [name (set/difference running torun)]
        (-kill nimbus name))
      (log-message "Waiting for topologies to die")
      (Thread/sleep (* 1000 *kill-timeout-sec*))
      (log-message "Starting new topologies")
      (doseq [name (set/difference torun running)]
        (log-message "Starting topology: " name)
        (StormSubmitter/submitTopology name args (tops name)))
      (log-message "Synchronization finished"))))


(defn -main
  ([] (run-local! "conf/local.config"))
  ([cfg] (topsync cfg)))

; REPL stuff

(use 'echo.dataserver.activitystream)
(use 'echo.dataserver.xml)
(use 'echo.dataserver.twitter)
(use 'echo.dataserver.rules)
(use 'echo.dataserver.config)
