(ns echo.dataserver.core
  (:import [backtype.storm StormSubmitter LocalCluster Config])
  (:require [echo.dataserver.twitter :as twitter])
  (:use [backtype.storm clojure config log])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (log-message "STARTING !!!!!!")
    (.submitTopology cluster "dataserver" {TOPOLOGY-DEBUG false Config/TOPOLOGY_ACKERS 1} (twitter/mk-topology))
    (Thread/sleep 60000)
    (log-message "DONE !!!!!!")
    (.shutdown cluster)))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
   name
   {TOPOLOGY-DEBUG true
    TOPOLOGY-WORKERS 15}
   (twitter/mk-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))
