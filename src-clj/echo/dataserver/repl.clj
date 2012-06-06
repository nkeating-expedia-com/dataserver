(ns echo.dataserver.repl
  (:import
    [backtype.storm StormSubmitter LocalCluster Config])
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

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "dataserver" {TOPOLOGY-DEBUG false} (twitter/mk-topology))
    (Thread/sleep 10000)
    (.shutdown cluster)))

(defn -main []
   (run-local!))