(ns echo.dataserver.rmq
  (:require [clojure.string :as str]
            [clojure.data.json :as json])
  (:use     [backtype.storm clojure])
  (:import  [com.rabbitmq.client ConnectionFactory Connection Channel]
            [echo.dataserver RMQSpout])
  (:gen-class))
(set! *warn-on-reflection* true)

(defn reader [queue-conf]
  (let [{:keys [host port queue exchange] :or {port 5672 exchange ""}} queue-conf]
    (RMQSpout. queue host port)))

(defbolt poster [] {:params [queue-conf] :prepare true}
  [conf context collector]
  (let [{:keys [host port queue exchange] :or {port 5672 exchange ""}} queue-conf
        cf (doto (ConnectionFactory.) (.setHost host) (.setPort port))
        cn (.newConnection cf)
        ch (.createChannel cn)]
    (.queueDeclare ch queue false false false nil)
    (bolt
      (execute [tuple]
        (let [payload (.getString tuple 0)]
          (.basicPublish ch exchange queue nil (.getBytes payload))
          (ack! collector tuple)))
      (cleanup []
        (.close ch)
        (.close cn)))))
