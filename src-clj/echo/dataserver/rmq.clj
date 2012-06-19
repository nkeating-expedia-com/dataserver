(ns echo.dataserver.rmq
  (:require [clojure.string :as str]
            [clojure.data.json :as json])
  (:use     [backtype.storm clojure log])
  (:import  [com.rabbitmq.client ConnectionFactory Connection Channel QueueingConsumer])
  (:gen-class))
(set! *warn-on-reflection* true)

(def default-queue-conf {:port 5672 :exchange ""})
(def ^:dynamic *timeout* 100)

(defn channel ^Channel [conf]
 (let [cf (doto (ConnectionFactory.) 
                (.setHost (:host conf))
                (.setPort (:port conf)))
       cn (.newConnection cf)
       ch (.createChannel cn)]
    (.queueDeclare ch (:queue conf) false false false nil)
    ch))

(defspout reader ["data"] {:params [queue-conf] :prepare true}
  [conf context collector]
  (let [conf     (merge queue-conf default-queue-conf)
        ch       (channel conf)
        consumer (QueueingConsumer. ch)
        queue    (:queue conf)]
    (.basicConsume ch queue true consumer)
    (spout
      (nextTuple []
        (if-let [delivery (.nextDelivery consumer *timeout*)]
          (let [data (String. (.getBody delivery))
                id   (+ 100000 (rand-int 899999))] ; FIXME
            (emit-spout! collector [data] :id id))))
      (ack [id]
        (log-debug "RMQ message ACKED: " queue ":" id)))))


(defbolt poster [] {:params [queue-conf] :prepare true}
  [conf context collector]
  (let [conf     (merge queue-conf default-queue-conf)
        ch       (channel conf)
        exchange (:exchange conf)
        queue    (:queue conf)]
      (bolt
        (execute [tuple]
          (let [payload (.getString tuple 0)]
            (.basicPublish ch exchange queue nil (.getBytes payload))
            (ack! collector tuple)))
        (cleanup []
          (.close (.getConnection ch))
          (.close ch)))))
