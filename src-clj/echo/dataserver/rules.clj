(ns echo.dataserver.rules
  (:require [clojure.string :as str]
            [clojure.data.json :as json])
  (:use     [backtype.storm clojure log])
  (:gen-class))
(set! *warn-on-reflection* true)

(def pass? (memoize 
  (fn [conditions]
    (fn [record]
      true)))) ; TODO

(def action-fns {
  :conj-in  (fn [record path val] (update-in record path conj val))
  :assoc-in (fn [record path val] (assoc-in record path val))
  })

(defn act [record action]
  (let [[action & args] action
        action-fn (action-fns action)]
    (apply action-fn record args)))


(defbolt apply-rules ["record"] {:params [rules]}
  [tuple collector]
  (let [record (read-string (.getString tuple 0))]
    (log-debug "CHECKING RECORD: " (:record-id record)) 
    (doseq [{:keys [name conditions actions]} rules]
      (when ((pass? conditions) record)
        (log-debug "RECORD PASSED: " (:record-id record)) 
        (let [record (reduce act record actions)
              record (assoc-in record [:rule :name] name)]
          (emit-bolt! collector [(pr-str record)] :anchor tuple)))))
  (ack! collector tuple))
