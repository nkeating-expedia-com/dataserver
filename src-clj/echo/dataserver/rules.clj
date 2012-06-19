(ns echo.dataserver.rules
  (:require [clojure.string :as str]
            [clojure.data.json :as json])
  (:use     [echo.dataserver utils]
            [backtype.storm clojure log])
  (:gen-class))
(set! *warn-on-reflection* true)

(def compile-expr (memoize 
  (fn [expr] (eval expr))))

(defn check [r c]
  ((compile-expr c) r))

(defn act [r a]
  ((compile-expr a) r))

(defbolt apply-rules ["record"] {:params [rules]} [tuple collector]
  (let [t0     (now-ts)
        record (read-string (.getString tuple 0))]
    (log-debug "CHECKING RECORD: " (:record-id record)) 
    (doseq [{:keys [name condition actions]} rules]
      (when (check record condition)
        (log-debug "RECORD PASSED for '" name "': " (:record-id record) " (" (get-in record [:source :name])  ")") 
        (let [record (-> (reduce act record actions)
                         (assoc-in [:rule :name] name)
                         (assoc-in [:timestamps :rules-in] t0)
                         (assoc-in [:timestamps :rules-out]  (now-ts)))]
          (emit-bolt! collector [(pr-str record)] :anchor tuple)))))
  (ack! collector tuple))
