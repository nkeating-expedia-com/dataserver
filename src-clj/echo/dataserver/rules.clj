(ns echo.dataserver.rules
  (:require [clojure.string :as str]
            [clojure.data.json :as json])
  (:use     [backtype.storm clojure])
  (:gen-class))
(set! *warn-on-reflection* true)

(defbolt apply-rules ["item" "submit-tokens"] [tuple collector] 
  (let [item (read-string (.getString tuple 0))
        item (merge-with concat item {:targets [:id "http://example.com/dataserver"]})
        submit-tokens {"key" "test-1.js-kit.com"
                       "secret" "5eb609327578195a00f5f47a08a72ae9"}]
    (emit-bolt! collector [(pr-str item) (json/json-str submit-tokens)] :anchor tuple)
    (ack! collector tuple)))
