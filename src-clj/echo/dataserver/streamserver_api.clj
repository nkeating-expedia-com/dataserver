(ns echo.dataserver.streamserver-api
  (:use     [echo.dataserver utils]
            [backtype.storm clojure log])
  (:require [echo.dataserver.httpc :as httpc]
            [clojure.string :as str]
            [clojure.data.json :as json])
  (:gen-class))
(set! *warn-on-reflection* true)

(defn decide [{:keys [code body]}]
  (cond
    (= 401 code)                ; OAuth
      (let [error (:errorCode (json/read-json body))]
        (if (= "oauth_nonce_already_used" error)
          :retry
          :discard))
    (#{408 429} code) :retry    ; Request Timeout; Too Many Requests
    (= 501 code)      :discard  ; Not Implemented
    (< 200 code 299)  :ack
    (< 400 code 499)  :discard
    (< 500 code 599)  :retry))

(defn submit [xml submit-tokens]
  (let [{:keys [key secret endpoint]} submit-tokens]
    (httpc/request-oauth-2leg key secret endpoint :post {:content xml})))

(defbolt submit-bolt [] [tuple collector]
  (try
    (let [t0        (now-ts)
          record    (json/read-json (.getString tuple 0))
          response  (submit (:xml record) (:submit-tokens record))
          t1        (now-ts)]
      (case (decide response)
        :ack      (do
                    (log-debug "Submitted " response)
                    (ack! collector tuple))
        :retry    (do
                    (log-warn "Failed, RETRYING " response)
                    (fail! collector tuple))
        :discard  (do
                    (log-error nil "Failed, DISCARDING " response)
                    (ack! collector tuple))))
    (catch Exception e
      (log-error e "CRASHED")
      (ack! collector tuple))))