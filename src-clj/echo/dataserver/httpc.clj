(ns echo.dataserver.httpc
  (:use     [echo.dataserver utils])
  (:import  [org.scribe.model OAuthRequest Token Verb]
            [org.scribe.builder ServiceBuilder]
            [org.scribe.builder.api Api DefaultApi10a])
  (:gen-class))

(def dummy-provider (proxy [DefaultApi10a] []))

(defn request-oauth-2leg [key secret endpoint method params]
  (let [token   (Token. "" "")
        service (-> (ServiceBuilder.)
                    (.apiKey key)
                    (.apiSecret secret)
                    (.provider dummy-provider)
                    (.build))
        request (case method
                  :post (OAuthRequest. Verb/POST endpoint)
                  :get  (OAuthRequest. Verb/GET  endpoint))]
    (doseq [[k v] params]
      (.addBodyParameter request (name k) v))
    (.signRequest service token request)
    (let [response (.send request)]
      {:code (.getCode response)
       :body (.getBody response)})))