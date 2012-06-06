(ns echo.dataserver.activitystream
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.data.xml :as xml])
  (:gen-class))

(defrecord NsElement [ns tag attrs content])
(defn ns-element [ns tag & [attrs & content]]
  (NsElement. ns tag (or attrs {}) (remove nil? content)))

(extend-protocol xml/Emit
  NsElement
  (xml/emit-element [e writer]
    (let [nspace (name (:ns e))
          qname  (name (:tag e))
          ^javax.xml.stream.XMLStreamWriter writer writer]
      (.writeStartElement writer (or nspace "yyy") (str "xxx" qname) nspace)
      (xml/write-attributes e writer)
      (doseq [c (:content e)]
        (xml/emit-element c writer))
      (.writeEndElement writer))))

(def activity-ns "activity")

(def default-object
  {:object-type "http://activitystrea.ms/schema/1.0/comment"
   :title ""})
(def default-actor 
  {:object-type "http://activitystrea.ms/schema/1.0/person"})
(def default-author {})
(def default-entry 
  {:verb "http://activitystrea.ms/schema/1.0/post"})

(defn entry [entry]
  (let [{:keys [object actor author]} entry
        object (merge default-object object)
        actor  (merge default-actor  actor)
        author (merge default-author author)
        entry  (merge default-entry {:object object, :actor actor, :author author})]
    (ns-element :activity :entry {}
      (ns-element :activity :verb {} (:verb entry))
      (xml/element :object {}
        (ns-element :activity :object-type {} (get-in entry [:object :object-type]))
        (ns-element :activity :id          {} (get-in entry [:object :id]))
        (ns-element :activity :title       {} (get-in entry [:object :title])))
    )
  ))
