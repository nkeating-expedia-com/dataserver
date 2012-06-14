(defproject dataserver "0.1.0-SNAPSHOT"
  :aot :all
  :java-source-paths ["src-java"]
  :source-paths      ["src-clj"]
  :resource-paths    ["src-resources" "~/.storm/"]
  :uberjar-name "dataserver.jar"
  :main         echo.dataserver.repl
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/data.json "0.1.2"]
                 [org.clojure/data.xml  "0.0.4"]
                 [com.rabbitmq/amqp-client "2.8.2"]
                 [com.googlecode.json-simple/json-simple "1.1"]
                 [org.scribe/scribe "1.3.0"]
                 [http.async.client "0.4.5"]]
  :profiles      {:dev {:dependencies [[storm "0.7.2"]]}}
  :jvm-opts     ["-Dfile.encoding=UTF-8" 
                 "-Xmx128M"
                 "-XX:ReservedCodeCacheSize=48m"
                 "-XX:InitialCodeCacheSize=32m"
                 "-Dstorm.jar=target/dataserver.jar"])
