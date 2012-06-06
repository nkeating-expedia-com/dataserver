(defproject dataserver "0.1.0-SNAPSHOT"
  :aot :all
  :java-source-paths ["src-java"]
  :source-paths      ["src-clj"]
  :main         echo.dataserver.core
  :repl-init    echo.dataserver.repl
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [storm "0.7.2"]
                 [org.clojure/data.json "0.1.2"]
                 [org.clojure/data.xml  "0.0.4"]]
  :jvm-opts     ["-Dfile.encoding=UTF-8"
                 "-Xmx512M"])
                 ; "-Djava.library.path=/System/Library/Java/Extensions:/usr/local/lib:/opt/local/lib:/usr/lib"])
