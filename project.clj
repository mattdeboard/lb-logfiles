(defproject lb-logparse "1.0.0-SNAPSHOT"
  :description "Cascalog job for parsing logfiles"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.10.0"]
                 [midje-cascalog "0.4.0"]
                 [org.clojure/data.json "0.2.0"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}}
  :main lb-logparse.core)
