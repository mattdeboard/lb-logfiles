(defproject lb-logparse "1.0.0-SNAPSHOT"
  :description "Parsing DirectEmployers load balancer logfiles"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [cascalog "1.8.7"]
                 [midje-cascalog "0.4.0"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]
  :main lb-logparse.core)
