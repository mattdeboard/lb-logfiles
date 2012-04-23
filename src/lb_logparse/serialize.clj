(ns lb-logparse.serialize
  (:use cheshire.core)
  (:require [clojure.string :as cs]
            [clojure.java.io :as io]))

(def tmp "/home/matt/lb-logparse/resources/json.log")

(defn vectorize
  "Convert each line of the input file to a vector of values as strings."
  [infile]
  (for [l (line-seq (io/reader (io/as-file infile)))]
    (vec (cs/split l #"\t"))))

(defn filemap [coll & fields] (for [v coll] (zipmap fields v)))

(def jsonify (generate-string (filemap (vectorize tmp)
                                       :timestamp :field :term :count)))

