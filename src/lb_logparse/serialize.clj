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

(defn filemap
  "Map the tuple of values returned by `vectorize` to the fields provided in
`fields`. The k-tuple returned by `vectorize` requires k fields in the args.
ex.: (filemap tmp :timestamp :field :term :count)"
  [infile & fields]
  (for [v (vectorize infile)]
    (zipmap fields v)))

(def jsonify (generate-string (filemap tmp :timestamp :field :term :count)))





