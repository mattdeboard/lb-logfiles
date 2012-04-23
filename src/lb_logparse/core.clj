(ns lb-logparse.core
  (:use cascalog.api)
  (:require [clojure.java.io :as o]
            [cascalog [ops :as c] [vars :as v]]
            [clojure.string :as cs])
  (:import (java.net.URLDecode))
  (:gen-class))

(def remote-addr (re-pattern #"[0-9.]+"))
(def timestamp
  ;; Call (second (re-find timestamp <line>)) to get the timestamp.
  (re-pattern #"\[([0-9]{2}/[A-Z][a-z]{2}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-0-9]{4,5})\]"))
(def method-url
  ;; (second) element of returned value is method
  ;; (last) element of returned value is target URL  
  (re-pattern #"(GET|POST|PUT|DELETE) (/[a-z0-9/.]+)"))
(def q-params
  (re-pattern #"([\w]+)=([\w%.+-]+)"))
(def status
  ;; (second) is HTTP status
  ;; (last) is response time in ms
  (re-pattern #"HTTP/1.1\" ([0-9]{3}) ([0-9]{0,5})"))
(def param-vals
  (re-pattern #"([\w_]+):\(([\"\w ]+)\)"))
(defn params [s]
  (vec
   (for [triplet (re-seq q-params s)]
     [(second triplet) (last triplet)])))

(defmapop logl [s]
  [
   (re-find remote-addr s)
   (second (re-find timestamp s))
   (second (re-find method-url s))
   (last (re-find method-url s))
   (second (re-find status s))
   (last (re-find status s))
   (params s)
   ])

(defn logdata
  "Query the source file located at `inpath` and 'destructure' each log line
according to the regexes defined above via the `logl` mapop. This operation
turns a log line into a k-tuple, where k is the number of regexes specified
in `logl`."
  [inpath]
  (let [src (hfs-textline inpath)
        ;; Assign a name to each member of the tuple to which it can later
        ;; be referenced.
        fields ["?remote-addr" "?timestamp" "?method" "?url" "?status"
                "?resp-time" "?params"]]
    (<- fields (src ?c) (logl ?c :>> fields) (:distinct false))))

(defmapcatop explode [l] l)
(defmapop url-decode [k v]
  (if (= k "q")
    (java.net.URLDecoder/decode v)
    v))
(defmapop q-only [k v]
  (if (= k "q")
    [k v]
    '()))
(defmapcatop yieldv [v]
  (re-seq param-vals v))

(defn query-count
  "Query the source file located at `inpath` and return a 4-tuple:
 [timestamp, field, term, count]."
  [inpath]
  (let [data (select-fields (logdata inpath)
                            ["?params" "?timestamp"])]
    (<- [?ts ?a ?b ?ct]
        (data ?param ?ts)
        ;; Convert a collection that looks like [["country" "United States"]]
        ;; ["country "United States"].
        (explode ?param :> ?key-out ?val)
        ;; Convert URL encoding to normal ASCII, e.g. %28foo%29 => (foo)
        (url-decode ?key-out ?val :> ?valout)
        ;; Filter out all parameters except 'q'
        (q-only ?key-out ?valout :> ?key-out ?val-out)
        ;; Split up search terms somewhat imperfectly due to imperfect regex.
        ;; The `param-vals` regex doesn't account for boolean clauses, e.g.
        ;; might miss "India" in e.g. `country:\"United States\" OR "India"`.
        (yieldv ?val-out :> _ ?a ?b)
        ;; Count the number of occurrences of the tuple
        (c/count ?ct)
        ;; Don't drop duplicates.
        (:distinct false))))

(defn -main
  "Execute the `query-count` query against the source file, and output the
results to HDFS."
  [inpath outpath]
  (let [src (query-count inpath)]
    (?<- (hfs-textline outpath :sinkmode :replace)
         [?ts ?f ?v ?counts] (src ?ts ?f ?v ?counts))))
   
