(ns lb-logparse.core
  (:use cascalog.api)
  (:require [clojure.java.io :as o]
            [cascalog [ops :as c] [vars :as v]]
            [clojure.string :as cs])
  (:import (java.net.URLDecode))
  (:gen-class))

(def sl "108.46.21.116, 10.183.252.24 - - [07/Dec/2012:23:05:26 -0500] \"GET /api/markup/1119/9780547148342/344/ HTTP/1.0\" 200 28 \"http://berkeley.courseload.com/\" \"Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11\" \"sessionid=fac5789e1a12f61e7e638784e749809e; csrftoken=805618b2677eb232b6a7263cb0ed47c6\"")
(def fp "/home/matt/lb-logfiles/resources/apachesample.log")
(def remote-addr (re-pattern #"[0-9.]+, [0-9.]+"))
(def timestamp
  ;; Call (second (re-find timestamp <line>)) to get the timestamp.
  (re-pattern #"\[[0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+\-0-9]{4,5}\]"))
(def method-url
  ;; (second) element of returned value is method
  ;; (last) element of returned value is target URL  
  (re-pattern #"(GET|POST|PUT|DELETE) (/[a-z0-9/.]+)"))
(def q-params
  (re-pattern #"([\w]+)=([\w%.+-]+)"))
(def status
  ;; (second) is HTTP status
  ;; (last) is response time in ms
  (re-pattern #"HTTP/1.[0-1]\" ([0-9]{3}) ([0-9]{0,5})"))
(def domain
  (re-pattern #"https?+://[\w./]+"))
;; (def param-vals
;;   (re-pattern #"([\w_]+):\(([\"\w ]+)\)"))
(defn params [s]
  (vec
   (for [triplet (re-seq q-params s)]
     [(second triplet) (last triplet)])))

(defmapop logl [s]
  [
   (re-find remote-addr s)
   (re-find timestamp s)
   (second (re-find method-url s))
   (last (re-find method-url s))
   (second (re-find status s))
   (last (re-find status s))
   (params s)
   ])

(defmapop split-ips [urls]
  (seq (clojure.string/split urls #", ")))

(deffilterop is-pageread? [path]
  (let [ptn #"/api/log/pageread/[0-9]+/[-a-zA-Z0-9_]+/"]
    (re-find ptn path)))

(defmapop page-and-slug [path]
  (let [ptn #"/api/log/pageread/([0-9]+)/([-a-zA-Z0-9_]+)/"]
    (rest (re-find ptn path))))

(defn logdata
  "Query the source file located at `inpath` and 'destructure' each log line
according to the regexes defined above via the `logl` mapop. This operation
turns a log line into a k-tuple, where k is the number of regexes specified
in `logl`."
  [inpath]
  (let [src (lfs-textline inpath)
        ;; Assign a name to each member of the tuple to which it can later
        ;; be referenced.
        fields ["?remote-addr" "?timestamp" "?method" "?url" "?status"
                "?resp-time" "?params"]]
    (<- fields
        (src ?c)
        (logl ?c :>> fields)
        (:distinct false))))

(defn page-visits
  [inpath]
  (let [src (logdata inpath)]
    (?<- (lfs-textline "resources/what" :sinkmode :replace)
         [?page ?slug ?a ?ct]
         (src ?remote-addr _ _ ?url _ _ _)
         (split-ips ?remote-addr :> ?a _)
         (is-pageread? ?url :> ?url)
         (page-and-slug ?url :> ?page ?slug)
         (c/count ?ct)
         (:distinct false))))

(defmapcatop explode [l] l)

(defmapop url-decode [k v]
  (if (= k "q")
    (java.net.URLDecoder/decode v)
    v))
;;         (split-ips ?remote-addr :> ?hostip ?lbip)
  
(defmapop q-only [k v]
  (if (= k "q")
    [k v]
    '()))
;; (defmapcatop yieldv [v]
;;   (re-seq param-vals v))

;; (defn prn-tuple
;;   (let [data (logdata sl)]
;;     (?<- (lfs-textline "resources/what") [] (data ?tuple) ?tuple)))

;; (defn -main
;;   "Execute the `query-count` query against the source file, and output the
;; results to HDFS."
;;   [inpath outpath]
;;   (let [src (query-count inpath)]
;;     (?<- (hfs-textline outpath :sinkmode :replace)
;;          [?ts ?f ?v ?counts] (src ?ts ?f ?v ?counts))))
   
;; (defn -main
;;   [inpath]
;;   (let [src (logdata inpath)]
;;     (?<- (stdout) [?ts]
;;          (src _ ?ts _ _ _ _ _)
;;          (c/count ?ct)
;;          (:distinct false))))

;; Original fns:
;; (defn query-count
;;   "Query the source file located at `inpath` and return a 4-tuple:
;;  [timestamp, field, term, count]."
;;   [inpath]
;;   (let [data (select-fields (logdata inpath)
;;                             ["?params" "?timestamp"])]
;;     (<- [?ts ?a ?b ?ct]
;;         (data ?param ?ts)
;;         ;; Convert a collection that looks like [["country" "United States"]]
;;         ;; ["country "United States"].
;;         (explode ?param :> ?key-out ?val)
;;         ;; Convert URL encoding to normal ASCII, e.g. %28foo%29 => (foo)
;;         (url-decode ?key-out ?val :> ?valout)
;;         ;; Filter out all parameters except 'q'
;;         (q-only ?key-out ?valout :> ?key-out ?val-out)
;;         ;; Split up search terms somewhat imperfectly due to imperfect regex.
;;         ;; The `param-vals` regex doesn't account for boolean clauses, e.g.
;;         ;; might miss "India" in e.g. `country:\"United States\" OR "India"`.
;;         (yieldv ?val-out :> _ ?a ?b)
;;         ;; Count the number of occurrences of the tuple
;;         (c/count ?ct)
;;         ;; Don't drop duplicates.
;;         (:distinct false))))

;; (defn -main
;;   "Execute the `query-count` query against the source file, and output the
;; results to HDFS."
;;   [inpath outpath]
;;   (let [src (query-count inpath)]
;;     (?<- (hfs-textline outpath :sinkmode :replace)
;;          [?ts ?f ?v ?counts] (src ?ts ?f ?v ?counts))))
   
