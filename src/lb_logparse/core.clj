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

(defn logdata [inpath]
  (let [src (hfs-textline inpath)
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

(defn query-count [inpath]
  (let [data (select-fields (logdata inpath)
                            ["?params" "?timestamp"])]
    (<- [?a ?b ?ct]
        (data ?param ?ts)
        (explode ?param :> ?key-out ?val)
        (url-decode ?key-out ?val :> ?valout)
        (q-only ?key-out ?valout :> ?key-out ?val-out)
        (yieldv ?val-out :> _ ?a ?b)
        (c/count ?ct)
        (:distinct false))))

(defn -main [inpath outpath]
  (let [src (query-count inpath)]
    (?<- (hfs-textline outpath :sinkmode :replace)
         [?f ?v ?counts] (src ?f ?v ?counts))))
   
