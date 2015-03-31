(ns logan.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [aws.sdk.s3 :as s3])
  (:gen-class))


(defn get-creds-from-file
  "Get credentials and log-bucket name from file"
  []
  (read-string (slurp "creds")))

(def cred (get-creds-from-file))

(def log-bucket (clojure.string/trim-newline (slurp "bucket")))

(defn pull-elb-log-segment
  "Get 1000 logs from elb log bucket"
  [bucket-name next-marker]
  (s3/list-objects cred bucket-name {:marker next-marker :max-keys 1000}))

(defn get-logs
  "Get log objects"
  [bucket-name next-marker coll]
  (let [log-part (pull-elb-log-segment bucket-name next-marker)]
    (if (:truncated? log-part)
      (let [new-coll (conj coll (:objects log-part))]
        (do 
          (println (:next-marker log-part))
          (get-logs bucket-name (:next-marker log-part) new-coll)))
      (conj coll (:objects log-part)))))

(defn get-all-logs
  "Get all logs as a persistent list for later processing"
  [bucket-name]
  (flatten (get-logs bucket-name nil nil)))

(defn lazy-input
  "Return a lazy sequence from an inputstream"
  [input-stream]
  (let [step (fn step []
               (let [c (.read input-stream)]
                 (when-not (= c -1)
                   (cons (char c) (lazy-seq (step))))))]
    (lazy-seq (step))))

(defn get-content
  "Get the contents of an object key truncates the final newline"
  [key]
  (clojure.string/join "" (drop-last (lazy-input 
                           (:content (s3/get-object cred log-bucket key))))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [test-data (get-logs log-bucket nil nil)]
    (println (type test-data))
    (println (count test-data))
    (println (count (nth test-data 7)))
    (println (type (nth test-data 7)))))
