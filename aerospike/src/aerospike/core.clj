(ns aerospike.core
  "Entry point for aerospike tests"
  (:require [aerospike [support :as support]
             [counter :as counter]
             [cas-register :as cas-register]
             [nemesis :as nemesis]
             [set :as set]]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as string]
            [jepsen [cli :as cli]
             [control :as c]
             [checker :as checker]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.cli :as cli])
  (:gen-class))

(def txns-enabled
  (string/starts-with? (System/getenv "JAVA_CLIENT_REF") "CLIENT-2848"))

(defn workloads
  "The workloads we can run. Each workload is a map like

      {:generator         a generator of client ops
       :final-generator   a generator to run after the cluster recovers
       :client            a client to execute those ops
       :checker           a checker
       :model             for the checker}

  Or, for some special cases where nemeses and workloads are coupled, we return
  a keyword here instead."
  ([]
  (workloads {})) 
  ([opts]
   (let [res {:cas-register (cas-register/workload)
              :counter      (counter/workload)
              :set          (set/workload)}]
     (if txns-enabled
       (do (require '[aerospike.transact :as transact]) ; for alias only(?)
          ;; add MRT workloads iff client branch supports it
           (assoc res
                  :transact ((requiring-resolve 'transact/workload))
                  :list-append ((requiring-resolve 'transact/workload-ListAppend) opts)))
       ;; otherwise, return SRT workloads only
       res))))

(defn workload+nemesis
  "Finds the workload and nemesis for a given set of parsed CLI options."
  [opts]
  (case (:workload opts)
    {:workload (get (workloads opts) (:workload opts))
     :nemesis  (nemesis/full opts)}))

(defn aerospike-test
  "Constructs a Jepsen test map from CLI options."
  [opts]
  (let [{:keys [workload nemesis]} (workload+nemesis opts)
        {:keys [generator
                final-generator
                client
                checker
                model]} workload
        time-limit (:time-limit opts)
        generator (->> generator
                       (gen/nemesis
                         (->> (:generator nemesis)
                              (gen/delay (if (= :pause (:workload opts))
                                           0 ; The pause workload has its own
                                             ; schedule
                                           (:nemesis-interval opts)))))
                       (gen/time-limit (:time-limit opts)))
        generator (if-not (or final-generator (:final-generator nemesis))
                    generator
                    (gen/phases generator
                                (gen/log "Healing cluster")
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log "Waiting for quiescence")
                                (gen/sleep 10)
                                (gen/clients final-generator)))]
    (info "constructed jepsen test-map")
    (merge tests/noop-test
           opts
           {:name     (str "aerospike " (name (:workload opts)))
            :os       debian/os
            :db       (support/db opts)
            :client   client
            :nemesis  (:nemesis nemesis)
            :generator generator
            :checker  (checker/compose
                      {:perf (checker/perf)
                       :workload checker})
            :model    model})))



(def mrt-opt-spec "Options for Elle-based workloads"
  [[nil "--max-txn-length MAX" "Maximum number of micro-ops per transaction"
    :default 2
    :parse-fn #(Long/parseLong %)
                 ; TODO: must be >= min-txn-length
    :validate [pos? "must be positive"]]
   [nil "--min-txn-length MIN" "Maximum number of micro-ops per transaction"
    :default 2
    :parse-fn #(Long/parseLong %)
                 ; TODO: must be <= min-txn-length
    :validate [pos? "must be positive"]]
   [nil "--key-count N_KEYS" "Number of active keys at any given time"
    :default  3 ; TODO: make this  default differently based on key-dist 
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   [nil "--max-writes-per-key N_WRITES" "Limit of writes to a particular key"
    :default  32 ; TODO: make this  default differently based on key-dist 
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   ])




(def srt-opt-spec
  "Additional command-line options"
  [
   [nil "--workload WORKLOAD" "Test workload to run"
    :parse-fn keyword
    :default :all
    :missing (str "--workload " (cli/one-of (workloads)))
    :validate [(assoc (workloads) :all "all") (cli/one-of (assoc (workloads) :all "all"))]
    ]
   [nil "--max-dead-nodes NUMBER" "Number of nodes that can simultaneously fail"
    :parse-fn #(Long/parseLong %)
    :default  2
    :validate [(complement neg?) "must be non-negative"]]
   [nil "--clean-kill" "Terminate processes with SIGTERM to simulate fsync before commit"
    :default false]
   [nil "--no-revives" "Don't revive during the test (but revive at the end)"
    :default false]
   [nil "--no-clocks" "Allow the nemesis to change the clock"
    :default  false
    :assoc-fn (fn [m k v] (assoc m :no-clocks v))]
   [nil "--no-partitions" "Allow the nemesis to introduce partitions"
    :default  false
    :assoc-fn (fn [m k v] (assoc m :no-partitions v))]
   [nil "--nemesis-interval SECONDS" "How long between nemesis actions?"
    :default 5
    :parse-fn #(Long/parseLong %)
    :validate [(complement neg?) "Must be non-negative"]]
   [nil "--no-kills" "Allow the nemesis to kill processes."
    :default  false
    :assoc-fn (fn [m k v] (assoc m :no-kills v))]
   [nil "--pause-mode MODE" "Whether to pause nodes by pausing the process, or slowing the network"
    :default :process
    :parse-fn keyword
    :validate [#{:process :net :clock} "Must be one of :clock, :process, :net."]]
   [nil "--heartbeat-interval MS" "Aerospike heartbeat interval in milliseconds"
    :default 150
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
  ])

(def opt-spec
  (if txns-enabled
    (cli/merge-opt-specs srt-opt-spec mrt-opt-spec)
    srt-opt-spec))


;; -- Why did we merge in the webserver
;; (defn -main
;;   "Handles command-line arguments, running a Jepsen command."
;;   [& args]
;;   (cli/run! (merge (cli/single-test-cmd {:test-fn   aerospike-test
;;                                          :opt-spec  opt-spec})
;;                    (cli/serve-cmd))
;;             args))


(defn noop-test [opts]
  (merge tests/noop-test
         {:pure-generators true}
         {:name     (str "aerospike set")}))

(defn -main
  "Handles command-line arguments, running a Jepsen command."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd
           {:test-fn   aerospike-test
            :opt-spec  opt-spec})
          (cli/test-all-cmd
           {:test-fn   noop-test
            :opt-spec  opt-spec}))
   args))