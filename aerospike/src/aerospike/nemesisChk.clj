(ns aerospike.nemesisChk
  "Sanity check our nemesis"
  (:require [aerospike.support :as s]
            [aerospike.nemesis :as dbNemesis]
            ;; [clojure.string :as str]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]])
  (:import (clojure.lang ExceptionInfo)
           (com.aerospike.client AerospikeClient
                                 AerospikeException
                                 AerospikeException$Connection
                                 AerospikeException$Timeout
                                 Bin
                                 Info
                                 Key
                                 Record)
           (com.aerospike.client.cluster Node)
           (com.aerospike.client.policy Policy
                                          ;; ConsistencyLevel
                                        GenerationPolicy
                                        WritePolicy)))

(defrecord myClient [client namespace set]
  client/Client
  (open! [this test node] (assoc this :client (s/connect node)))
  (setup! [this test] this)
  (invoke! [this test op] (assoc op :type :fail))
  (teardown! [this test])
  (close! [this test] (s/close client)) 
  )

(defn dumbClient []
  (myClient. nil s/ans "foo"))

(defn workload []
  {:client (dumbClient)
;;    :checker (checker/set)
;;    :generator (dbNemesis/full-gen {})
  })