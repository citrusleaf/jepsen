(ns aerospike.nemesisChk
  "Sanity check our nemesis"
  (:require [aerospike.support :as s]
            [aerospike.nemesis :as dbNemesis]
            ;; [clojure.string :as str]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]])
  )


(defn workload []
  {
   :client (client/Client)
   :checker (checker/set)
   :generator (dbNemesis/full-gen {})
  })