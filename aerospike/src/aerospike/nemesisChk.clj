(ns aerospike.nemesisChk
  "Sanity check our nemesis"
  (:require [aerospike.support :as s]
            [aerospike.nemesis :as dbNemesis]
            ;; [clojure.string :as str]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]])
  )

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

(defn r   [_ _] {:type :invoke, :f :read})
(defn add [_ _] {:type :invoke, :f :add, :value 1})

(defn workload []
  {:client (dumbClient)
   :checker (checker/counter)
   :generator (->> (repeat 1000 add)
                   (gen/delay 120))
  })