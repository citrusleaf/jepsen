(ns aerospike.transact
  "Tests MRTs"
  (:require [aerospike.support :as s]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [independent :as independent]]
            [jepsen.tests.cycle
             [wr :as rw]
             [append :as la]])
  (:import (com.aerospike.client Txn
                                 AerospikeException
                                 AerospikeException$Commit
                                ;;  CommitError
                                 CommitStatus)))


(def txn-set "Set Name for Txn Test" "entries")

(defn txn-wp [tid]
  (let [p (s/write-policy)]
    (set! (.txn p) tid)
    p))

(defn mop!
  "Given a client, transaction-id, and micro-operation,
   Executes micro-op invocation & Returns the completed micro-op."
  [conn tid [f k v]]
  [f k (case f
         :r (-> conn
                (s/fetch s/ans txn-set k tid)
                :bins
                :value
                (or []))
         :w (do
              (let [wp (txn-wp tid)]
                (s/put! conn wp s/ans txn-set k {:value v}))
              v)
         :append (let [wp (txn-wp tid)]
                   (s/list-append! conn wp s/ans txn-set k {:value v})
                   v))])

(defrecord TranClient [client namespace set]
  client/Client
  (open! [this _ node]
    (assoc this :client (s/connect node)))
  (setup! [this _] this)
  (invoke! [this test op]
    (info "Invoking" op)
    (if (= (:f op) :txn)
      (s/with-errors op #{}
        (let [tid (Txn.)
              txn' (atom nil)
              cs (atom nil)]
          (try
            (let [;; wp (txn-wp tid)
                  txn (:value op)
                  txn-res (mapv (partial mop! client tid) txn)]
              (reset! txn' txn-res)
           ;; (info "TRANSACTION!" tid "begin")
           ;; (mapv (partial mop! client wp) txn)
              (info "Txn: " (.getId tid) " ..DONE!")
              (reset! cs (.commit client tid))
              (info "OKAY? "(= (:val cs) CommitStatus/OK))
              (info (:val cs))
              (info "for ref:" CommitStatus/OK)
              (info "COMMITED!")
              (assoc op :type :ok :value @txn'))
           ;; (info  op)
            (catch AerospikeException$Commit e#
              (info "Encountered Commit Error! " (.getResultCode e#) (.getMessage e#))
              ;; (throw e#))
            ;;   (if (or (= (.error e#) CommitError/ROLL_FORWARD_ABANDONED)
            ;;           (= (.error e#) CommitError/CLOSE_ABANDONED))
            ;;     (do (info "COMMITS EVENTUALLY") (assoc op :type :ok, :value @txn'))
                (do (info "FAILURE COMMITTING") (assoc op :type :fail, :error :commit)))
            (catch AerospikeException e#
              (info "Exception caught:" (.getResultCode e#) (.getMessage e#))
              (info "Aborting..")
              (.abort client tid)
              (case (.getResultCode e#)
                29 (do
                     (info "CAUGHT CODE 29 in TranClient.invoke --> ABORTING " (:value op))
                     (assoc op :type :fail, :error :MRT-blocked))
                30 (do
                     (info "CAUGHT CODE 30 in TranClient.invoke --> ABORTING " (:value op))
                     (assoc op :type :fail, :error :read-verify))
                (throw e#))))))
      (info "REGULAR OP!")))
  (teardown! [_ test])
  (close! [this test]
    (s/close client)))


(defn workload []
  {:client (TranClient. nil s/ans "vals")
   :checker (rw/checker)
   :generator (rw/gen {:key-dist :uniform, :key-count 3})})


(defn workload-ListAppend 
  ([]
   (workload-ListAppend {})
  ;; {:client (TranClient. nil s/ans "vals")
  ;;  :checker (la/checker)
  ;;  :generator (la/gen {:key-dist :uniform, :key-count 3})}
  )
  ([opts]
   {:client (TranClient. nil s/ans "vals")
    :checker (la/checker)
    :generator (la/gen 
                {:key-dist        (:key-dist opts :uniform), 
                 :key-count       (:key-count opts 3)
                 :min-txn-length  (:min-txn-length opts 1)
                 :max-txn-length  (:max-txn-length opts 3)
                 })}
   )
)

(defn workload-Tunable [opts]
  {:client (TranClient. nil s/ans "vals")
   :checker (la/checker)
   :generator (la/gen opts)})