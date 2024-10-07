(ns scratch (:require [jepsen [cli :as jc]]))

(defn all-combos
  "Takes a map of options to collections of values for that option. Computes a
  collection of maps with the combinatorial expansion of every possible option
  value."
  ([opts]
   (all-combos {} opts))
  ([m opts]
   (if (seq opts)
     (let [[k vs] (first opts)]
       (mapcat (fn [v]
                 (all-combos (assoc m k v) (next opts)))
               vs))
     (list m))))

(defn all-workload-options
  "Expands workload-options into all possible CLI opts for each combination of
  workload options."
  [workload-options]
  (mapcat (fn [[workload opts]]
            (all-combos {:workload workload} opts))
          workload-options))


(def all-nemeses
  "All nemesis specs to run as a part of a complete test suite."
  (->> (concat
         ; No faults
        [[]])
       ; Convert to maps like {:fault-type true}
       (map (fn [faults] (zipmap faults (repeat true))))))

(def nemesis-specs
  "These are the types of failures that the nemesis can perform."
  #{:partition
    :partition-one
    :partition-pd-leader
    :partition-half
    :partition-ring
    :kill
    :pause
    :kill-pd
    :kill-kv
    :kill-db
    :pause-pd
    :pause-kv
    :pause-db
    :schedules
    :shuffle-leader
    :shuffle-region
    :random-merge
    :clock-skew
    ; Special-case generators
    :slow-primary
    :restart-kv-without-pd})


(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword o) true]))
         (into {}))))


(def workload-options
  "For each workload, a map of workload options to all values that option
  supports."
  {:append          {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :read-lock         [nil "FOR UPDATE"]
                     :predicate-read    [true false]}
   :bank            {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :update-in-place   [true false]
                     :read-lock         [nil "FOR UPDATE"]}
   :bank-multitable {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :update-in-place   [true false]
                     :read-lock         [nil "FOR UPDATE"]}
   :long-fork       {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :use-index         [true false]}
   :monotonic       {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :use-index         [true false]}
   :register        {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :read-lock         [nil "FOR UPDATE"]
                     :use-index         [true false]}
   :set             {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]}
   :set-cas         {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :read-lock         [nil "FOR UPDATE"]}
   :sequential      {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]}
   :table           {}})


(def cli-opts
  "Command line options for tools.cli"
  [[nil "--faketime MAX_RATIO"
    "Use faketime to skew clock rates up to MAX_RATIO"
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "should be a positive number"]]

   [nil "--force-reinstall" "Don't re-use an existing TiDB directory"]

   [nil "--nemesis-interval SECONDS"
    "Roughly how long to wait between nemesis operations. Default: 10s."
    :parse-fn parse-long
    :assoc-fn (fn [m k v] (update m :nemesis assoc :interval v))
    :validate [(complement neg?) "should be a non-negative number"]]

   [nil "--nemesis SPEC" "A comma-separated list of nemesis types"
    :default {:interval 10}
    :parse-fn parse-nemesis-spec
    :assoc-fn (fn [m k v] (update m :nemesis merge v))
    :validate [(fn [parsed]
                 (and (map? parsed)
                      (every? nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure "
                    (.toLowerCase (jc/one-of nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]

   [nil "--nemesis-long-recovery" "Every so often, have a long period of no faults, to see whether the cluster recovers."
    :default false
    :assoc-fn (fn [m k v] (update m :nemesis assoc :long-recovery v))]

   [nil "--nemesis-schedule SCHEDULE" "Whether to have randomized delays between nemesis actions, or fixed ones."
    :parse-fn keyword
    :assoc-fn (fn [m k v] (update m :nemesis assoc :schedule v))
    :validate [#{:fixed :random} "Must be either 'fixed' or 'random'"]]


   [nil "--recovery-time SECONDS"
    "How long to wait for cluster recovery before final ops."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   ["-v" "--version VERSION" "What version of TiDB should to install"
    :default "3.0.0-beta.1"]

   [nil "--tarball-url URL" "URL to TiDB tarball to install, has precedence over --version"
    :default nil]])


(def workloads
  "A map of workload names to functions that can take CLI opts and construct
  workloads."
  {:bank            nil})


(def test-all-opts
  "CLI options for running the entire test suite."
  [[nil "--quick" "Runs a limited set of workloads and nemeses for faster testing."
    :default false]

   ["-w" "--workload NAME"
    "Test workload to run. If omitted, runs all workloads"
    :parse-fn keyword
    :default nil
    :validate [workloads (jc/one-of workloads)]]

   [nil "--only-workloads-expected-to-pass"
    "If present, skips tests which are not expected to pass, given Fauna's docs"
    :default false]])


(defn test-all-cmd
  "A command that runs a whole suite of tests in one go."
  []
  {"test-all"
   {:opt-spec (concat jc/test-opt-spec cli-opts test-all-opts)
    :opt-fn   jc/test-opt-fn
    :usage    "Runs all combinations of workloads, nemeses, and options."
    :run      (fn [{:keys [options]}]
                (info "CLI options:\n" (with-out-str (pprint options)))
                (let [w         (:workload options)
                      workload-opts (cond
                                      :else
                                      workload-options)
                      workloads (cond->> (all-workload-options workload-opts)
                                  w (filter (comp #{w} :workload)))

                      tests (for [workload  workloads
                                  i         (range (:test-count options))]
                              (do
                                (-> options
                                    (merge workload)
                                    (update :nemesis merge nemesis))))]
                  (->> tests
                       (map-indexed
                        (fn [i test-opts]
                          (try
                            (info "\n\n\nTest " (inc i) "/" (count tests))
                            (jepsen/run! (test test-opts))
                            (catch Exception e
                              (warn e "Test crashed; moving on...")))))
                       dorun)))}})

(test-all-cmd)


