(require '[clojure.tools.cli :refer [parse-opts]]
        ;;  [clojure.tools.logging :refer :all]
         )

;; Define the CLI options spec
(def opt-spec
  [["-a" "--option-a A" "First option"
    :parse-fn #(Integer/parseInt %)
    :default 10]
   ["-b" "--option-b B" "Second option"
    :parse-fn #(Integer/parseInt %)
    :default nil
    ;; Use assoc-fn to dynamically set :option-b based on :option-a if :option-b is not supplied
    :assoc-fn (fn [m k v]
             (let [updated-m (assoc m k v)]
               (if (nil? (:option-b updated-m))
                 (assoc updated-m :option-b (+ v 5)) ;; Set dynamic default for option-b
                 updated-m)))] ;; Else return map with no changes
    ])

;; Simulating parse-opts call (cannot modify outside of here)
(let [{:keys [options]} (parse-opts ["-a" "20"] opt-spec)]
  (println "Final parsed options:" options))