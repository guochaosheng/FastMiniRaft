
; Licensed to the Apache Software Foundation (ASF) under one or more
; contributor license agreements.  See the NOTICE file distributed with
; this work for additional information regarding copyright ownership.
; The ASF licenses this file to You under the Apache License, Version 2.0
; (the "License"); you may not use this file except in compliance with
; the License.  You may obtain a copy of the License at
;
;     http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;
; See https://github.com/openmessaging/openmessaging-dledger-jepsen/blob/master/src/dledger_jepsen_test/core.clj
;
 
(ns org.nopasserby.fastminiraft.jepsenclient.core
    (:require [clojure.tools.logging :refer :all]
              [clojure.string :as cstr]
              [knossos.model :as model]
              [jepsen [cli :as cli]
               [control :as c]
               [db :as db]
               [tests :as tests]
               [checker :as checker]
               [client :as client]
               [generator :as gen]
               [independent :as independent]
               [nemesis :as nemesis]]
              [jepsen.checker.timeline :as timeline]
              [jepsen.control.util :as cu]
              [jepsen.os :as os])
    (:import [org.nopasserby.fastminiraft.jepsenclient JepsenClient]))

  (defonce raft-script-path "/jepsen/fastminiraft-jepsen/script")
  (defonce raft-port 26001)
  (defonce raft-entry-buffer (* 64 1024 1024))
  (defonce raft-index-buffer (* 32 1024 1024))
  (defonce raft-bin "java")
  (defonce raft-start "startup.sh")
  (defonce raft-stop "stop.sh")
  (defonce raft-stop-dropcaches "stop_dropcaches.sh")
  (defonce raft-data-path "/jepsen/fastminiraft-jepsen/data")
  (defonce raft-log-path "/jepsen/fastminiraft-jepsen/log")

  (defn server-id [node]
    (str node))

  (defn server-str [node]
    (str (server-id node) "-" node ":" raft-port))

    (defn server-cluster
      "Constructs an initial cluster string for a test, like
      \"n0-host1:26001;n1-host2:26001,...\""
      [test]
      (->> (:nodes test)
           (map (fn [node]
                  (server-str node)))
           (cstr/join ";")))
    
    (defn vm-args [test node]
      (str "-Dserver.id=" (server-id node) 
              " -Dserver.cluster=" (server-cluster test) 
              " -Dapp.data.path=" raft-data-path 
              " -Dentry.buffer.capacity=" raft-entry-buffer
              " -Dindex.buffer.capacity=" raft-index-buffer))

    (defn start! [test node]
      (info "Start Raft Server" node)
      (c/cd raft-script-path
            (c/exec :sh
                    raft-start
                    (vm-args test node))))

    (defn stop! [node]
      (info "Stop Raft Server" node)
      (c/cd raft-script-path
            (c/exec :sh
                    raft-stop)))

    (defn stop_dropcaches! [node]
      (info "Stop Raft Server and drop caches" node)
      (c/cd raft-script-path
            (c/exec :sh
                    raft-stop)))

    (defn- create-client [test]
      (doto (JepsenClient. "jepsen" (server-cluster test))
        (.startup)))

    (defn- start-client [client]
      (-> client
          :conn
          (.startup)))

    (defn- shutdown-client [client]
      (-> client
          :conn
          (.shutdown)))

    (defn r   [_ _] {:type :invoke, :f :read, :value nil})
    (defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
    (defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

    (defn- read
      "Get value from raft server by key."
      [client key]
        (-> client
          :conn
          (.get (pr-str key))))

    (defn- write
      "Write a key/value to raft server"
      [client key value]
      (-> client
          :conn
          (.set (pr-str key) value)))

    (defn- compare-and-set
      "Compare and set value for key"
      [client key old new]
      (-> client
          :conn
          (.compareAndSet (pr-str key) old new)))

    (defn db
      "Raft Database."
      []
      (reify db/DB
        (setup! [_ test node]
          (start! test node)
          (Thread/sleep 20000)
          )

        (teardown! [_ test node]
          (stop! node)
          (Thread/sleep 20000)
          (c/exec :rm
                  :-rf
                  raft-data-path))))

    (defrecord Client [conn]
      client/Client
      (open! [this test node]
        (-> this
            (assoc :node node)
            (assoc :conn (create-client test))))

      (setup! [this test])

      (invoke! [this test op]
        (let [[k v] (:value op)
              crash (if (= :read (:f op)) :fail :info)]
          (try
            (case (:f op)
              :read (assoc op :type :ok :value (independent/tuple k (read this k)))
              :write (do
                       (write this k v)
                       (assoc op :type :ok))
              :cas (let [[old new] v]
                     (assoc op :type (if (compare-and-set this k old new)
                                       :ok
                                       :fail))))
            (catch Exception e
              (let [code (.getErrorCode e)]
                (cond
                  (= code 301) (assoc op :type crash, :error "metadata error")
                  (= code 401) (assoc op :type crash, :error "timeout error")
                  (= code 501) (assoc op :type crash, :error "connect error")
                  (= code 601) (assoc op :type crash, :error "unknown error")
                  :else
                  (assoc op :type crash :error e)))))))

      (teardown! [this test])

      (close! [this test]
        (shutdown-client this)))

    (defn mostly-small-nonempty-subset
      "Returns a subset of the given collection, with a logarithmically decreasing
      probability of selecting more elements. Always selects at least one element.

          (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
               repeatedly
               (map count)
               (take 10000)
               frequencies
               sort)
          ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
      [xs]
      (-> xs
          count
          inc
          Math/log
          rand
          Math/exp
          long
          (take (shuffle xs))))

    (def crash-random-nodes
      "A nemesis that crashes a random subset of nodes."
      (nemesis/node-start-stopper
        mostly-small-nonempty-subset
        (fn start [test node]
          (info "Crash start" node)
          (stop_dropcaches! node)
          [:killed node])
        (fn stop [test node]
          (info "Crash stop" node)
          (start! test node)
          [:restarted node])))

    (def kill-random-processes
      "A nemesis that kills a random subset of processes."
      (nemesis/node-start-stopper
        mostly-small-nonempty-subset
        (fn start [test node]
          (info "Kill start" node)
          (stop! node)
          [:killed node])
        (fn stop [test node]
          (info "Kill stop" node)
          (start! test node)
          [:restarted node])))

    (def nemesis-map
      "A map of nemesis names to functions that construct nemesis, given opts."
      {"partition-random-halves"           (nemesis/partition-random-halves)
       "partition-random-node"             (nemesis/partition-random-node)
       "kill-random-processes"             kill-random-processes
       "crash-random-nodes"                crash-random-nodes
       "hammer-time"                       (nemesis/hammer-time raft-bin)
       "bridge"                            (nemesis/partitioner (comp nemesis/bridge shuffle))
       "partition-majorities-ring"         (nemesis/partition-majorities-ring)})

    (defn- parse-int [s]
      (Integer/parseInt s))

    (def cli-opts
      "Additional command line options."
      [["-r" "--rate HZ" "Approximate number of requests per second, per thread."
        :default  10
        :parse-fn read-string
        :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
       [nil "--nemesis NAME" "What nemesis should we run?"
        :default  "partition-random-halves"
        :validate [nemesis-map (cli/one-of nemesis-map)]]
       ["-i" "--interval TIME" "How long is the nemesis interval?"
        :default  15
        :parse-fn parse-int
        :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
       [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
        :default  100
        :parse-fn parse-int
        :validate [pos? "Must be a positive integer."]]]
      )


    (defn raft-test
      "Given an options map from the command line runner (e.g. :nodes, :ssh,
      :concurrency ...), constructs a test map."
      [opts]
      (let [nemesis (get nemesis-map (:nemesis opts))]
        (merge tests/noop-test
               opts
               {:name          "raft"
                :os            os/noop
                :db            (db)
                :client        (Client. nil)
                :nemesis       nemesis
                :checker       (checker/compose
                                 {:perf     (checker/perf)
                                  :indep (independent/checker
                                           (checker/compose
                                             {:timeline (timeline/html)
                                              :linear   (checker/linearizable
                                                          {:model (model/cas-register)})}))})
                :generator     (->> (independent/concurrent-generator
                                      (:concurrency opts 5)
                                      (range)
                                      (fn [k]
                                        (->> (gen/mix [r w cas])
                                             (gen/stagger (/ (:rate opts)))
                                             (gen/limit (:ops-per-key opts)))))
                                    (gen/nemesis
                                      (gen/seq (cycle [(gen/sleep (:interval opts))
                                                       {:type :info, :f :start}
                                                       (gen/sleep (:interval opts))
                                                       {:type :info, :f :stop}])))
                                    (gen/time-limit (:time-limit opts)))})))

    (defn -main
      "Handles command line arguments. Can either run a test, or a web server for
      browsing results."
      [& args]
      (cli/run! (merge (cli/single-test-cmd {:test-fn raft-test
                                             :opt-spec cli-opts})
                       (cli/serve-cmd))
                args))
