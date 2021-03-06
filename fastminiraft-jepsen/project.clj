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

(defproject fastminiraft-jepsen-project "0.0.1-SNAPSHOT"
  :description "A jepsen test for fastminiraft"
  :license {:name "Apache License 2.0"}
  :main org.nopasserby.fastminiraft.jepsenclient.core
  :dependencies [
                 [org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.15-SNAPSHOT"]
                 [org.nopasserby/fastminiraft-jepsen "0.1.1"]
                 ]
  :source-paths ["src" "src/main/clojure"]
  :java-source-paths ["src/main/java"])
