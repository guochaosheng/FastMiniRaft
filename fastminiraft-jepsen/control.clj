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

(defcluster :jepsen
            :clients [{:host "n1" :user "root"}
                      {:host "n2" :user "root"}
                      {:host "n3" :user "root"}
                      {:host "n4" :user "root"}
                      {:host "n5" :user "root"}])

(deftask :date "echo date on server cluster"  []
         (ssh "date"))

(deftask :build []
         (local
           (run
             (cd "/jepsen"
                 (run "mvn -DskipTests clean install")
                 )))
         (local
           (run
             (cd "/jepsen/fastminiraft-jepsen/script"
                 (run "chmod a+x startup.sh")
                 (run "chmod a+x stop.sh")
                 (run "chmod a+x stop_dropcaches.sh")
                 )))
         (local (run "rm jepsen.tar.gz; tar zcvPf jepsen.tar.gz /jepsen/fastminiraft-jepsen/target/fastminiraft-jepsen.jar /jepsen/fastminiraft-jepsen/script/*.sh")))


(deftask :deploy []
         (scp "jepsen.tar.gz" "/root/")
         (ssh
           (run
             (cd "/root"
                 (run "rm -rf /jepsen/")
                 (run "tar zxvPf jepsen.tar.gz")))))