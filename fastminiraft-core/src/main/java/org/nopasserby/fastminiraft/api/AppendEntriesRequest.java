/*
 * Copyright 2021 Guo Chaosheng
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nopasserby.fastminiraft.api;

import java.io.Serializable;
import java.util.List;

public class AppendEntriesRequest implements Serializable {

    private static final long serialVersionUID = 2829205559285260442L;

    /**
     * leader's term
     * */
    private long term = -1;
    
    /**
     * leaderId so follower can redirect clients
     * */
    private String leaderId;
    
    /**
     * index of log entry immediately preceding new ones
     * */
    private long prevLogIndex = -1;
    
    /**
     * term of prevLogIndex entry
     * */
    private long prevLogTerm = -1;
    
    /**
     * leader's commitIndex
     * */
    private long leaderCommitIndex = -1;
    
    /**
     * log entries to store (empty for heartbeat; may send more than on for efficiency)
     * */
    private List<Entry> entries;
    
    public AppendEntriesRequest() {
    }
    
    public AppendEntriesRequest(long term, String leaderId) {
        this.term = term;
        this.leaderId = leaderId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }

    public long getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public void setLeaderCommitIndex(long leaderCommitIndex) {
        this.leaderCommitIndex = leaderCommitIndex;
    }

}
