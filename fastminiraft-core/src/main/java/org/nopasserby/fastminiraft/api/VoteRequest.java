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

public class VoteRequest implements Serializable {
    
    private static final long serialVersionUID = -6798233825617921468L;

    /** 
     * candidate's term
     * */
    private long term;

    /** 
     * candidate requesting vote
     * */
    private String candidateId;

    /** 
     * index of candidate's last log entry
     * */
    private long lastLogIndex;

    /** 
     * term of candidate's last log entry
     * */
    private long lastLogTerm;
    
    /**
     * pre-vote
     * */
    private boolean prepare;
    
    public VoteRequest() {
    }

    public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm, boolean prepare) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.prepare = prepare;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public boolean isPrepare() {
        return prepare;
    }

    public void setPrepare(boolean prepare) {
        this.prepare = prepare;
    }

    @Override
    public String toString() {
        return "VoteRequest [term=" + term + ", candidateId=" + candidateId + ", lastLogIndex=" + lastLogIndex
                + ", lastLogTerm=" + lastLogTerm + ", prepare=" + prepare + "]";
    }
    
}
