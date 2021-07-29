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

public class AppendEntriesResponse implements Serializable {

    private static final long serialVersionUID = -1252577295846907914L;
    
    /**
     * currentTerm, for leader to update itself
     * */
    private long term;
    
    private long index;
    
    /**
     * true if follower contained entry matching prevLogIndex and prevLogTerm
     * */
    private boolean success;

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }
    
    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
    
}
