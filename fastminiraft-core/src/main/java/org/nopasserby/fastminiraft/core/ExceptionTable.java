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

package org.nopasserby.fastminiraft.core;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class ExceptionTable {

    public final static NotLeaderException NOT_LEADER = new NotLeaderException(1001, "not leader");
    
    public final static RequestQueueFullException QUEUE_OF_REQUESTS_FULL = new RequestQueueFullException(1002, "queue of requests full");
    
    public final static PendingQueueFullException QUEUE_OF_PENDING_FULL = new PendingQueueFullException(1003, "queue of pending full");
    
    public final static TermChangedException TERM_CHANGED = new TermChangedException(1004, "term changed");
    
    public final static InvalidIndexException INVALID_INDEX = new InvalidIndexException(1005, "invalid index");
    
    public final static AbortException ABORT = new AbortException(9999, "abort");
    
    // 
    private static Map<Integer,  OperationException> map = new HashMap<Integer,  OperationException>();
    
    static {
        for (Field field: ExceptionTable.class.getFields()) {
            try {
                OperationException operationException = (OperationException) field.get(ExceptionTable.class);
                map.put(operationException.getCode(), operationException);
            } catch (IllegalAccessException e) {
            }
        }
    }
    
    public static OperationException getException(int code) {
        return map.get(code);
    }
    
    public static class NotLeaderException extends OperationException {
        
        static final long serialVersionUID = -4362087385448343402L;

        public NotLeaderException(int code, String message) {
            super(code, message);
        }
        
    }
    
    public static class RequestQueueFullException extends OperationException {
        
        static final long serialVersionUID = 8118205530985317362L;

        public RequestQueueFullException(int code, String message) {
            super(code, message);
        }
        
    }
    
    public static class PendingQueueFullException extends OperationException {
        
        static final long serialVersionUID = -7220592701575361251L;

        public PendingQueueFullException(int code, String message) {
            super(code, message);
        }
        
    }
    
    public static class TermChangedException extends OperationException {
        
        static final long serialVersionUID = -2373735348049590322L;

        public TermChangedException(int code, String message) {
            super(code, message);
        }
        
    }
    
    public static class InvalidIndexException extends OperationException {
        
        static final long serialVersionUID = -1442306301779915416L;

        public InvalidIndexException(int code, String message) {
            super(code, message);
        }
        
    }
    
    public static class AbortException extends OperationException {
        
        static final long serialVersionUID = 665272692132308309L;

        public AbortException(int code, String message) {
            super(code, message);
        }
        
    }
    
    public static class OperationException extends RuntimeException {

        static final long serialVersionUID = -4422308685736075382L;
        
        final int code;
        
        final String message;

        public OperationException(int code, String message) {
            super();
            this.code = code;
            this.message = message;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
        
    }

}
