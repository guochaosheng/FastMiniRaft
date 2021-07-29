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

package org.nopasserby.fastminirpc.core;

import java.io.Serializable;

public class RpcResponse implements Serializable {

    private static final long serialVersionUID = -8682345638000766687L;
    
    public byte[] methodSignature;
    
    public Object returnObject;
    
    public Exception exception;
    
    public RpcResponse() {
    }

    public RpcResponse(byte[] methodSignature) {
        this.methodSignature = methodSignature;
    }
    
    public RpcResponse(byte[] methodSignature, Object returnObject) {
        this.methodSignature = methodSignature;
        this.returnObject = returnObject;
    }
    
    public RpcResponse(byte[] methodSignature, Exception exception) {
        this.methodSignature = methodSignature;
        this.exception = exception;
    }

    public byte[] getMethodSignature() {
        return methodSignature;
    }

    public void setMethodSignature(byte[] methodSignature) {
        this.methodSignature = methodSignature;
    }

    public Object getReturnObject() {
        return returnObject;
    }

    public void setReturnObject(Object returnObject) {
        this.returnObject = returnObject;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

}
