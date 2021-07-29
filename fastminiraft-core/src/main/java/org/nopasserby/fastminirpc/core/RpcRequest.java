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

public class RpcRequest implements Serializable {

    private static final long serialVersionUID = -4941508714127315996L;

    private byte[] methodSignature;
    
    private Object[] parameters;

    public RpcRequest() {
    }

    public RpcRequest(byte[] methodSignature) {
        this.methodSignature = methodSignature;
    }
    
    public RpcRequest(byte[] methodSignature, Object[] parameters) {
        this.methodSignature = methodSignature;
        this.parameters = parameters;
    }

    public byte[] getMethodSignature() {
        return methodSignature;
    }

    public void setMethodSignature(byte[] methodSignature) {
        this.methodSignature = methodSignature;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }
    
}
