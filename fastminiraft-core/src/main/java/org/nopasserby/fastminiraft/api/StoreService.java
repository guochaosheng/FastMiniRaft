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

import java.util.concurrent.CompletableFuture;

public interface StoreService {

    public CompletableFuture<Void> set(byte[] key, byte[] value);
    
    public CompletableFuture<Boolean> cas(byte[] key, byte[] expect, byte[] update);
    
    public CompletableFuture<byte[]> get(byte[] key);
    
    public CompletableFuture<Void> del(byte[] key);
    
    public CompletableFuture<Long> add(byte[] body);
    
    public CompletableFuture<byte[]> get(long index);
    
}
