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

import java.util.concurrent.CompletableFuture;
import org.nopasserby.fastminiraft.api.ClientService;
import org.nopasserby.fastminiraft.util.FunctionUtil;

public class ClientServiceImpl implements ClientService {

    private Node node;
    
    private ConsensusModule consensusModule;
    
    public ClientServiceImpl(ConsensusModule consensusModule) {
        this.consensusModule = consensusModule;
        this.node = consensusModule.getNode();
    }

    @Override
    public CompletableFuture<String> getLeaderId() {
        return CompletableFuture.completedFuture(node.getLeaderId());
    }

    @Override
    public CompletableFuture<String> getServerCluster() {
        return CompletableFuture.completedFuture(node.getServerCluster());
    }
    
    /**
     * Add node steps:
     * step 1: Call the addServer interface to request the addition of a server node
     * step 2: Start a new server node
     * step 3: Check whether the latest node configuration of all nodes is correct,
     *         only one node is allowed to be added or deleted each time, 
     *         and the next node addition or deletion operation is not allowed 
     *         before the previous one is confirmed to be completed
     * */
    @Override
    public CompletableFuture<Void> addServer(String serverId, String serverHost) {
        CompletableFuture<Void> future = new CompletableFuture<Void>();
        byte[] coded = LogstoreProto.ADD_SER.getCodec().encode(serverId, serverHost);
        consensusModule.appendEntry(LogstoreProto.ADD_SER.getCode(), coded, FunctionUtil.newConsumer(future, Void.class));
        return future;
    }
    
    /**
     * Add node steps:
     * step 1: Kill target server node
     * step 2: Call the removeServer interface to request the deletion of a server node
     * step 3: Check whether the latest node configuration of all nodes is correct,
     *         only one node is allowed to be added or deleted each time, 
     *         and the next node addition or deletion operation is not allowed 
     *         before the previous one is confirmed to be completed
     * */
    @Override
    public CompletableFuture<Void> removeServer(String serverId) {
        CompletableFuture<Void> future = new CompletableFuture<Void>();
        byte[] coded = LogstoreProto.DEL_SER.getCodec().encode(serverId);
        consensusModule.appendEntry(LogstoreProto.DEL_SER.getCode(), coded, FunctionUtil.newConsumer(future, Void.class));
        return future;
    }

}
