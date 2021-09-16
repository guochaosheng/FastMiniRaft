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

package org.nopasserby.fastminiraft.benchmark;

import java.util.concurrent.CompletableFuture;

import org.nopasserby.fastminiraft.api.ClientService;
import org.nopasserby.fastminiraft.api.ConsensusService;
import org.nopasserby.fastminiraft.api.StoreService;
import org.nopasserby.fastminiraft.boot.ServiceSerializer;
import org.nopasserby.fastminiraft.core.ClientServiceImpl;
import org.nopasserby.fastminiraft.core.ConsensusModule;
import org.nopasserby.fastminiraft.core.LogstoreModule;
import org.nopasserby.fastminiraft.core.Node;
import org.nopasserby.fastminiraft.core.Options;
import org.nopasserby.fastminiraft.core.StateMachineModule;
import org.nopasserby.fastminiraft.core.StoreServiceImpl;
import org.nopasserby.fastminirpc.core.RpcClient;
import org.nopasserby.fastminirpc.core.RpcServer;

public class ThroughputTestServer {     
    
    public static void main(String[] args) throws Exception {
        
        StatsThroughput statsThroughput = new StatsThroughput();
        
        Options options = new Options();
        
        ServiceSerializer serviceSerializer = new ServiceSerializer();
        RpcClient rpcClient = new RpcClient(serviceSerializer);
        
        Node node = new Node(options, rpcClient);
        LogstoreModule logstore = new LogstoreModule(node);
        StateMachineModule stateMachineModule = new StateMachineModule(node);
        if (stateMachineModule.getServerCluster() != null) {
            options.setServerCluster(stateMachineModule.getServerCluster());
            node.reload(options);
        }
        
        ConsensusModule consensusModule = new ConsensusModule(node, logstore, stateMachineModule);
        
        StoreService storeService = new StoreServiceImpl(consensusModule, stateMachineModule) {
            @Override
            public CompletableFuture<Long> add(byte[] body) {
                CompletableFuture<Long> future = super.add(body);
                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        statsThroughput.getReceiveResponseFailedCount().incrementAndGet();
                    } else {
                        statsThroughput.getReceiveResponseSuccessCount().incrementAndGet();
                    }
                });
                return future;
            }
        };
        ClientService clientService = new ClientServiceImpl(consensusModule);
        
        RpcServer rpcServer = new RpcServer(options.getServerHost(), serviceSerializer);
        rpcServer.registerService(ConsensusService.class, consensusModule);
        rpcServer.registerService(StoreService.class, storeService);
        rpcServer.registerService(ClientService.class, clientService);
        
        statsThroughput.startup();
        rpcServer.startup();
    }
    
}
