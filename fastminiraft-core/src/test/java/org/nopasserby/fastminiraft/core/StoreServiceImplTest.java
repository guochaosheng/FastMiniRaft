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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminiraft.util.FileUtil;
import org.nopasserby.fastminirpc.core.RpcClient;

public class StoreServiceImplTest {
    
    @Test
    public void getTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getAppDataPath()); // clear history data
        // leader
        Node node = new Node(options, new RpcClient());
        
        List<Server> servers = new ArrayList<Server>();
        for (Server server: node.getServers()) {
            Options newOptions = new Options();
            newOptions.setServerId(server.getServerId());
            ConsensusModule consensusModule = new ConsensusModule(new Node(newOptions, new RpcClient()), null, null);
            Server newServer = new Server(server.getServerId(), server.getServerHost(), consensusModule);
            servers.add(newServer);
        }
        Node newNode = new Node(options, new RpcClient()) {
            public Iterable<Server> getServers() {
                return servers;
            }
        };
        node = newNode;
        
        // mock election
        node.changeFollowerToPreCandidate();
        long currentTerm = node.getCurrentTerm();
        node.changePreCandidateToCandidate(currentTerm);
        node.incTerm();
        currentTerm = node.getCurrentTerm();
        node.changeCandidateToLeader(currentTerm);
        
        StateMachineModule stateMachineModule = new StateMachineModule(node); 
        ConsensusReadModule consensusReadModule = new ConsensusReadModule(node, stateMachineModule);
        
        String key1 = "Hello", key2 = "Hi", value = "Hello world!";
        CompletableFuture<byte[]> future1 = consensusReadModule.get(key1.getBytes());
        CompletableFuture<byte[]> future2 = consensusReadModule.get(key2.getBytes());
        
        while (!future1.isDone() || !future2.isDone()) {
            consensusReadModule.execute0();
            consensusReadModule.execute1();            
        }
        
        Assert.assertTrue(future1.isCompletedExceptionally());
        Assert.assertTrue(future2.isCompletedExceptionally());
        
        stateMachineModule.getDB().put(key1.getBytes(), value.getBytes());
        ((DBStateMathine) stateMachineModule.getDB()).advanceAppliedIndex(node.getLastLogIndex() + 1);
        
        future1 = consensusReadModule.get(key1.getBytes());
        future2 = consensusReadModule.get(key2.getBytes());
        
        while (!future1.isDone() || !future2.isDone()) {
            consensusReadModule.execute0();
            consensusReadModule.execute1();            
        }
        
        Assert.assertEquals(value, new String(future1.get()));
        Assert.assertNull(future2.get());
        
        consensusReadModule.shutdown();
        stateMachineModule.shutdown();
        
        FileUtil.delete(options.getAppDataPath()); // clear history data
    }

}
