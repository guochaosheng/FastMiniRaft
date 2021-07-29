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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminiraft.api.AppendEntriesRequest;
import org.nopasserby.fastminiraft.api.AppendEntriesResponse;
import org.nopasserby.fastminiraft.api.ClientService;
import org.nopasserby.fastminiraft.util.FileUtil;
import org.nopasserby.fastminirpc.core.RpcClient;

public class ClientServiceImplTest {

    @Test
    public void addServerTest() throws Exception {
        String rootdir = System.getProperty("user.dir") + "/data";
        FileUtil.delete(rootdir); // clear history data
        
        String serverCluster = "n1-192.168.0.101:6001;n2-192.168.0.102:6001;n3-192.168.0.103:6001;";
        Options options = new Options();
        options.setServerCluster(serverCluster);
        options.setServerId("n1");
        options.setAppDataPath(rootdir);
        options.setFlushSyncDisk(true);
        options.setBufferCapacityOfEntry(64 * 1024);
        options.setBufferCapacityOfIndex(64 * 1024);
        
        // leader
        Node node = new Node(options, new RpcClient());
        
        List<Server> servers = new ArrayList<Server>();
        Node newNode = new Node(options, new RpcClient()) {
            @Override
            public Iterable<Server> getServers() {
                return servers;
            }
        };
        
        List<LogstoreModule> logstoreModules = new ArrayList<>();
        List<ConsensusModule> consensusModules = new ArrayList<>();
        List<StateMachineModule> stateMachineModules = new ArrayList<>();
        
        LogstoreModule logstoreModule = new LogstoreModule(newNode);
        StateMachineModule stateMachineModule = new StateMachineModule(newNode);
        LeaderReplicator leaderReplicator = new LeaderReplicator(newNode, logstoreModule, stateMachineModule);
        
        ConsensusModule consensusModule = new ConsensusModule(newNode, logstoreModule, stateMachineModule) {
            @Override
            public void appendEntry(int bodyType, byte[] body, BiConsumer<Object, Throwable> action) {
                leaderReplicator.appendEntry(bodyType, body, action);
            }
        };
        logstoreModules.add(logstoreModule);
        consensusModules.add(consensusModule);
        stateMachineModules.add(stateMachineModule);
        
        List<Node> otherNodes = new ArrayList<Node>();
        Map<String, LeaderToFollowerReplicator> otherLeaderToFollowerReplicators = new LinkedHashMap<>();
        Map<String, FollowerReplicator> otherFollowerReplicators = new LinkedHashMap<>();
        
        for (Server server: node.getServers()) {
            
            if (newNode.getServerId().equals(server.getServerId())) {
                servers.add(new Server(server.getServerId(), server.getServerHost(), null));
                continue;
            }
            
            Options otherOptions = new Options();
            otherOptions.setServerCluster(serverCluster);
            otherOptions.setServerId(server.getServerId());
            otherOptions.setAppDataPath(rootdir);
            otherOptions.setFlushSyncDisk(true);
            
            Node otherNode = new Node(otherOptions, new RpcClient());
            
            LogstoreModule otherLogstoreModule = new LogstoreModule(otherNode);
            StateMachineModule otherStateMachineModule = new StateMachineModule(otherNode);
            FollowerReplicator otherFollowerReplicator = new FollowerReplicator(otherNode, otherLogstoreModule, otherStateMachineModule);
            
            ConsensusModule otherConsensusModule = new ConsensusModule(otherNode, otherLogstoreModule, otherStateMachineModule) {
                public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
                    CompletableFuture<AppendEntriesResponse> future = otherFollowerReplicator.appendEntries(request);
                    otherFollowerReplicator.execute0();
                    otherFollowerReplicator.execute1();
                    return future;
                }
            };
            
            Server otherServer = new Server(server.getServerId(), server.getServerHost(), otherConsensusModule);
            servers.add(otherServer);
            
            LeaderToFollowerReplicator leaderToFollowerReplicator = new LeaderToFollowerReplicator(otherServer, newNode, logstoreModule);
            
            otherNodes.add(otherNode);
            otherLeaderToFollowerReplicators.put(server.getServerId(), leaderToFollowerReplicator);
            otherFollowerReplicators.put(server.getServerId(), otherFollowerReplicator);
            
            logstoreModules.add(otherLogstoreModule);
            consensusModules.add(otherConsensusModule);
            stateMachineModules.add(otherStateMachineModule);
        }
        node = newNode;
        
        // mock election
        long currentTerm = node.getCurrentTerm();
        node.changeFollowerToPreCandidate();
        node.changePreCandidateToCandidate(currentTerm);
        node.incTerm();
        currentTerm = node.getCurrentTerm();
        node.changeCandidateToLeader(currentTerm);
        for (Node otherNode: otherNodes) {
            otherNode.changeToFollower(currentTerm);
        }
        
        ClientService clientService = new ClientServiceImpl(consensusModule);
        
        String deletedServerId = "n2";
        CompletableFuture<Void> deleteServerFuture = clientService.removeServer(deletedServerId);
        otherLeaderToFollowerReplicators.remove(deletedServerId);
        
        leaderReplicator.execute0();
        while (!deleteServerFuture.isDone()) {            
            for (LeaderToFollowerReplicator leaderToFollowerReplicator: otherLeaderToFollowerReplicators.values()) {
                leaderToFollowerReplicator.execute();
            }
            leaderReplicator.execute1();
        }
        
        // leader node
        Assert.assertEquals(node.getServerCluster(), "n1-192.168.0.101:6001;n3-192.168.0.103:6001");
        
        for (LeaderToFollowerReplicator leaderToFollowerReplicator: otherLeaderToFollowerReplicators.values()) {
            leaderToFollowerReplicator.execute();
        }
        
        for (Node otherNode: otherNodes) {
            if (otherNode.getServerId().equals(deletedServerId)) {
                continue;
            }
            // follower node
            Assert.assertEquals(otherNode.getServerCluster(), "n1-192.168.0.101:6001;n3-192.168.0.103:6001");
        }
        
        for (LogstoreModule logstore :logstoreModules) {
            logstore.close();
        }
        for (StateMachineModule stateMachine: stateMachineModules) {
            stateMachine.shutdown();
        }
        for (ConsensusModule consensus: consensusModules) {
            consensus.shutdown();
        }
        
        FileUtil.delete(rootdir); // clear history data
    }
    
    @Test
    public void deleteServerTest() throws Exception {
        String rootdir = System.getProperty("user.dir") + "/data";
        FileUtil.delete(rootdir); // clear history data
        
        String serverCluster = "n1-192.168.0.101:6001;n2-192.168.0.102:6001;n3-192.168.0.103:6001;";
        Options options = new Options();
        options.setServerCluster(serverCluster);
        options.setServerId("n1");
        options.setAppDataPath(rootdir);
        options.setFlushSyncDisk(true);
        options.setBufferCapacityOfEntry(64 * 1024);
        options.setBufferCapacityOfIndex(64 * 1024);
        
        // leader
        Node node = new Node(options, new RpcClient());
        
        List<Server> servers = new ArrayList<Server>();
        Node newNode = new Node(options, new RpcClient()) {
            @Override
            public Iterable<Server> getServers() {
                return servers;
            }
        };
        
        List<LogstoreModule> logstoreModules = new ArrayList<>();
        List<ConsensusModule> consensusModules = new ArrayList<>();
        List<StateMachineModule> stateMachineModules = new ArrayList<>();
        
        LogstoreModule logstoreModule = new LogstoreModule(newNode);
        StateMachineModule stateMachineModule = new StateMachineModule(newNode);
        LeaderReplicator leaderReplicator = new LeaderReplicator(newNode, logstoreModule, stateMachineModule);
        
        ConsensusModule consensusModule = new ConsensusModule(newNode, logstoreModule, stateMachineModule) {
            @Override
            public void appendEntry(int bodyType, byte[] body, BiConsumer<Object, Throwable> action) {
                leaderReplicator.appendEntry(bodyType, body, action);
            }
        };
        logstoreModules.add(logstoreModule);
        consensusModules.add(consensusModule);
        stateMachineModules.add(stateMachineModule);
        
        List<Node> otherNodes = new ArrayList<Node>();
        Map<String, LeaderToFollowerReplicator> otherLeaderToFollowerReplicators = new LinkedHashMap<>();
        Map<String, FollowerReplicator> otherFollowerReplicators = new LinkedHashMap<>();
        
        for (Server server: node.getServers()) {
            
            if (newNode.getServerId().equals(server.getServerId())) {
                servers.add(new Server(server.getServerId(), server.getServerHost(), null));
                continue;
            }
            
            Options otherOptions = new Options();
            otherOptions.setServerCluster(serverCluster);
            otherOptions.setServerId(server.getServerId());
            otherOptions.setAppDataPath(rootdir);
            otherOptions.setFlushSyncDisk(true);
            
            Node otherNode = new Node(otherOptions, new RpcClient());
            
            LogstoreModule otherLogstoreModule = new LogstoreModule(otherNode);
            StateMachineModule otherStateMachineModule = new StateMachineModule(otherNode);
            FollowerReplicator otherFollowerReplicator = new FollowerReplicator(otherNode, otherLogstoreModule, otherStateMachineModule);
            
            ConsensusModule otherConsensusModule = new ConsensusModule(otherNode, otherLogstoreModule, otherStateMachineModule) {
                public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
                    CompletableFuture<AppendEntriesResponse> future = otherFollowerReplicator.appendEntries(request);
                    otherFollowerReplicator.execute0();
                    otherFollowerReplicator.execute1();
                    return future;
                }
            };
            
            Server otherServer = new Server(server.getServerId(), server.getServerHost(), otherConsensusModule);
            servers.add(otherServer);
            
            LeaderToFollowerReplicator leaderToFollowerReplicator = new LeaderToFollowerReplicator(otherServer, newNode, logstoreModule);
            
            otherNodes.add(otherNode);
            otherLeaderToFollowerReplicators.put(server.getServerId(), leaderToFollowerReplicator);
            otherFollowerReplicators.put(server.getServerId(), otherFollowerReplicator);
            
            logstoreModules.add(otherLogstoreModule);
            consensusModules.add(otherConsensusModule);
            stateMachineModules.add(otherStateMachineModule);
        }
        node = newNode;
        
        // mock election
        long currentTerm = node.getCurrentTerm();
        node.changeFollowerToPreCandidate();
        node.changePreCandidateToCandidate(currentTerm);
        node.incTerm();
        currentTerm = node.getCurrentTerm();
        node.changeCandidateToLeader(currentTerm);
        for (Node otherNode: otherNodes) {
            otherNode.changeToFollower(currentTerm);
        }
        
        ClientService clientService = new ClientServiceImpl(consensusModule);
        
        CompletableFuture<Void> addServerFuture = clientService.addServer("n4", "192.168.0.104:6001");
        
        leaderReplicator.execute0();
        while (!addServerFuture.isDone()) {            
            for (LeaderToFollowerReplicator leaderToFollowerReplicator: otherLeaderToFollowerReplicators.values()) {
                leaderToFollowerReplicator.execute();
            }
            leaderReplicator.execute1();
        }
        
        Assert.assertEquals(node.getServerCluster(), "n1-192.168.0.101:6001;n2-192.168.0.102:6001;n3-192.168.0.103:6001;n4-192.168.0.104:6001");
        
        for (LeaderToFollowerReplicator leaderToFollowerReplicator: otherLeaderToFollowerReplicators.values()) {
            leaderToFollowerReplicator.execute();
        }
        
        for (Node otherNode: otherNodes) {
            Assert.assertEquals(otherNode.getServerCluster(), "n1-192.168.0.101:6001;n2-192.168.0.102:6001;n3-192.168.0.103:6001;n4-192.168.0.104:6001");
        }
        
        for (LogstoreModule logstore :logstoreModules) {
            logstore.close();
        }
        for (StateMachineModule stateMachine: stateMachineModules) {
            stateMachine.shutdown();
        }
        for (ConsensusModule consensus: consensusModules) {
            consensus.shutdown();
        }
        
        FileUtil.delete(rootdir); // clear history data
    }
    
}
