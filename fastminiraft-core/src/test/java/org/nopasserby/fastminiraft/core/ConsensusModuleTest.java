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
import org.nopasserby.fastminiraft.api.AppendEntriesRequest;
import org.nopasserby.fastminiraft.api.AppendEntriesResponse;
import org.nopasserby.fastminiraft.api.Entry;
import org.nopasserby.fastminiraft.api.Logstore;
import org.nopasserby.fastminiraft.util.FileUtil;
import org.nopasserby.fastminiraft.util.FunctionUtil;
import org.nopasserby.fastminirpc.core.RpcClient;

public class ConsensusModuleTest {

    @Test
    public void leaderReplicatorExecuteTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getAppDataPath()); // cleanup history
        
        int queueDepthOfRequests = 4;
        options.setQueueDepthOfRequests(queueDepthOfRequests);
        options.setBufferCapacityOfEntry(128 * 1024);
        options.setBufferCapacityOfIndex(128 * 1024);
        Node node = new Node(options, new RpcClient());
        Logstore logstore = new LogstoreModule(node);
        StateMachineModule stateMachine = new StateMachineModule(node);
        LeaderReplicator leaderReplicator = new LeaderReplicator(node, logstore, stateMachine);
        
        // mock election
        node.changeFollowerToPreCandidate();
        long currentTerm = node.getCurrentTerm();
        node.changePreCandidateToCandidate(currentTerm);
        node.incTerm();
        currentTerm = node.getCurrentTerm();
        node.changeCandidateToLeader(currentTerm);
        
        long initLogIndex = node.getLastLogIndex();
        
        String body = "Hello world!";
        
        List<CompletableFuture<Long>> appendFutureList = new ArrayList<CompletableFuture<Long>>();
        for (int i = 0; i < queueDepthOfRequests; i++) {
            CompletableFuture<Long> future = new CompletableFuture<Long>();
            appendFutureList.add(future);
            byte[] coded = LogstoreProto.ADD.getCodec().encode(body.getBytes());
            leaderReplicator.appendEntry(LogstoreProto.ADD.getCode(), coded, FunctionUtil.newConsumer(future, Long.class));
        }
        
        CompletableFuture<Long> future = new CompletableFuture<Long>();
        byte[] coded = LogstoreProto.ADD.getCodec().encode(body.getBytes());
        leaderReplicator.appendEntry(LogstoreProto.ADD.getCode(), coded, FunctionUtil.newConsumer(future, Long.class));
        
        Assert.assertTrue(future.isCompletedExceptionally());// queue of requests full
        
        leaderReplicator.execute0();
        Assert.assertEquals(initLogIndex + queueDepthOfRequests, logstore.getLastLogIndex());
        
        // mock update servers match index
        for (Server server:node.getServers()) {
            node.updateMatchIndex(node.getCurrentTerm(), server.getServerId(), logstore.getLastLogIndex());            
        }
        leaderReplicator.execute1();
        
        for (int i = 0; i < queueDepthOfRequests; i++) {            
            long index = appendFutureList.get(i).get();
            Assert.assertEquals(initLogIndex + (i + 1), index);
        }
        
        byte[] coded1 = LogstoreProto.ADD.getCodec().encode(body.getBytes());
        CompletableFuture<Long> appendFuture1 = new CompletableFuture<Long>();
        leaderReplicator.appendEntry(LogstoreProto.ADD.getCode(), coded1, FunctionUtil.newConsumer(appendFuture1, Long.class));
        
        leaderReplicator.execute0();
        // mock term changed
        node.changeToFollower(currentTerm + 1);
        
        byte[] coded2 = LogstoreProto.ADD.getCodec().encode(body.getBytes());
        CompletableFuture<Long> appendFuture2 = new CompletableFuture<Long>();
        leaderReplicator.appendEntry(LogstoreProto.ADD.getCode(), coded2, FunctionUtil.newConsumer(appendFuture2, Long.class));
        leaderReplicator.execute0();
        leaderReplicator.execute1();
        
        Assert.assertTrue(appendFuture1.isCompletedExceptionally()); // term changed
        Assert.assertTrue(appendFuture2.isCompletedExceptionally()); // not leader
        
        logstore.flush();
        logstore.close();
        stateMachine.shutdown();
        
        FileUtil.delete(options.getAppDataPath()); // cleanup history
    }
    
    @Test
    public void leaderToFollowerReplicatorExecuteTest() throws Exception {
        
        String nodeId0 = "n0", nodeId1 = "n1";
        String serverCluster = "n0-0.0.0.0:6001;n1-0.0.0.0:6002;n2-0.0.0.0:6003";
        
        Options options0 = new Options();
        FileUtil.delete(options0.getAppDataPath()); // cleanup history
        
        options0.setBufferCapacityOfEntry(128 * 1024);
        options0.setBufferCapacityOfIndex(128 * 1024);
        options0.setServerId(nodeId0);
        options0.setServerCluster(serverCluster);
        Node node0 = new Node(options0, new RpcClient());
        LogstoreModule node0LogModule = new LogstoreModule(node0);
        
        Options options1 = new Options();
        FileUtil.delete(options1.getAppDataPath()); // cleanup history
        
        options1.setBufferCapacityOfEntry(128 * 1024);
        options1.setBufferCapacityOfIndex(128 * 1024);
        options1.setServerCluster(serverCluster);
        options1.setServerId(nodeId1);
        Node node1 = new Node(options1, new RpcClient());
        LogstoreModule node1LogModule = new LogstoreModule(node1);
        
        StateMachineModule stateMachineModule = new StateMachineModule(node1);
        
        // mock election
        long currentTerm = node0.getCurrentTerm();
        node0.changeFollowerToPreCandidate();
        node0.changePreCandidateToCandidate(currentTerm);
        node0.incTerm();
        currentTerm = node0.getCurrentTerm();
        node0.changeCandidateToLeader(currentTerm);
        node1.changeToFollower(currentTerm);
        
        // append log entry 
        int entryCount = 10;
        for (int i = 0; i < entryCount; i++) {
            Entry entry = new Entry();
            byte[] body = "Hello world!".getBytes();
            entry.setTerm(currentTerm);
            entry.setIndex(i);
            entry.setBody(body);
            node0LogModule.append(entry);
        }
        node0LogModule.flush();
        
        
        ConsensusModule node1ConsensusModule = new ConsensusModule(node1, node1LogModule, stateMachineModule) {
            FollowerReplicator followerReplicator = new FollowerReplicator(node1, node1LogModule, stateMachineModule);
            public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
                AppendEntriesResponse response = followerReplicator.appendEntries0(request);
                node1LogModule.flush();
                return CompletableFuture.completedFuture(response);
            }
        };
        Server server1 = new Server(nodeId1, "", node1ConsensusModule);
        LeaderToFollowerReplicator toFollowerReplicator = new LeaderToFollowerReplicator(server1, node0, node0LogModule);
        toFollowerReplicator.execute(); // Backtracking nextIndex
        Assert.assertEquals(0, toFollowerReplicator.nextIndex());
            
        // push node0 log entry to node1
        do {
            toFollowerReplicator.execute();
        } while (toFollowerReplicator.nextIndex() < node0LogModule.getLastLogIndex());
        
        long matchIndex = node0.getMatchIndexTableByTerm(currentTerm).get(server1.getServerId());
        Assert.assertEquals(node0.getLastLogIndex(), node1.getLastLogIndex());
        Assert.assertEquals(node0.getLastLogIndex(), matchIndex);
        
        // clear history data
        node0LogModule.close();
        node1LogModule.close();
        stateMachineModule.shutdown();
        
        FileUtil.delete(options0.getAppDataPath()); // cleanup history
        FileUtil.delete(options1.getAppDataPath()); // cleanup history
    }
    
    @Test
    public void followerReplicatorExecuteTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getAppDataPath()); // cleanup history
        
        options.setBufferCapacityOfEntry(128 * 1024);
        options.setBufferCapacityOfIndex(128 * 1024);
        
        Node node = new Node(options, new RpcClient());
        LogstoreModule logstoreModule = new LogstoreModule(node);
        StateMachineModule stateMachineModule = new StateMachineModule(node);
        FollowerReplicator followerReplicator = new FollowerReplicator(node, logstoreModule, stateMachineModule);
        
        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest();
        List<Entry> entries = new ArrayList<Entry>();
        long currentTerm = node.getCurrentTerm();
        for (int i = 0; i < 10; i++) {            
            Entry entry = new Entry();
            byte[] body = "Hello world!".getBytes();
            entry.setTerm(currentTerm);
            entry.setIndex(i);
            entry.setBody(body);
            entries.add(entry);
        }
        appendEntriesRequest.setTerm(currentTerm);
        appendEntriesRequest.setEntries(entries);
        CompletableFuture<AppendEntriesResponse> appendEntriesResponseFuture = followerReplicator.appendEntries(appendEntriesRequest);
        followerReplicator.execute0();
        followerReplicator.execute1();
        AppendEntriesResponse appendEntriesResponse = appendEntriesResponseFuture.get();
        Assert.assertNotNull(appendEntriesResponse);
        Assert.assertTrue(appendEntriesResponse.isSuccess());
        
        logstoreModule.flush();
        logstoreModule.close();
        stateMachineModule.shutdown();
        FileUtil.delete(options.getAppDataPath()); // cleanup history
    }
    
}
