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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.nopasserby.fastminiraft.api.Logstore;
import org.nopasserby.fastminiraft.api.StoreService;
import org.nopasserby.fastminiraft.api.AppendEntriesRequest;
import org.nopasserby.fastminiraft.util.AssertUtil;
import org.nopasserby.fastminiraft.util.DateUtil;
import org.nopasserby.fastminiraft.util.FunctionUtil;
import org.nopasserby.fastminiraft.util.ThreadUtil;
import org.nopasserby.fastminirpc.core.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBufUtil;
import io.netty.util.internal.ConcurrentSet;

public class StoreServiceImpl implements StoreService, LifeCycle {
    
    private Node node;
    
    private Logstore logstore;
    
    private ConsensusModule consensusModule;
    
    // Read optimization
    private ConsensusReadModule consensusReadModule;
    
    public StoreServiceImpl(ConsensusModule consensusModule, StateMachineModule stateMachineModule) {
        this.consensusModule = consensusModule;
        this.node = consensusModule.getNode();
        this.logstore = consensusModule.getLogstore();
        this.consensusReadModule = new ConsensusReadModule(node, stateMachineModule);
    }

    @Override
    public CompletableFuture<Void> set(byte[] key, byte[] value) {
        AssertUtil.assertTrue(node.isLeader(), ExceptionTable.NOT_LEADER);
        CompletableFuture<Void> future = new CompletableFuture<Void>();
        byte[] coded = LogstoreProto.SET.getCodec().encode(key, value);
        consensusModule.appendEntry(LogstoreProto.SET.getCode(), coded, FunctionUtil.newConsumer(future, Void.class));
        return future;
    }
    
    @Override
    public CompletableFuture<Boolean> cas(byte[] key, byte[] expect, byte[] update) {
        AssertUtil.assertTrue(node.isLeader(), ExceptionTable.NOT_LEADER);
        CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();
        byte[] coded = LogstoreProto.CAS.getCodec().encode(key, expect, update);
        consensusModule.appendEntry(LogstoreProto.CAS.getCode(), coded, FunctionUtil.newConsumer(future, Boolean.class));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        AssertUtil.assertTrue(node.isLeader(), ExceptionTable.NOT_LEADER);
        return consensusReadModule.get(key);
    }

    @Override
    public CompletableFuture<Void> del(byte[] key) {
        AssertUtil.assertTrue(node.isLeader(), ExceptionTable.NOT_LEADER);
        CompletableFuture<Void> future = new CompletableFuture<Void>();
        byte[] coded = LogstoreProto.DEL.getCodec().encode(key);
        consensusModule.appendEntry(LogstoreProto.DEL.getCode(), coded, FunctionUtil.newConsumer(future, Void.class));
        return future;
    }

    @Override
    public CompletableFuture<Long> add(byte[] body) {
        AssertUtil.assertTrue(node.isLeader(), ExceptionTable.NOT_LEADER);
        CompletableFuture<Long> future = new CompletableFuture<Long>();
        byte[] coded = LogstoreProto.ADD.getCodec().encode(body);
        consensusModule.appendEntry(LogstoreProto.ADD.getCode(), coded, FunctionUtil.newConsumer(future, Long.class));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> get(long index) {
        AssertUtil.assertTrue(node.isLeader(), ExceptionTable.NOT_LEADER);
        AssertUtil.assertTrue(index <= node.getCommitIndex(), ExceptionTable.INVALID_INDEX);
        byte[] body = logstore.get(index).getBody();
        return CompletableFuture.completedFuture(body);
    }

    @Override
    public void startup() {
        consensusReadModule.startup();
    }

    @Override
    public void shutdown() {
        consensusReadModule.shutdown();
    }

}

class ConsensusReadModule implements LifeCycle {

    private Logger logger = LoggerFactory.getLogger(ConsensusReadModule.class);
    
    private Node node;
    
    private StateMachineModule stateMachineModule;
    
    private BlockingQueue<GetRequest> queue = new ArrayBlockingQueue<GetRequest>(64 * 1024);
    private BlockingQueue<Set<GetRequest>> squeue = new ArrayBlockingQueue<Set<GetRequest>>(64 * 1024);
    
    private int batchCapacity = 2000;
    
    private int heartbeatBufferCapacity = 64 * 1024;
    
    private Map<String, ExecutorService> executorTable = new ConcurrentHashMap<String, ExecutorService>();
    
    private Map<Long, Set<GetRequest>> futureTable = new ConcurrentHashMap<Long, Set<GetRequest>>();
    
    private long timeoutCheckInterval = 1000;
    
    private long timeoutCheckPoint;
    
    private AtomicLong sequencer = new AtomicLong();
    
    private LoopExecutor trunk = LoopExecutor.newLoopExecutor("heartbeat-replica-trunk", this::execute0);
    
    private LoopExecutor branch = LoopExecutor.newLoopExecutor("heartbeat-replica-branch", this::execute1);
    
    public ConsensusReadModule(Node node, StateMachineModule stateMachineModule) {
        this.node = node;
        this.stateMachineModule = stateMachineModule;
    }
    
    public CompletableFuture<byte[]> get(byte[] key) {
        CompletableFuture<byte[]> future = new CompletableFuture<byte[]>();
        GetRequest request = new GetRequest();
        request.getTimeout = DateUtil.now() + node.getOptions().getQuorumTimeout();
        request.key = key;
        request.future = future;
        
        if (!queue.offer(request)) {
            future.completeExceptionally(ExceptionTable.QUEUE_OF_REQUESTS_FULL);
        }
        return future;
    }
    
    public void execute0() {
        GetRequest request = null;
        try {
            request = queue.poll(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("", e);
        }
        if (request == null) {
            return;
        }
        
        AtomicContext ctx = node.getAtomicContext();
        if (!ctx.isLeader()) {
            request.future.completeExceptionally(ExceptionTable.NOT_LEADER);
            ThreadUtil.sleep(1);
            return;
        }
        
        if (stateMachineModule.getLastApplied() < node.getAllowedCommitIndex(ctx.getCurrentTerm())) {
            request.future.completeExceptionally(ExceptionTable.TERM_CHANGED);
            ThreadUtil.sleep(1);
            return;
        }
        
        Set<GetRequest> batch = new ConcurrentSet<GetRequest>();
        batch.add(request);
        queue.drainTo(batch, batchCapacity);
        
        AtomicInteger unacceptedCount = new AtomicInteger();
        AtomicInteger acceptedCount = new AtomicInteger();
        long sequence = sequencer.incrementAndGet();
        futureTable.put(sequence, batch);
        
        Iterable<Server> servers = node.getServers();
        int serversCount = node.getServersCount();
        
        Runnable heartbeatCheckupResult = () -> {
            if (acceptedCount.get() > serversCount / 2) {
                Set<GetRequest> acceptedBatch = futureTable.remove(sequence);
                if (acceptedBatch == null || squeue.offer(acceptedBatch)) {
                    return;
                }
                
                for (GetRequest acceptedRequest :acceptedBatch) {
                    acceptedRequest.future.completeExceptionally(ExceptionTable.QUEUE_OF_REQUESTS_FULL);
                }
            }
            if (unacceptedCount.get() > serversCount / 2) {
                Set<GetRequest> unacceptedBatch = futureTable.remove(sequence);
                for (GetRequest unacceptedRequest :unacceptedBatch) {
                    unacceptedRequest.future.completeExceptionally(ExceptionTable.TERM_CHANGED);
                }
            }
        };
        
        for (Server server: servers) {
            Runnable heartbeat = () -> {
                // asynchronous execution, skipping completed
                if (!futureTable.containsKey(sequence)) {
                    return;
                }
                AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(ctx.getCurrentTerm(), ctx.getLeaderId());
                
                server.getConsensusService().appendEntries(appendEntriesRequest).whenComplete((response, ex) -> {
                    if (response != null && response.isSuccess()) {
                        acceptedCount.incrementAndGet();
                    } else {
                        logger.debug("", ex);
                        unacceptedCount.incrementAndGet();
                    }
                    heartbeatCheckupResult.run();
                });
            };
            
            if (server.getServerId().equals(node.getServerId())) {
                acceptedCount.incrementAndGet();
                heartbeatCheckupResult.run();
            } else {
                getExecutor(server.getServerId()).execute(heartbeat);
            }
        }
    }
    
    public void execute1() {
        Set<GetRequest> batch = null;
        try {
            batch = squeue.poll(timeoutCheckInterval, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("", e);
        }
        
        long now = DateUtil.now();
        
        if (batch != null) {
            for (GetRequest request :batch) {
                if (now > request.getTimeout || request.future.isDone()) {
                    if (batch.remove(request)) {                        
                        logger.debug("remove timeout request[key: {}, timeout: {}]", 
                                ByteBufUtil.hexDump(request.key), request.getTimeout);
                    }
                    continue;
                }
                
                try {
                    byte[] value = stateMachineModule.getDB().get(request.key);
                    request.future.complete(value);
                } catch (Exception e) {
                    logger.error("get error", e);
                    request.future.completeExceptionally(e);
                }
            }
        }
        
        if (now > timeoutCheckPoint) {
            cleanupTimeout(futureTable);
            timeoutCheckPoint = DateUtil.now() + timeoutCheckInterval;
        }
    }
    
    public void cleanupTimeout(Map<Long, Set<GetRequest>> map) {
        long now = DateUtil.now();
        for (Set<GetRequest> batch: map.values()) {
            for (GetRequest request :batch) {
                if (now > request.getTimeout && !request.future.isDone()) {
                    if (batch.remove(request)) {                        
                        logger.debug("remove timeout request[key: {}, timeout: {}]", 
                                ByteBufUtil.hexDump(request.key), request.getTimeout);
                    }
                }
            }            
        }
    }
    
    public ExecutorService getExecutor(String serverId) {
        return executorTable.computeIfAbsent(serverId, key -> {
            ThreadFactory threadFactory = ThreadUtil.newThreadFactory("heartbeat-replica-executor-thread-" + serverId);
            return ThreadUtil.newSingleThreadExecutor(heartbeatBufferCapacity, threadFactory);
        });
    }
    
    private static class GetRequest {
        long getTimeout;
        byte[] key;
        CompletableFuture<byte[]> future;
    }
    
    @Override
    public void startup() {
        trunk.startup();
        branch.startup();
    }

    @Override
    public void shutdown() {
        for (ExecutorService executor :executorTable.values()) {            
            executor.shutdown();
        }
        trunk.shutdown();
        branch.shutdown();
    }
    
}

