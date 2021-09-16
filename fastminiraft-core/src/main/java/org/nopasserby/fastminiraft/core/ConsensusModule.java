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

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.nopasserby.fastminiraft.api.ConsensusService;
import org.nopasserby.fastminiraft.api.Logstore;
import org.nopasserby.fastminiraft.api.StateMachine;
import org.nopasserby.fastminiraft.api.AppendEntriesRequest;
import org.nopasserby.fastminiraft.api.AppendEntriesResponse;
import org.nopasserby.fastminiraft.api.Entry;
import org.nopasserby.fastminiraft.api.VoteRequest;
import org.nopasserby.fastminiraft.api.VoteResponse;
import org.nopasserby.fastminiraft.core.ExceptionTable.OperationException;
import org.nopasserby.fastminiraft.core.Node.ServerChangeListener;
import org.nopasserby.fastminiraft.util.CollectionUtil;
import org.nopasserby.fastminiraft.util.DateUtil;
import org.nopasserby.fastminiraft.util.ThreadUtil;
import org.nopasserby.fastminirpc.core.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread model:
 *                                         Leader Node  |  Follower Nodes
 *        request |  ^                                  |
 *              ① v  | ⑩            Log store           |
 *      +--------------------+  ②   +----------------+  |
 *      |  LeaderReplicator  | ---> | x <- 3 | y - 1 |  |
 *      +--------------------+ <--- +----------------+  |
 *        ④ | ^   ④ | ^         ③                       |
 *          | |     | |                                 |
 *          | | ⑨   v | ⑨                               |                                   Log store
 *          | |   +------------------------------+  ⑤   |     +----------------------+  ⑥   +----------------+    
 *          | |   |  LeaderToFollowerReplicator  | -----|---> |  FollowerReplicator  | ---> | x <- 3 | y - 1 |
 *          | |   +------------------------------+ <----|---- +----------------------+ <--- +----------------+   
 *          v |                                     ⑧   |                               ⑦   Log store
 *     +------------------------------+             ⑤   |     +----------------------+  ⑥   +----------------+    
 *     |  LeaderToFollowerReplicator  | ----------------|---> |  FollowerReplicator  | ---> | x <- 3 | y - 1 |
 *     +------------------------------+ <---------------|---- +----------------------+ <--- +----------------+   
 *                                                  ⑧   |                               ⑦
 *                                                      |
 * */
public class ConsensusModule implements ConsensusService, ServerChangeListener, LifeCycle  {

    private Logger logger = LoggerFactory.getLogger(ConsensusModule.class);
    
    private Node node;
    
    private Logstore logstore;

    private ElectionScheduler electionScheduler;
    
    private LeaderReplicator leaderReplicator;
    
    private Map<String, LeaderToFollowerReplicator> leaderToFollowerReplicatorTable;
    
    private FollowerReplicator followerReplicator;
    
    public ConsensusModule(Node node, Logstore logstore, StateMachine stateMachine) {
        this.node = node;
        this.node.addServerChangeListener(this);
        this.logstore = logstore;
        this.electionScheduler = new ElectionScheduler(node);
        this.leaderReplicator = new LeaderReplicator(node, logstore, stateMachine);
        this.leaderToFollowerReplicatorTable = new HashMap<String, LeaderToFollowerReplicator>();
        for (Server server: node.getServers()) {
            if (server.getServerId().equals(node.getServerId())) continue;
            LeaderToFollowerReplicator leaderToFollowerReplicator = new LeaderToFollowerReplicator(server, node, logstore);
            this.leaderToFollowerReplicatorTable.put(server.getServerId(), leaderToFollowerReplicator);    
        }
        this.followerReplicator = new FollowerReplicator(node, logstore, stateMachine);
    }
    
    public void appendEntry(int bodyType, byte[] body, BiConsumer<Object, Throwable> action) {
        leaderReplicator.appendEntry(bodyType, body, action);
    }
    
    @Override
    public CompletableFuture<VoteResponse> requestVote(VoteRequest request) {
        logger.debug("request vote: {}", request);
        
        VoteResponse voteResponse = new VoteResponse();
        long currentTerm, lastLogTerm, lastLogIndex;
        String votedFor;
        synchronized (node) {
            // All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            if (request.getTerm() > node.getCurrentTerm()) {
                node.changeToFollower(request.getTerm());
            }
            
            currentTerm = node.getCurrentTerm();
            votedFor = node.getVotedFor();
            lastLogTerm = node.getLastLogTerm();
            lastLogIndex = node.getLastLogIndex();
        
            voteResponse.setTerm(currentTerm);
            
            // 1. Reply false if term < currentTerm (§5.1)
            if (request.getTerm() < currentTerm) {
                return CompletableFuture.completedFuture(voteResponse);
            }
            // 2.1 votedFor is null or candidateId (§5.2)
            boolean conditionA = votedFor == null || votedFor.equals(request.getCandidateId());
            // 2.2 candidate’s log is at least as up-to-date as receiver’s log (§5.4)
            boolean conditionB = request.getLastLogTerm() > lastLogTerm
                    || (request.getLastLogTerm() == lastLogTerm &&  request.getLastLogIndex() >= lastLogIndex);
            
            if (request.isPrepare() && conditionB) { // Pre-Vote (§9.6)
                voteResponse.setVoteGranted(true);
                return CompletableFuture.completedFuture(voteResponse);
            }
            
            if (conditionA && conditionB) {
                node.updateVotedFor(request.getCandidateId());
                voteResponse.setVoteGranted(true);
            }
            return CompletableFuture.completedFuture(voteResponse);
        }
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        AppendEntriesResponse response = new AppendEntriesResponse();
        
        synchronized (node) {
            response.setTerm(node.getCurrentTerm());
            if (request.getTerm() < node.getCurrentTerm()) {                
                return CompletableFuture.completedFuture(response);
            }
            
            node.changeToFollower(request.getTerm(), request.getLeaderId());
            electionScheduler.resetElectionScheduledTimeout();
        }
        
        boolean heartbeat = request.getEntries() == null;
        if (heartbeat) {
            response.setSuccess(true);
            logger.debug("append entries request[term:{}, leaderId:{}, heartbeat:{}]", request.getTerm(), request.getLeaderId(), heartbeat);
            return CompletableFuture.completedFuture(response);
        }
        
        return followerReplicator.appendEntries(request);
    }

    @Override
    public void startup() {
        electionScheduler.startup();
        leaderReplicator.startup();
        followerReplicator.startup();
        for (LeaderToFollowerReplicator replicator :leaderToFollowerReplicatorTable.values()) {            
            replicator.startup();
        }
    }
    
    @Override
    public void shutdown() {
        electionScheduler.shutdown();
        leaderReplicator.shutdown();
        followerReplicator.shutdown();
        for (LeaderToFollowerReplicator replicator :leaderToFollowerReplicatorTable.values()) {            
            replicator.shutdown();
        }
    }

    public Node getNode() {
        return node;
    }

    public Logstore getLogstore() {
        return logstore;
    }

    @Override
    public void addServer(Server server) {
        if (!leaderToFollowerReplicatorTable.containsKey(server.getServerId())) {            
            LeaderToFollowerReplicator replicator = new LeaderToFollowerReplicator(server, node, logstore);
            leaderToFollowerReplicatorTable.put(server.getServerId(), replicator);
            replicator.startup();
        }
    }

    @Override
    public void removeServer(Server server) {
        if (leaderToFollowerReplicatorTable.containsKey(server.getServerId())) {
            LeaderToFollowerReplicator replicator = leaderToFollowerReplicatorTable.get(server.getServerId());
            replicator.shutdown();
        }
    }
    
}

class LeaderReplicator {
    
    private Logger logger = LoggerFactory.getLogger(LeaderReplicator.class);
    
    /**
     * <term, <index, TimeoutFuture>> there may be the same index and different terms
     * 
     * (When there are multiple leaders with different terms at the same time, 
     * there may be logs with the same index and different terms between different nodes, 
     * Raft ensures that the logs are consistent before the data is visible to the public by rewriting the logs.)
     * */
    private Map<Long, Map<Long, EntryRequest>> replicaTable = new ConcurrentHashMap<Long, Map<Long, EntryRequest>>();
    
    private BlockingQueue<EntryRequest> queue = new ArrayBlockingQueue<EntryRequest>(64 * 1024);
    
    private Node node;
    
    private Logstore logstore;
    
    private StateMachine stateMachine;
    
    private int appendBatchCapacity;
    
    private List<EntryRequest> appendBatch;
    
    private LoopExecutor trunk = LoopExecutor.newLoopExecutor("trunk", this::execute0);
    
    private LoopExecutor branch = LoopExecutor.newLoopExecutor("branch", this::execute1);
    
    private List<Long> matchIndexList = new ArrayList<Long>();
    
    public LeaderReplicator(Node node, Logstore logstore, StateMachine stateMachine) {
        this.node = node;
        this.logstore = logstore;
        this.stateMachine = stateMachine;
        this.queue = new ArrayBlockingQueue<EntryRequest>(node.getOptions().getQueueDepthOfRequests());
        this.appendBatchCapacity = node.getOptions().getFlushMaxEntries();
        this.appendBatch = new ArrayList<EntryRequest>(appendBatchCapacity);
    }

    public void appendEntry(int bodyType, byte[] body, BiConsumer<Object, Throwable> action) {
        EntryRequest entryRequest = new EntryRequest();
        entryRequest.timeout = DateUtil.now() + node.getOptions().getQuorumTimeout();
        entryRequest.logEntry = new Entry(bodyType, body);
        entryRequest.action = action;
        
        if (!queue.offer(entryRequest)) {
            action.accept(null, ExceptionTable.QUEUE_OF_REQUESTS_FULL);
        }
    }

    public void startup() {
        trunk.startup();
        branch.startup();
    }
    
    public void execute0() {
        EntryRequest entryRequest = null;
        try {
            entryRequest = queue.take();
        } catch (Exception e) {
            logger.error("", e);
        }
        
        appendBatch.add(entryRequest);
        queue.drainTo(appendBatch, appendBatchCapacity);
        
        boolean isLeader;
        synchronized (node) {
            isLeader = node.isLeader();
            if (isLeader) {
                long currentTerm = node.getCurrentTerm();
                long lastLogIndex1 = node.getLastLogIndex();
                long lastLogIndex2 = lastLogIndex1;
                
                Map<Long, EntryRequest> replicaTable = getReplicaTableByTerm(currentTerm);
                for (EntryRequest r: appendBatch) {
                    replicaTable.put(++lastLogIndex1, r);
                }
                
                for (EntryRequest r: appendBatch) {
                    Entry logEntry = r.logEntry;
                    logEntry.setTerm(currentTerm);
                    logEntry.setIndex(++lastLogIndex2);
                    logstore.append(logEntry);
                }
                logstore.flush();
            }
        }
        
        if (!isLeader) {
            for (EntryRequest r: appendBatch) {
                r.action.accept(null, ExceptionTable.NOT_LEADER);
            }
        }
        
        appendBatch.clear();
    }
    
    public void execute1() {
        AtomicContext ctx = node.getAtomicContext();
        
        long currentTerm = ctx.getCurrentTerm();
        int termCount = replicaTable.size();
        if (termCount > 1 || (termCount == 1 && !replicaTable.containsKey(currentTerm))) {
            replicaTable.forEach((term, t) -> {
                if (term == currentTerm) {
                    return;
                }
                
                replicaTable.remove(term);
                for (EntryRequest r: t.values()) {
                    r.action.accept(null, ExceptionTable.TERM_CHANGED); // May fail or follow a leader's no-op being committed
                }
            });
        }
        
        if (!ctx.isLeader()) {
            ThreadUtil.sleep(1);
            return;
        }
        
        node.updateMatchIndex(currentTerm, node.getServerId(), ctx.getLastLogIndex());
        
        /**
         *Leaders:
         *    • If there exists an N such that N > commitIndex, a majority
         *    of matchIndex[i] ≥ N, and log[N].term == currentTerm:
         *    set commitIndex = N (§5.3, §5.4).
         */
        Iterable<Server> servers = node.getServers();
        Map<String, Long> matchIndexTable = node.getMatchIndexTableByTerm(currentTerm);
        servers.forEach(server -> {
            matchIndexList.add(matchIndexTable.getOrDefault(server.getServerId(), -1L));
        });
        
        long newCommitIndex = CollectionUtil.median(matchIndexList);
        matchIndexList.clear();
        
        if (newCommitIndex < node.getAllowedCommitIndex(currentTerm)) {
            ThreadUtil.sleep(1);
            return;
        }
        
        node.updateCommitIndex(newCommitIndex);
        
        long lastApplied = stateMachine.getLastApplied();
        
        if (newCommitIndex == lastApplied) {
            ThreadUtil.sleep(1);
            return;
        }
       
        long now = DateUtil.now();
        Map<Long, EntryRequest> replicaTable = getReplicaTableByTerm(currentTerm);
        
        // All Servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        while (lastApplied < newCommitIndex) {
            lastApplied++;
            
            EntryRequest logEntryRequest = replicaTable.remove(lastApplied);
            
            if (logEntryRequest == null) {
                stateMachine.apply(logstore.get(lastApplied), null);
            } else {
                boolean requestTimeout = logEntryRequest.timeout < now;
                stateMachine.apply(logEntryRequest.logEntry, requestTimeout ? null : logEntryRequest.action);
            }
            if (node.hasChanged(servers)) {
                break;
            }
        }
    }
    
    public Map<Long, EntryRequest> getReplicaTableByTerm(long term) {
        return replicaTable.computeIfAbsent(term, key -> new ConcurrentHashMap<>());
    }

    public void shutdown() {
        trunk.shutdown();
        branch.shutdown();
    }
    
    public static class EntryRequest {
        Entry logEntry;
        long timeout;
        BiConsumer<Object, Throwable> action;
    }
    
}

class LeaderToFollowerReplicator extends LoopExecutor {
    
    private Server server;
    
    private Node node;
    
    private Logstore logstore;
    
    private int maxCount = 200; // < maxFrameLength / maxBodyLength
    
    private long currentTerm = -1;
    
    private volatile long nextIndex;
    
    private volatile boolean reinitialized;
    
    private long lastTimestamp;
    
    public LeaderToFollowerReplicator(Server server, Node node, Logstore logstore) {
        this.server = server;
        this.node = node;
        this.logstore = logstore;
    }
    
    /**
     *Leaders:
     *    • If last log index ≥ nextIndex for a follower: send
     *    AppendEntries RPC with log entries starting at nextIndex
     *    • If successful: update nextIndex and matchIndex for
     *    follower (§5.3)
     *    • If AppendEntries fails because of log inconsistency:
     *    decrement nextIndex and retry (§5.3)
     */
    @Override
    public void execute() throws Exception {
        AtomicContext ctx = node.getAtomicContext();
        
        if (!ctx.isLeader()) {
            ThreadUtil.sleep(1);
            return;
        }
        
        long currentTerm = ctx.getCurrentTerm();
        if (currentTerm != this.currentTerm) {
            this.currentTerm = currentTerm;
            this.nextIndex = ctx.getLastLogIndex() + 1;
            this.reinitialized = true;
        }
        
        long lastLogIndex = ctx.getLastLogIndex();
        
        long backIndex = nextIndex;
        long prevLogIndex = nextIndex - 1;
        long prevLogTerm = logstore.getEntryTerm(prevLogIndex);
        List<Entry> entries = new ArrayList<Entry>();
        long replicatedIndex = nextIndex;
        while (entries.size() < maxCount && replicatedIndex <= lastLogIndex) {
            entries.add(logstore.get(replicatedIndex++));
        }
        
        AtomicContext newCtx = node.getAtomicContext();
        /**
         * 1. The < currentterm, role, leaderid, lastlogindex > values obtained from atomic context may not be the latest values
         * 2. During the logstore get call, the tenure may change. If the current node changes to a follower, 
         *    some logs at the end of the logstore may be overwritten due to the appendentries operation
         * 
         * Here, check whether the tenure number is the same to prevent entries from being loaded into the overwritten log
         * 
         * Proof.
         * Assume that the old atomic context tenure value before reading the logstore (time point A0) is T0, 
         * and the new old atomic context tenure value after reading the logstore (time point A1) is T1
         * 1. if there is no change in term between A0 and A1, so no logs will be overwritten
         * 2. if the term has changed between A0 and A1 (one or more term changes)
         * (Prerequisite, the process of changing the term number of a node object is to first add a global lock, 
         * then update the term number and other values, then update the atomic context value, 
         * and finally release the global lock, and no logs will be overwritten in the logstore during the locking period)
         * 2.1 If the values of currentTerm, role, leaderId of node object have been updated, 
         * but the atomic context value has not been updated, T1 is not the latest value, 
         * the global lock has not been released during A0 to A1, no log will be overwritten.
         * 2.2 If node object currentTerm, role, and leaderId have been updated, and atomic context value has been updated, 
         * and T1 is the latest value, the global lock may or may not be released, 
         * and the log may be overwritten during A0 to A1, then T1 ! = T0, the logstore log is discarded
         *
         * */
        if (ctx.getCurrentTerm() != newCtx.getCurrentTerm()) {
            return;
        }
        
        long now = DateUtil.now();
        int count = entries.size();
        boolean zeroEntries = !reinitialized && count == 0; // append commit index only
        
        if (zeroEntries && now - lastTimestamp < 100) {
            ThreadUtil.sleep(1);
            return;
        }
        
        long matchIndex = prevLogIndex + count;
        
        String leaderId = ctx.getLeaderId();
        long commitIndex = node.getCommitIndex();
        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest();
        appendEntriesRequest.setLeaderId(leaderId);
        appendEntriesRequest.setTerm(currentTerm);
        appendEntriesRequest.setPrevLogIndex(prevLogIndex);
        appendEntriesRequest.setPrevLogTerm(prevLogTerm);
        appendEntriesRequest.setLeaderCommitIndex(commitIndex);
        appendEntriesRequest.setEntries(entries);
        
        CompletableFuture<AppendEntriesResponse> future = server.getConsensusService().appendEntries(appendEntriesRequest);
        
        if (zeroEntries) {
            lastTimestamp = now;
            return;
        }
        
        if (reinitialized || future.isCompletedExceptionally()) {
            future.get(node.getOptions().getReplicaTimeout(), TimeUnit.MILLISECONDS);
        }
        
        nextIndex = replicatedIndex;
        
        future.whenComplete((appendEntriesResponse, ex) -> {
            if (ex != null) {
                logger.debug("server id: " + server.getServerId() + ", server host: " + server.getServerHost(), ex);
                nextIndex = backIndex;
                return;
            }
            if (appendEntriesResponse.getTerm() != currentTerm) {
                return;
            }
            if (!appendEntriesResponse.isSuccess()) {
                logger.debug("server id: {} , server host: {}, next index: {}, append entries response index: {}, ", 
                        server.getServerId(), server.getServerHost(), backIndex, appendEntriesResponse.getIndex());
                nextIndex = appendEntriesResponse.getIndex() + 1;
                reinitialized = true;
                return;
            }
            reinitialized = false;
            node.updateMatchIndex(currentTerm, server.getServerId(), matchIndex);
        });
    }
    
    @Override
    public void exceptionCaught(Throwable e) {
        logger.warn(String.format("server[serverId: %s] exception caught", server.getServerId()), e);
        ThreadUtil.sleep(node.getOptions().getReplicaTimeout());
    }
    
    public long nextIndex() {
        return nextIndex;
    }
    
}

class FollowerReplicator {
    
    static Logger logger = LoggerFactory.getLogger(FollowerReplicator.class);
    
    private Node node;
    
    private StateMachine stateMachine;
    
    private Logstore logstore; 
    
    private Map<Long, SimpleEntry<AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>>> replicaTable = new ConcurrentHashMap<>();
    
    private int appendBatchCapacity;
    
    private List<SimpleEntry<CompletableFuture<AppendEntriesResponse>, Object>> resultList;
    
    private LoopExecutor trunk = LoopExecutor.newLoopExecutor("trunk", this::execute0);
    
    private LoopExecutor branch = LoopExecutor.newLoopExecutor("branch", this::execute1);
    
    public FollowerReplicator(Node node, Logstore logstore, StateMachine stateMachine) {
        this.node = node;
        this.logstore = logstore;
        this.stateMachine = stateMachine;
        this.appendBatchCapacity = node.getOptions().getFlushMaxEntries();
        this.resultList = new ArrayList<>(appendBatchCapacity);
    }

    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<AppendEntriesResponse>();
        SimpleEntry<AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> replicaRequest = new SimpleEntry<>(request, future);
        replicaTable.put(request.getPrevLogIndex(), replicaRequest);            
        return future;
    }
    
    public void execute0() {
        try {
            int replicaCount = replicaTable.size();
            if (replicaCount == 0) {
                ThreadUtil.sleep(1);
                return;
            }
            
            long commitIndex = -1;
            int appendBatchCount = appendBatchCapacity;
            while (appendBatchCount > 0 && replicaCount > 0) {
                long lastLogIndex = logstore.getLastLogIndex();
                SimpleEntry<AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> replicaRequest = replicaTable.remove(lastLogIndex);
                if (replicaRequest == null) {
                    if (appendBatchCount != appendBatchCapacity) {
                        break;
                    }
                    replicaRequest = replicaTable.remove(CollectionUtil.min(replicaTable.keySet()));
                }
                replicaCount--;
                AppendEntriesRequest request = replicaRequest.getKey();
                appendBatchCount -= request.getEntries().size();
                Object result = ExceptionTable.ABORT;
                try {
                    AppendEntriesResponse response = appendEntries0(request);
                    if (response.isSuccess()) {
                        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                        commitIndex = Math.min(request.getLeaderCommitIndex(), request.getPrevLogIndex() + request.getEntries().size());
                    }
                    result = response;
                } catch (Exception e) {
                    logger.error("", e);
                }
                CompletableFuture<AppendEntriesResponse> future = replicaRequest.getValue();  
                SimpleEntry<CompletableFuture<AppendEntriesResponse>, Object> entry = new SimpleEntry<>(future, result);
                resultList.add(entry);
            }
            if (appendBatchCount < appendBatchCapacity) {
                logstore.flush();
            }
            
            node.updateCommitIndex(commitIndex);
            
            for (SimpleEntry<CompletableFuture<AppendEntriesResponse>, Object> entry: resultList) {
                CompletableFuture<AppendEntriesResponse> future = entry.getKey();
                Object result = entry.getValue();
                if (result == ExceptionTable.ABORT) {
                    future.completeExceptionally((OperationException) result);
                } else {
                    future.complete((AppendEntriesResponse) result);
                }
            }
        } finally {
            resultList.clear();
        }
    }
    
    public AppendEntriesResponse appendEntries0(AppendEntriesRequest request) {
        AppendEntriesResponse response = new AppendEntriesResponse();
        synchronized (node) {
            response.setTerm(node.getCurrentTerm());
            response.setIndex(node.getLastLogIndex());
            if (!node.isFollower()) {
                logger.debug("current term: {}, current role: {}, append entry failed because current node is not a follower.",
                        node.getCurrentTerm(), node.getRole());
                response.setSuccess(false);
                return response;
            }
            if (node.getCurrentTerm() != request.getTerm()) {
                // 1. Reply false if term < currentTerm (§5.1)
                logger.debug("current term: {}, current role: {}, request term: {}, append entries failed because of inconsistent terms.",
                        node.getCurrentTerm(), node.getRole(), request.getTerm());
                response.setSuccess(false);
                return response;
            }
            long prevLogTerm = logstore.getEntryTerm(request.getPrevLogIndex());
            if (prevLogTerm != request.getPrevLogTerm()) {
                logger.debug("current term: {}, current role: {}, "
                        + "request previous entry index: {}, request previous entry term: {}, "
                        + "log store get term by request index: {}, log store last log index: {}, "
                        + "append entry failed because the previous entry term was inconsistent", 
                        node.getCurrentTerm(), node.getRole(), 
                        request.getPrevLogIndex(), request.getPrevLogTerm(), 
                        prevLogTerm, logstore.getLastLogIndex());
                // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                response.setSuccess(false);
                response.setIndex(Math.min(request.getPrevLogIndex() - 1, node.getLastLogIndex()));
                return response;
            }
            
            long index = request.getPrevLogIndex();
            for (Entry entry : request.getEntries()) {
                index++;
                if (logstore.getLastLogIndex() >= index) {
                    if (logstore.getEntryTerm(index) == entry.getTerm()) continue;
                    // 3. If an existing entry conflicts with a new one (same index
                    //    but different terms), delete the existing entry and all that
                    //    follow it (§5.3)
                    logstore.truncate(index);
                }
                // 4. Append any new entries not already in the log
                logstore.append(entry);
            }

            response.setIndex(node.getLastLogIndex());
            response.setSuccess(true);
            return response;
        }
        
    }
    
    public void execute1() {
        if (!node.isFollower()) {
            ThreadUtil.sleep(1);
            return;
        }
        
        long newCommitIndex = node.getCommitIndex();
        long lastApplied = stateMachine.getLastApplied();
        if (lastApplied == newCommitIndex) {
            ThreadUtil.sleep(1);
            return;
        }
        // All Servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        while (lastApplied < newCommitIndex) {
            stateMachine.apply(logstore.get(++lastApplied), null);
        }
    }

    public void startup() {
        trunk.startup();
        branch.startup();
    }
    
    public void shutdown() {
        trunk.shutdown();
        branch.shutdown();
    }
    
}