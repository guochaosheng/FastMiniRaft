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

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.nopasserby.fastminiraft.api.ConsensusService;
import org.nopasserby.fastminiraft.api.AppendEntriesRequest;
import org.nopasserby.fastminiraft.api.VoteRequest;
import org.nopasserby.fastminiraft.api.VoteResponse;
import org.nopasserby.fastminiraft.core.Node.Role;
import org.nopasserby.fastminiraft.util.DateUtil;
import org.nopasserby.fastminiraft.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.ConcurrentSet;

/**
 * BASIC RAFT ALGORITHM (§3.3)
 *                                               times out,
 *                                               new election
 * startup                                          +-------+
 *   |                                              |       |     
 *   |                       times out,             V       |     receives votes from 
 *   |        +----------+   start election       +-----------+   majority of servers    +----------+
 *   +------->| Follower |----------------------->| Candidate |------------------------->|  Leader  |
 *            +----------+                        +-----------+                          +----------+
 *                ^                                     |                                      |
 *                |                                     |                                      |
 *                +-------------------------------------+                                      |
 *                |    discovers current leader or new term                                    |
 *                |                                                                            |
 *                +----------------------------------------------------------------------------+
 *                     discovers server with higher term
 *                     
 */
public class ElectionScheduler extends LoopExecutor {
    
    private Node node;
    
    private ElectionExecutor electionExecutor;
    
    private volatile long scheduledTimeout;
    
    public ElectionScheduler(Node node) {
        this.node = node;
        this.electionExecutor = new ElectionExecutor(node, this);
        this.resetElectionScheduledTimeout();
    }
    
    public void resetHeartbeatScheduledTimeout() {
        scheduledTimeout = System.currentTimeMillis() + node.getOptions().getHeartbeatPeriod();
        logger.debug("term:{}, {} reset heart beat timer and timeout is {} and new schedule time millis is {}.", 
                node.getCurrentTerm(), node.getRole(), node.getOptions().getHeartbeatPeriod(), DateUtil.formatToLongDate(scheduledTimeout));
    }
    
    public void resetElectionScheduledTimeout() {
        int minElectionTimeout = node.getOptions().getMinElectionTimeout();
        int maxElectionTimeout = node.getOptions().getMaxElectionTimeout();
        long timeout = minElectionTimeout + ThreadLocalRandom.current().nextInt(0, maxElectionTimeout -  minElectionTimeout);
        scheduledTimeout = System.currentTimeMillis() + timeout;
        
        if (!node.isFollower()) {            
            logger.debug("term:{}, {} reset election timer and timeout is {} and new schedule time millis is {}.", 
                    node.getCurrentTerm(), node.getRole(), timeout, DateUtil.formatToLongDate(scheduledTimeout));
        }
    }
    
    public void cancelScheduledTimeout() {
        scheduledTimeout = -1;
        logger.debug("term:{}, {} cancel scheduled timeout.", node.getCurrentTerm(), node.getRole());
    }
    
    public void awaitScheduledTimeout() {
        while (System.currentTimeMillis() < scheduledTimeout) {
            ThreadUtil.sleep(1);
        }
    }

    @Override
    protected void execute() throws Exception {
        electionExecutor.execute();
    }
    
    @Override
    public void shutdown() {
        electionExecutor.shutdown();
    }
    
}

class ElectionExecutor {
    
    private static Logger logger = LoggerFactory.getLogger(ElectionScheduler.class);
    
    private Set<String> electionThreads = new ConcurrentSet<String>();
    
    private ExecutorService executor = Executors.newCachedThreadPool(ThreadUtil.newThreadFactory("election-scheduler-executor"));
    
    private Node node;
    
    private ElectionScheduler scheduler;
    
    public ElectionExecutor(Node node, ElectionScheduler scheduler) {
        this.node = node;
        this.scheduler = scheduler;
    }

    /**
     * Add Pre-Vote: preventing disruptions when a server rejoins the cluster
     * 
     * One downside of Raft’s leader election algorithm is that a server that has been partitioned from the
     * cluster is likely to cause a disruption when it regains connectivity. When a server is partitioned, it
     * will not receive heartbeats. It will soon increment its term to start an election, although it won’t
     * be able to collect enough votes to become leader. When the server regains connectivity sometime
     * later, its larger term number will propagate to the rest of the cluster (either through the server’s
     * RequestVote requests or through its AppendEntries response). This will force the cluster leader to
     * step down, and a new election will have to take place to select a new leader. Fortunately, such events
     * are likely to be rare, and each will only cause one leader to step down.
     * If desired, Raft’s basic leader election algorithm can be extended with an additional phase to
     * prevent such disruptions, forming the Pre-Vote algorithm. In the Pre-Vote algorithm, a candidate
     * only increments its term if it first learns from a majority of the cluster that they would be willing
     * to grant the candidate their votes (if the candidate’s log is sufficiently up-to-date, and the voters
     * have not received heartbeats from a valid leader for at least a baseline election timeout).  (§9.6)
     * 
     * */
    public void execute() {
        // At any given time each server is in one of three states: leader, follower, or candidate (§3.3)
        scheduler.awaitScheduledTimeout();
        Role role = node.getRole();
        if (role == Role.FOLLOWER) {
            executeAsFollower();
        } else if (role == Role.PREPARED_CANDIDATE || role == Role.CANDIDATE) {
            executeAsCandidate(role);
        } else if (role == Role.LEADER) {
            executeAsLeader();
        }
    }
    
    /**
    * Followers (§5.2):
    *    • Respond to RPCs from candidates and leaders
    *    • If election timeout elapses without receiving AppendEntries
    *    RPC from current leader or granting vote to candidate:
    *    convert to candidate
    */
    public void executeAsFollower() {
        synchronized (node) {
            if (!node.isFollower()) {
                return;
            }
            node.changeFollowerToPreCandidate();
            scheduler.cancelScheduledTimeout();
        }
    }
    
    /**
    * Candidates (§5.2):
    *    • On conversion to candidate, start election:
    *    • Increment currentTerm
    *    • Vote for self
    *    • Reset election timer
    *    • Send RequestVote RPCs to all other servers
    *    • If votes received from majority of servers: become leader
    *    • If AppendEntries RPC received from new leader: convert to follower
    *    • If election timeout elapses: start new election
    */
    public void executeAsCandidate(Role role) {
        CandidateElectionContext ctx = new CandidateElectionContext();
        ctx.role = role;
        ctx.voteGrantedCount = new AtomicInteger();
        synchronized (node) {
            if (role != node.getRole()) {
                return;
            }
            
            if (role == Role.CANDIDATE) {
                node.incTerm();                              // 1.1 Increment currentTerm
                node.updateVotedFor(node.getServerId());     // 1.2 Vote for self
            }
            
            ctx.servers = node.getServers();
            ctx.serversCount = node.getServersCount();
            
            ctx.candidateId = node.getServerId();    
            ctx.lastLogTerm = node.getLastLogTerm();
            ctx.lastLogIndex = node.getLastLogIndex();
            ctx.term = node.getCurrentTerm();
            
            scheduler.resetElectionScheduledTimeout();       // 1.3 Reset election timer
        }
        
        Runnable candidateElectionReuslt = () -> {
            synchronized (node) {
                boolean changed = false;
                if (ctx.role == Role.PREPARED_CANDIDATE && ctx.voteGrantedCount.get() > ctx.serversCount / 2) {
                    changed = node.changePreCandidateToCandidate(ctx.term);
                }
                else if (ctx.role == Role.CANDIDATE && ctx.voteGrantedCount.get() > ctx.serversCount / 2) {
                    // 1.5 If votes received from majority of servers: become leader
                    changed = node.changeCandidateToLeader(ctx.term);   
                }
                if (changed) {
                    scheduler.cancelScheduledTimeout();
                }
            }
        };
        
        for (Server server: ctx.servers) {
            if (electionThreads.contains(server.getServerId())) {
                continue;
            }
            
            Runnable candidateElection = () -> {
                boolean isPrepare = ctx.role == Role.PREPARED_CANDIDATE;
                VoteRequest voteRequest = new VoteRequest(ctx.term, ctx.candidateId, ctx.lastLogIndex, ctx.lastLogTerm, isPrepare);
                ConsensusService consensusService = server.getConsensusService();
                logger.debug("request: {}-{}, {}", server.getServerId(), server.getServerHost(), voteRequest);
                consensusService.requestVote(voteRequest).whenComplete((VoteResponse voteResponse, Throwable ex) -> {
                    if (ex != null) {
                        logger.debug("reponse: " + server.getServerId() + "-" + server.getServerHost() + " error", ex);
                    } else {                        
                        logger.debug("reponse: {}-{}, {}", server.getServerId(), server.getServerHost(), voteResponse);
                    }
                    
                    // TODO discovers current leader or new term
                    
                    if (voteResponse.isVoteGranted()) {
                        ctx.voteGrantedCount.incrementAndGet();
                    }
                    candidateElectionReuslt.run();
                });
                electionThreads.remove(server.getServerId());
            };
            
            if (server.getServerId().equals(node.getServerId())) {
                ctx.voteGrantedCount.incrementAndGet();
                candidateElectionReuslt.run();
            }
            else {
                electionThreads.add(server.getServerId());
                executor.execute(candidateElection);  // 1.4 Send RequestVote RPCs to all other servers
            }            
        }
    }
    
    /**
    *Leaders:
    *    • Upon election: send initial empty AppendEntries RPCs
    *    (heartbeat) to each server; repeat during idle periods to
    *    prevent election timeouts (§5.2)
    */
    public void executeAsLeader() {
        AtomicContext ctx = node.getAtomicContext();
        if (!ctx.isLeader()) {
            return;
        }
        scheduler.resetHeartbeatScheduledTimeout();
        
        // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; 
        //                repeat during idle periods to prevent election timeouts (§5.2)
        Iterable<Server> servers = node.getServers();
        for (Server server: servers) {
            if (server.getServerId().equals(node.getServerId()) || !server.isActiveTimeout() 
                    || electionThreads.contains(server.getServerId())) {
                continue;
            }
            
            Runnable heartbeat = () -> {
                logger.debug("current term:{}, request: {}-{}, heartbeat start", ctx.getCurrentTerm(), server.getServerId(), server.getServerHost());
                server.getConsensusService().appendEntries(new AppendEntriesRequest(ctx.getCurrentTerm(), ctx.getLeaderId()));
                logger.debug("current term:{}, request: {}-{}, heartbeat end", ctx.getCurrentTerm(), server.getServerId(), server.getServerHost());
                
                // TODO discovers server with higher term
                
                electionThreads.remove(server.getServerId());
            };
            
            // when trying to establish a TCP connection, the TCP connection blocking time may exceed the heartbeat timeout
            electionThreads.add(server.getServerId()); 
            executor.execute(heartbeat);
        }
    }
    
    public void shutdown() {
        executor.shutdown();
    }
    
    static class CandidateElectionContext {
        Role role;
        AtomicInteger voteGrantedCount;
        long term, lastLogIndex, lastLogTerm;
        String candidateId;
        Iterable<Server> servers;
        int serversCount;
    }
    
}
