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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.nopasserby.fastminiraft.api.ConsensusService;
import org.nopasserby.fastminiraft.core.Node.Role;
import org.nopasserby.fastminiraft.core.Node.RoleChangeListener;
import org.nopasserby.fastminiraft.util.DateUtil;
import org.nopasserby.fastminiraft.util.FileUtil;
import org.nopasserby.fastminiraft.util.IterableUtil;
import org.nopasserby.fastminirpc.core.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node {
    
    private final Logger logger = LoggerFactory.getLogger(Node.class);
    
    public enum Role {
        PREPARED_CANDIDATE,
        CANDIDATE,
        LEADER,
        FOLLOWER;
    }
    
    public interface RoleChangeListener {
        void changed(Role oldRole, Role newRole);
    }
    
    public static interface ServerChangeListener {
        public void addServer(Server server);
        public void removeServer(Server server);
    }
        
    private long currentTerm;

    private String votedFor;
    
    private AtomicLong commitIndex = new AtomicLong(-1);
    
    /**
     * <term, <serverId, matchIndex>>
     * */     
    private Map<Long, Map<String, Long>> matchIndexTermTable;
    
    /**
     * <term, availableCommitIndex>
     * 
     * */
    private Map<Long, Long> allowedCommitIndexTermTable; 
    
    private long lastLogIndex = -1;
    
    private long lastLogTerm = -1;
    
    private String serverId;
    
    private String leaderId;
    
    private Role role = Role.FOLLOWER;
    
    private List<RoleChangeListener> roleChangeListeners;
    
    private List<ServerChangeListener> serverChangeListeners;
        
    private volatile AtomicContext atomicContext;
    
    private volatile int belowQuorum;
    
    private Options options;
    
    private Iterable<Server> servers;
    
    private RpcClient rpcClient;
    
    public Node(Options options, RpcClient rpcClient) throws Exception {
        this.rpcClient = rpcClient;
        this.reload(options);
    }
    
    public void reload(Options options) throws Exception {
        this.options = options;
        this.serverId = options.getServerId();
        
        options.getProperties().forEach((key, value) -> {
            logger.debug("option - {}: {}", key, value);
        });
        
        logger.info("current server id: {}, current cluster: {}", serverId, options.getServerCluster());
        
        // persistent state
        Map<String, String> map = FileUtil.readMapFromFile(options.getServerStatePersistPath());
        currentTerm = Long.valueOf(map.getOrDefault("currentTerm", "-1"));
        votedFor = map.get("votedFor");
        
        List<Server> servers = new ArrayList<Server>();
        for (String server: options.getServerCluster().split(";")) {
            String serverId = server.split("-")[0];
            String serverHost = server.split("-")[1];            
            if (serverId.equals(this.serverId)) {
                servers.add(new Server(serverId, serverHost, null));
            } else {                
                servers.add(new Server(serverId, serverHost, rpcClient.getService(serverHost, ConsensusService.class)));
            }
        }
        this.servers = IterableUtil.newReadOnlyIterable(servers);
        
        matchIndexTermTable = new ConcurrentHashMap<Long, Map<String, Long>>();
        allowedCommitIndexTermTable = new ConcurrentHashMap<Long, Long>();
        
        roleChangeListeners = new ArrayList<RoleChangeListener>();
        serverChangeListeners = new ArrayList<ServerChangeListener>();
        
        atomicContext = new AtomicContext(currentTerm, isLeader(), leaderId, lastLogIndex, lastLogTerm);
        
        roleChangeListeners.add(new RoleChangeReport(this));
        
        logger.info("role init [server id: {}, term: {}, role: {}, datetime: {}]", serverId, currentTerm, role, DateUtil.nowOfLongDate());
    }
    
    public Iterable<Server> getServers() {
        return servers;
    }
    
    public int getServersCount() {
        return IterableUtil.newList(servers).size();
    }

    public String getVotedFor() {
        return votedFor;
    }

    public long getCommitIndex() {
        return commitIndex.get();
    }

    public AtomicContext getAtomicContext() {
        return atomicContext;
    }
    
    public String getServerId() {
        return serverId;
    }
    
    public long getCurrentTerm() {
        return currentTerm;
    }
    
    public Role getRole() {
        return role;
    }
    
    public Options getOptions() {
        return options;
    }
    
    public String getLeaderId() {
        return this.leaderId;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public boolean incTerm() {
        if (!isCandidate()) {
            return false;
        }
        
        currentTerm++;
        leaderId = null;
        votedFor = null;
        persist();
        atomicContext = new AtomicContext(currentTerm, isLeader(), leaderId, lastLogIndex, lastLogTerm);
        
        logger.info("term changed [server id: {}, term: {}, role: {}, datetime: {}]", serverId, currentTerm, role, DateUtil.nowOfLongDate());
        
        return true;
    }
    
    public void addRoleChangeListener(RoleChangeListener roleChangeListener) {
        roleChangeListeners.add(roleChangeListener);
    }
    
    public void addServerChangeListener(ServerChangeListener serverChangeListener) {
        serverChangeListeners.add(serverChangeListener);
    }
    
    public void updateCommitIndex(long newCommitIndex) {
        long oldCommitIndex = commitIndex.get();
        while (newCommitIndex > oldCommitIndex && !commitIndex.compareAndSet(oldCommitIndex, newCommitIndex)) {
            oldCommitIndex = commitIndex.get();
        }
    }
    
    public void updateLastLogIndex(long lastLogTerm, long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.atomicContext = new AtomicContext(currentTerm, isLeader(), leaderId, lastLogIndex, lastLogTerm);
    }
    
    public void changeRole(Role newRole) {
        Role oldRole = role;
        role = newRole;
        
        for (RoleChangeListener listener: roleChangeListeners) {
            try {
                listener.changed(oldRole, newRole);
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        
        atomicContext = new AtomicContext(currentTerm, isLeader(), leaderId, lastLogIndex, lastLogTerm);
        belowQuorum = 0;
    }
    
    public boolean changeFollowerToPreCandidate() {
        if (!isFollower()) {
            return false;
        }
        
        changeRole(Role.PREPARED_CANDIDATE);
        return true;
    }
    
    public boolean changePreCandidateToCandidate(long term) {
        if (!isPreCandidate() || currentTerm != term) {
            return false;
        }
        
        changeRole(Role.CANDIDATE);
        return true;
    }
    
    public boolean changeCandidateToLeader(long term) {
        if (!isCandidate() || currentTerm != term) {
            return false;
        }
        
        this.leaderId = serverId; 
        this.allowedCommitIndexTermTable.put(term, lastLogIndex + 1);
        
        changeRole(Role.LEADER);
        return true;
    }
    
    public boolean changeToFollower(long term) {
        return changeToFollower(term, null);
    }
    
    public boolean changeToFollower(long term, String leaderId) {
        if (term < currentTerm) {
            return false;
        }
        
        // candidate discovers current leader
        if (term == currentTerm) {
            this.leaderId = leaderId;
        } else if (term > currentTerm) {
            this.currentTerm = term;
            this.leaderId = leaderId;
            this.votedFor = null;
            persist();
            changeRole(Role.FOLLOWER); // need record change when old term follower change to new term follower 
        }
        if (role != Role.FOLLOWER) {            
            changeRole(Role.FOLLOWER);
        }
        return true;
    }
    
    public void updateVotedFor(String candidateId) {
        this.votedFor = candidateId;
        persist();
    }
    
    public long getAllowedCommitIndex(long currentTerm) {
        return allowedCommitIndexTermTable.getOrDefault(currentTerm, 0L);
    }
    
    private void persist() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("currentTerm", String.valueOf(currentTerm));
        map.put("votedFor", votedFor);
        try {
            FileUtil.writeMapToFile(map, options.getServerStatePersistPath());
        } catch (IOException e) {
            throw new IllegalStateException(
                    "node state cannot be persisted, persist path:" + options.getServerStatePersistPath(), e);
        }
    }
    
    public boolean isLeader() {
        return role == Role.LEADER;
    }
    
    public boolean isFollower() {
        return role == Role.FOLLOWER;
    }
    
    public boolean isCandidate() {
        return role == Role.CANDIDATE;
    }
    
    public boolean isPreCandidate() {
        return role == Role.PREPARED_CANDIDATE;
    }
    
    public int incBelowQuorum() {
        return ++belowQuorum;
    }
    
    public void resetBelowQuorum(long term) {
        if (currentTerm == term)
            this.belowQuorum = 0;
    }
    
    public void updateMatchIndex(long term, String serverId, long matchIndex) {
        Map<String, Long> matchIndexTable = getMatchIndexTableByTerm(term);
        synchronized (matchIndexTable) {
            if (matchIndexTable.getOrDefault(serverId, -1L) < matchIndex)
                matchIndexTable.put(serverId, matchIndex);
        }
    }
    
    public Map<String, Long> getMatchIndexTableByTerm(long term) {
        Map<String, Long> matchIndexTable = matchIndexTermTable.get(term);
        if (matchIndexTable != null) return matchIndexTable;
        
        Map<String, Long> table = new ConcurrentHashMap<>();
        for (Server server: servers) {
            table.put(server.getServerId(), -1L);
        }
        
        matchIndexTermTable.putIfAbsent(term, table);
        return matchIndexTermTable.get(term);
    }
    
    public void addServer(String serverId, String serverHost) {
        Map<String, Server> serverTable = new LinkedHashMap<>();
        IterableUtil.newList(this.servers).forEach(server -> {
            serverTable.put(server.getServerId(), server);
        });
        
        Server server = new Server(serverId, serverHost, rpcClient.getService(serverHost, ConsensusService.class));
        serverTable.putIfAbsent(serverId, server);
        this.servers = IterableUtil.newReadOnlyIterable(serverTable.values());
        
        for (ServerChangeListener serverChangeListener :serverChangeListeners) {          
            serverChangeListener.addServer(server);
        }
    }

    public void deleteServer(String serverId) {
        if (this.serverId.equals(serverId)) {
            return;
        }
        Map<String, Server> serverTable = new LinkedHashMap<>();
        IterableUtil.newList(this.servers).forEach(server -> {
            serverTable.put(server.getServerId(), server);
        });
        Server server = serverTable.remove(serverId);
        this.servers = IterableUtil.newReadOnlyIterable(serverTable.values());
        
        for (ServerChangeListener serverChangeListener :serverChangeListeners) {         
            serverChangeListener.removeServer(server);
        }
    }
    
    public String getServerCluster() {
        return IterableUtil.newList(this.servers).stream()
                .map(server -> server.getServerId() + "-" + server.getServerHost()).collect(Collectors.joining(";"));
    }

    public boolean hasChanged(Iterable<Server> servers) {
        return this.servers != servers;
    }
    
}

class Server {
    
    private final String serverId;
    
    private final String serverHost;
    
    private final ConsensusService consensus;
    
    public Server(String serverId, String serverHost, ConsensusService consensusService) {
        this.serverId = serverId;
        this.serverHost = serverHost;
        this.consensus = consensusService;
    }
    
    public ConsensusService getConsensusService() {
        return consensus;
    }
    
    public String getServerId() {
        return serverId;
    }

    public String getServerHost() {
        return serverHost;
    }

}

class AtomicContext {
    
    private final long currentTerm;
    
    private final boolean isLeader;
    
    private final String leaderId;
    
    private final long lastLogIndex;
    
    private final long lastLogTerm;
    
    public AtomicContext(long currentTerm, boolean isLeader, String leaderId, long lastLogIndex, long lastLogTerm) {
        this.currentTerm = currentTerm;
        this.isLeader = isLeader;
        this.leaderId = leaderId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }
    
}

class RoleChangeReport implements RoleChangeListener {
    
    private Logger logger = LoggerFactory.getLogger(RoleChangeReport.class); 
    
    private long roleEnableTimeMs = DateUtil.now();
    
    private Map<Role, Long> roleEnableTimeMsTable = new HashMap<>();
    
    private Node node;
    
    public RoleChangeReport(Node node) {
        this.node = node;
    }
    
    @Override
    public void changed(Role oldRole, Role newRole) {
        String serverId = node.getServerId();
        long currentTerm = node.getCurrentTerm();
        
        long newRoleEnableTimeMs = DateUtil.now();
        logger.info("role changed [server id: {}, term: {}, role: {}, datetime: {}, old role: {}, old role duration: {} ms]", 
                serverId, currentTerm, newRole, DateUtil.nowOfLongDate(), oldRole, newRoleEnableTimeMs - roleEnableTimeMs);
        
        roleEnableTimeMs = newRoleEnableTimeMs;
        
        roleEnableTimeMsTable.put(node.getRole(), DateUtil.now());
        if (newRole == Role.LEADER) {
            long electionUsingTimeMs = roleEnableTimeMsTable.get(Role.LEADER) - roleEnableTimeMsTable.get(Role.PREPARED_CANDIDATE);
            logger.info("server id: {}, term: {}, election using {} ms", serverId, currentTerm, electionUsingTimeMs);
        }
    }
    
}
