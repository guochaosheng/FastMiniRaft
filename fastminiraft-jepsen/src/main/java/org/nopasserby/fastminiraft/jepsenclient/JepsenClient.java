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

package org.nopasserby.fastminiraft.jepsenclient;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.nopasserby.fastminirpc.util.ByteUtil.toByteArray;
import static org.nopasserby.fastminirpc.util.ByteUtil.toLong;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.nopasserby.fastminiraft.api.ClientService;
import org.nopasserby.fastminiraft.api.StoreService;
import org.nopasserby.fastminiraft.boot.ServiceSerializer;
import org.nopasserby.fastminiraft.core.ExceptionTable.NotLeaderException;
import org.nopasserby.fastminiraft.core.LoopExecutor;
import org.nopasserby.fastminiraft.util.AssertUtil;
import org.nopasserby.fastminiraft.util.ThreadUtil;
import org.nopasserby.fastminirpc.core.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.ConcurrentSet;

public class JepsenClient {
    
    private static Logger logger = LoggerFactory.getLogger(JepsenClient.class);
    
    private static int timeout = 3000;
    
    private Map<String, StandardService> services;
    
    private RpcClient client;
    
    private LeaderIdUpdater leaderIdUpdater;
    
    public JepsenClient(String group, String serverCluster) {
        logger.info("group: {}, server cluster: {}", group, serverCluster);
        
        client = new RpcClient(new ServiceSerializer());
        
        services = new HashMap<>();
        for (String server: serverCluster.split(";")) {
            String serverId = server.split("-")[0];
            String serverHost = server.split("-")[1];
            
            ClientService clientService = client.getService(serverHost, ClientService.class);
            StoreService storeService = client.getService(serverHost, StoreService.class);
            services.put(serverId, new StandardService(clientService, storeService));
        }
        
        leaderIdUpdater = new LeaderIdUpdater();
    }
    
    public <T> T call(Callable<T> callable) {
        leaderIdUpdater.waitFor(1500);
        
        AssertUtil.assertNotNull(leaderIdUpdater.getLeaderId(), JepsenError.METADATA_ERROR);
        try {
            return callable.call();
        } 
        catch (NotLeaderException e) {
            // ignore
        }
        catch (Exception e) {
            throw JepsenError.getError(e); 
        }
        
        // If not leader, try again
        leaderIdUpdater.reset();
        leaderIdUpdater.waitFor(1500);
        
        AssertUtil.assertNotNull(leaderIdUpdater.getLeaderId(), JepsenError.METADATA_ERROR);
        try {            
            return callable.call();
        }
        catch (Exception e) {
            leaderIdUpdater.reset();
            throw JepsenError.getError(e);
        }
    }
    
    public boolean compareAndSet(String key, long expect, long update) throws Exception {
        return call(() ->  {
            String leaderId = leaderIdUpdater.getLeaderId();
            try {
                return services.get(leaderId).cas(toByteArray(key), toByteArray(expect), toByteArray(update)).get(timeout, MILLISECONDS);                
            } catch (Exception e) {
                if (!NotLeaderException.class.isInstance(e)) {
                    logger.error(String.format("jepsen client: cas exception, key: %s, expect: %s, update: %s, target: %s", 
                            key, expect, update, leaderId), e);
                }
                throw e;
            }
        });
    }
    
    public boolean set(String key, long value) throws Exception {
        return call(() ->  {
            String leaderId = leaderIdUpdater.getLeaderId();
            try {
                services.get(leaderId).set(toByteArray(key), toByteArray(value)).get(timeout, MILLISECONDS);
                return true;                
            } catch (Exception e) {
                if (!NotLeaderException.class.isInstance(e)) {
                    logger.error(String.format("jepsen client: set exception, key: %s, value: %s, target: %s", 
                            key, value, leaderId, e));    
                }
                throw e;
            }
        });
    }
    
    public Long get(String key) throws Exception {
        return call(() ->  {
            String leaderId = leaderIdUpdater.getLeaderId();
            try {
                byte[] value = services.get(leaderId).get(toByteArray(key)).get(timeout, MILLISECONDS);
                return value != null ? toLong(value) : null;                
            } catch (Exception e) {
                if (!NotLeaderException.class.isInstance(e)) {
                    logger.error(String.format("jepsen client: get exception, key: %s, target: %s", key, leaderId), e);
                }
                throw e;
            }
        });
    }
    
    private class LeaderIdUpdater extends LoopExecutor {
        
        private volatile String leaderId;
        
        private AtomicBoolean safeLock = new AtomicBoolean();
        
        private Set<String> serverThreads = new ConcurrentSet<String>(); 
        
        private ExecutorService executor = Executors.newCachedThreadPool(ThreadUtil.newThreadFactory("get-leader-executor"));
        
        public void reset() {
            leaderId = null;
        }
        
        public void waitFor(long timeout) {
            long start = System.currentTimeMillis();
            while (leaderId == null && System.currentTimeMillis() - start < timeout) {
                try {
                    synchronized (this) {                        
                        wait(1000);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        
        @Override
        public void execute() throws Exception {
            if (!safeLock.compareAndSet(false, true)) {
                return;
            }
            if (leaderId == null) {
                update();
                if (leaderId != null) {
                    synchronized (this) {
                        notifyAll();
                    }
                }
            } 
            Thread.sleep(1);
            safeLock.compareAndSet(true, false);
        }
        
        public void update() throws Exception {
            List<String> serverids = services.keySet().stream()
                    .filter(serverId -> !serverThreads.contains(serverId))
                    .collect(Collectors.toList());
            
            int availableCount = serverids.size();
            int leastCount = services.size() / 2 + 1;
            if (availableCount < leastCount) {
                return;
            }
            
            AtomicReference<String> newLeaderId = new AtomicReference<String>();
            CountDownLatch latch = new CountDownLatch(1);
            
            AtomicInteger totalCnt = new AtomicInteger();
            Map<String, AtomicInteger> counter = new ConcurrentHashMap<String, AtomicInteger>();
            
            long start = System.currentTimeMillis();
            for (String serverid: serverids) {
                serverThreads.add(serverid);
                executor.execute(() -> {
                    StandardService standardService = services.get(serverid);
                    CompletableFuture<String> future = standardService.getLeaderId();
                    future.whenComplete((leaderId, ex) -> {
                        int tCnt = totalCnt.incrementAndGet();
                        if (tCnt == availableCount) {
                            latch.countDown();
                        }
                        if (ex != null) {
                            logger.warn("server id:" + serverid + " get leader id", ex);
                            return;
                        }
                        int cnt = counter.computeIfAbsent(leaderId, key -> new AtomicInteger()).incrementAndGet();
                        if (cnt == leastCount) {
                            newLeaderId.set(leaderId);
                            latch.countDown();
                        }
                    });
                    serverThreads.remove(serverid);
                });
            }
            latch.await(timeout, MILLISECONDS);
            long usetime = System.currentTimeMillis() - start;
            logger.info("get leader id usetime: {}, servers:{}, result: {}", usetime, serverids, newLeaderId.get());
                
            if (newLeaderId.get() != null) {
                logger.info("leader id change, old leader id:{}, new leader id:{}", leaderId, newLeaderId.get());
                this.leaderId = newLeaderId.get();
                return;
            }
        }
        
        @Override
        public void shutdown() {
            while (safeLock.compareAndSet(false, true)) {
                Thread.yield();
            }
            executor.shutdown();
            super.shutdown();
        }
        
        public String getLeaderId() {
            return leaderId;
        }
        
    }

    public void startup() {
        try {
            leaderIdUpdater.startup();
        } catch (Exception e) {
            logger.error("jepsen client: startup exception", e);
        }
    }

    public void shutdown() {
        try {
            leaderIdUpdater.shutdown();
            client.shutdown();
        } catch (Exception e) {
            logger.error("jepsen client: shutdown exception", e);
        }
    }
    
    public static class StandardService {
        
        private ClientService clientService;
        
        private StoreService storeService;
        
        public StandardService(ClientService clientService, StoreService storeService) {
            this.clientService = clientService;
            this.storeService = storeService;
        }

        public CompletableFuture<Void> set(byte[] key, byte[] value) {
            return storeService.set(key, value);
        }

        public CompletableFuture<Boolean> cas(byte[] key, byte[] expect, byte[] update) {
            return storeService.cas(key, expect, update);
        }

        public CompletableFuture<byte[]> get(byte[] key) {
            return storeService.get(key);
        }

        public CompletableFuture<String> getLeaderId() {
            return clientService.getLeaderId();
        }

    }
    
    public static class JepsenError {

        public final static JepsenErrorException METADATA_ERROR = new JepsenErrorException(301);
        
        public final static JepsenErrorException TIMEOUT_ERROR = new JepsenErrorException(401); 
        
        public final static JepsenErrorException CONNECT_ERROR = new JepsenErrorException(501);
        
        public final static JepsenErrorException UNKNOWN_ERROR = new JepsenErrorException(601);

        public static JepsenErrorException getError(Exception e) {
            if (TimeoutException.class.isInstance(e)) {
                return JepsenError.TIMEOUT_ERROR;
            } 
            else if (ConnectException.class.isInstance(e)) {
                return JepsenError.CONNECT_ERROR;
            }
            else {
                return JepsenError.UNKNOWN_ERROR;
            }
        }

    }
    
    public static class JepsenErrorException extends RuntimeException {
        
        private final static long serialVersionUID = -5935896323811693526L;
        
        private final int errorCode;
        
        public JepsenErrorException(int errorCode) {
            this.errorCode = errorCode;
        }

        public int getErrorCode() {
            return errorCode;
        }
        
    }
    
}
