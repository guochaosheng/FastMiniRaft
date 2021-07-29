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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.nopasserby.fastminiraft.api.ClientService;
import org.nopasserby.fastminiraft.api.StoreService;
import org.nopasserby.fastminiraft.boot.ServiceSerializer;
import org.nopasserby.fastminiraft.util.ThreadUtil;
import org.nopasserby.fastminirpc.core.RpcClient;

import io.netty.util.internal.ConcurrentSet;

public class ThroughputTest {
    
    String serverCluster = System.getProperty("server.cluster", "n1-0.0.0.0:6001;n2-0.0.0.0:6002;n3-0.0.0.0:6003;");

    int threadCount = Integer.parseInt(System.getProperty("thread.count", "4"));
    
    int messageSize = Integer.parseInt(System.getProperty("message.size", "128"));
    
    long messageCount = Long.parseLong(System.getProperty("message.count", Long.MAX_VALUE + ""));
    
    StatsThroughput statsThroughput = new StatsThroughput();
    
    AtomicLong blance = new AtomicLong();
    
    public static void main(String[] args) throws Exception {
        ThroughputTest throughputTest = new ThroughputTest();
        throughputTest.run(args);
    }
    
    public void run(String[] args) throws Exception {
        
        // Print start info
        StringBuilder startInfo = new StringBuilder(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        startInfo.append(" ready to start raft throughput benchmark, server cluster is ");
        startInfo.append(serverCluster);
        startInfo.append(",thread count is:").append(threadCount);
        startInfo.append(",message size is:").append(messageSize);
        startInfo.append(",message count is:").append(messageCount);
        System.out.println(startInfo.toString());
        
        StoreService storeService = createStoreService(serverCluster);
        statsThroughput.startup();
        startupOperations(storeService);
    }
    
    public StoreService createStoreService(String serverCluster) throws Exception {
        ServiceSerializer serviceSerializer = new ServiceSerializer();
        RpcClient rpcClient = new RpcClient(serviceSerializer);
        
        Map<String, String> servers = Arrays.asList(serverCluster.split(";")).stream()
                .collect(Collectors.toMap(s -> s.split("-")[0], s -> s.split("-")[1]));
        
        String host = servers.values().iterator().next();
        ClientService clientService = rpcClient.getService(host, ClientService.class);
        
        String leaderId = clientService.getLeaderId().get();
        System.out.printf("leader id: %s %n", leaderId);
        
        StoreService storeService = rpcClient.getService(servers.get(leaderId), StoreService.class);
        return storeService;
    }
    
    public void startupOperations(StoreService storeService) throws Exception {
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        AtomicLong remainCounter = new AtomicLong(messageCount);
        
        byte[] body = createBody("salt", 128);
        
        Set<Long> set = new ConcurrentSet<Long>();
        
        Runnable processor = new Runnable() {
            
            @Override
            public void run() {
                while ((remainCounter.decrementAndGet()) >= 0) {
                    
                    if (blance.get() > 1024 * 64) {
                        ThreadUtil.sleep(10);
                    }
                    
                    long uniqueTimestamp = TimeUtil.uniqueTimestamp();
                    set.add(uniqueTimestamp);
                    CompletableFuture<Long> future = storeService.add(body);
                    
                    blance.incrementAndGet();
                    
                    future.whenComplete((index, ex) -> {
                        
                        blance.decrementAndGet();
                        
                        set.remove(uniqueTimestamp);
                        
                        if (index != null && index > 0) {
                            statsThroughput.getReceiveResponseSuccessCount().incrementAndGet();
                            
                            statsThroughput.offerCurrentRT(calculateRT(uniqueTimestamp));
                        } else {
                            statsThroughput.getReceiveResponseFailedCount().incrementAndGet();                           
                        }
                        
                    });
                }
                latch.countDown();
            }
        };
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.execute(processor);
        }
        latch.await();
        
        executor.shutdown();
    }

    byte[] createBody(String salt, int size) {
        StringBuilder stringBuilder = new StringBuilder();
        byte[] b = salt.getBytes();
        for (int i = 0; i < size; i++) {
            stringBuilder.append((char) b[i % b.length]);
        }
        return stringBuilder.toString().getBytes();
    }
    
    long calculateRT(long timestamp) {
        long startTimeMillis = (timestamp >> 22) + 1288834974657L;// valid only if timeutil.uniqueTimestamp is used
        return System.currentTimeMillis() - startTimeMillis;
    }
    
}
