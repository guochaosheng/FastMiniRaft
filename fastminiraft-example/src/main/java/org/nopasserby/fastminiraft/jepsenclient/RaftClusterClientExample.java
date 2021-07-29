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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.nopasserby.fastminiraft.api.ClientService;
import org.nopasserby.fastminiraft.api.StoreService;
import org.nopasserby.fastminiraft.boot.ServiceSerializer;
import org.nopasserby.fastminirpc.core.RpcClient;

public class RaftClusterClientExample {
    
    public static void main(String[] args) throws Exception {
        RpcClient rpcClient = new RpcClient(new ServiceSerializer());
        
        String serverCluster = "n1-127.0.0.1:6001;n2-127.0.0.1:6002;n3-127.0.0.1:6003;";
        Map<String, String> servers = Arrays.asList(serverCluster.split(";")).stream()
                .collect(Collectors.toMap(s -> s.split("-")[0], s -> s.split("-")[1]));
        
        String host = servers.values().iterator().next();
        ClientService clientService = rpcClient.getService(host, ClientService.class);
        
        // get leaderId
        String leaderId = clientService.getLeaderId().get();
        System.out.printf("request get leader id, response: %s %n", leaderId);
        
        StoreService storeService = rpcClient.getService(servers.get(leaderId), StoreService.class);
        
        // set
        byte[] key = "hello".getBytes();
        byte[] value = "hi".getBytes();
        CompletableFuture<Void> setFuture = storeService.set(key, value);
        setFuture.get(30, TimeUnit.SECONDS);
        System.out.printf("request set [key: %s, value: %s] %n", new String(key), new String(value));

        // cas
        byte[] expect = "hi".getBytes();
        byte[] update = "world".getBytes();
        CompletableFuture<Boolean> casFuture = storeService.cas(key, expect, update);
        boolean result = casFuture.get(30, TimeUnit.SECONDS);
        System.out.printf("request cas [key: %s, expect: %s, update: %s], response: %s %n", 
                new String(key), new String(expect), new String(update), result);
        
        // cas
        expect = "hi".getBytes();
        update = "world".getBytes();
        result = casFuture.get(30, TimeUnit.SECONDS);
        System.out.printf("request cas [key: %s, expect: %s, update: %s], response: %s %n", 
                new String(key), new String(expect), new String(update), result);

        // get
        CompletableFuture<byte[]> getByKeyFuture = storeService.get(key);
        value = getByKeyFuture.get(30, TimeUnit.SECONDS);
        System.out.printf("request get [key: %s], response: %s %n", new String(key), value == null ? value : new String(value));
        
        // del
        CompletableFuture<Void> delFuture = storeService.del(key);
        delFuture.get(30, TimeUnit.SECONDS);
        System.out.printf("request del [key: %s] %n", new String(key));
      
        // add
        key = "hello world".getBytes();
        CompletableFuture<Long> addFuture = storeService.add(key);
        long index = addFuture.get(30, TimeUnit.SECONDS);
        System.out.printf("request add [body: %s], response: %s %n", new String(key), index);
        
        // get by index
        CompletableFuture<byte[]> getByIndexFuture = storeService.get(index);
        value = getByIndexFuture.get(30, TimeUnit.SECONDS);
        System.out.printf("request get [index: %s], response: %s %n", index, value == null ? value : new String(value));
        
        rpcClient.shutdown();
    }

}
