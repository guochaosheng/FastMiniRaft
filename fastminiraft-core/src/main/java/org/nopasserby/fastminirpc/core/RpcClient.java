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

package org.nopasserby.fastminirpc.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.nopasserby.fastminirpc.remoting.NettyClient;
import org.nopasserby.fastminirpc.remoting.RemotingCommand;
import org.nopasserby.fastminirpc.util.ByteUtil;
import org.nopasserby.fastminirpc.util.ProxyUtil;
import org.nopasserby.fastminirpc.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.SystemPropertyUtil;

public class RpcClient {

    private static Logger logger = LoggerFactory.getLogger(RpcClient.class);
    
    private static long rpcRequestTimeout;
    
    static {
        rpcRequestTimeout = SystemPropertyUtil.getLong("io.rpc.request.timeout", 30 * 1000);
        
        logger.debug("-Dio.rpc.request.timeout: {}", rpcRequestTimeout);
    }
    
    private Timer timeoutTimer = new HashedWheelTimer(new DefaultThreadFactory("rpc-timeout", true));
    
    private AtomicLong opaque = new AtomicLong();
    
    private NettyClient client;
    
    private Serializer serializer = SerializerUtil.newJavaSerializer();
    
    private Map<String, Map<Class<?>, Object>> proxyTable = new ConcurrentHashMap<String, Map<Class<?>, Object>>();
    
    private Map<Long, RemotingFuture> futureTable = new ConcurrentHashMap<Long, RemotingFuture>();
    
    private ChannelInboundHandler channelInboundHandler = new SimpleChannelInboundHandler<RemotingCommand>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand response) throws Exception {
            RemotingFuture future = futureTable.remove(response.getOpaque());
            ByteBuf payload = response.getPayload();
            if (future == null) {
                payload.release();
                return;
            }
            future.timeout.cancel();
            
            DataInput input = SerializerUtil.newDataInputDelegate(payload);
            RpcResponse rpcResponse = serializer.readObject(input, RpcResponse.class);
            if (rpcResponse.getException() != null) {
                future.completeExceptionally(rpcResponse.getException());
            } 
            else {
                future.complete(rpcResponse.getReturnObject());
            }
            payload.release();
        }
        
    };
    
    public RpcClient() {
        client = new NettyClient(channelInboundHandler);
    }
    
    public RpcClient(Serializer serializer) {
        this();
        this.serializer = serializer;
    }
    
    public <T> T getService(String host, Class<T> serviceClass) {
        Map<Class<?>, Object> serviceProxyTable = proxyTable.computeIfAbsent(host, key -> new ConcurrentHashMap<>());
        Object serviceProxy = serviceProxyTable.computeIfAbsent(serviceClass, key -> {
            return ProxyUtil.newInterfaceProxy(serviceClass, newInvocationHandler(host, serviceClass));
        });
        return serviceClass.cast(serviceProxy);
    }
    
    private <T> InvocationHandler newInvocationHandler(final String host, final Class<T> serviceClass) {
        return new InvocationHandler() {
            Map<Method, byte[]> services = new HashMap<Method, byte[]>();
            {
                Map<Method, String> methodSignatures = ReflectUtil.getMethodSignatureMap(serviceClass, true);
                methodSignatures.forEach((method, methodSignature) -> {
                    services.put(method, ByteUtil.toByteArray(methodSignature));
                });
            }
            
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Class<?> returnType = method.getReturnType();
                byte[] signature = services.get(method);
                RemotingFuture future = remoteInvoke(host, new RpcRequest(signature, args));
                if (CompletableFuture.class.isAssignableFrom(returnType)) {
                    return future;
                }
                return future.get();
            }
            
        };
    };
    
    private RemotingFuture remoteInvoke(String host, RpcRequest rpcObject) {
        long futureId = opaque.incrementAndGet();
        RemotingFuture future = new RemotingFuture();
        try {
            ByteBuf payload = Unpooled.buffer();
            DataOutput output = SerializerUtil.newDataOutputDelegate(payload);
            serializer.writeObject(rpcObject, output);
            
            RemotingCommand remotingCommand = new RemotingCommand();
            remotingCommand.setOpaque(futureId);
            remotingCommand.setPayload(payload);
            
            futureTable.put(futureId, future);
            
            future.timeout = scheduleFutureTimeout(futureId, rpcRequestTimeout);
            
            client.send(host, remotingCommand);
        } catch (Exception e) {
            futureTable.remove(futureId);
            if (future.timeout != null) {
                future.timeout.cancel();
            }
            future.completeExceptionally(e);
        }
        return future;
    }
    
    private Timeout scheduleFutureTimeout(long futureId, long timeout) {
        return timeoutTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                RemotingFuture future = futureTable.remove(futureId);
                if (future != null) {
                    future.completeExceptionally(RemotingFuture.timeoutException);
                }
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }
    
    public void shutdown() throws Exception {
        client.shutdown();
        timeoutTimer.stop();
    }
    
    private static class RemotingFuture extends CompletableFuture<Object> {
        static TimeoutException timeoutException = new TimeoutException("rpc timeout [ " + rpcRequestTimeout + " ms ]");
        Timeout timeout;
    }
    
}
