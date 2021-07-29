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
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.nopasserby.fastminirpc.remoting.NettyServer;
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
import io.netty.util.internal.SystemPropertyUtil;

public class RpcServer {
    
    private static Logger logger = LoggerFactory.getLogger(RpcServer.class);

    private static boolean isCompressSignature;
    
    static {
        isCompressSignature = SystemPropertyUtil.getBoolean("io.rpc.compress.signature", false);
        
        logger.debug("-Dio.rpc.compress.signature: {}", isCompressSignature);
    }
    
    private NettyServer server;
    
    private Set<LifeCycle> serviceWithLifeCycle = new HashSet<LifeCycle>();
    
    private Serializer serializer = SerializerUtil.newJavaSerializer();
    
    private Map<Integer, Function<Object[], Object>> servicesWithCompressSignature = new HashMap<Integer, Function<Object[], Object>>();
    private Map<String, Function<Object[], Object>> services = new HashMap<String, Function<Object[], Object>>();
    
    private ChannelInboundHandler channelInboundHandler = new SimpleChannelInboundHandler<RemotingCommand>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
            dispatch(ctx, request);
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("", cause);
        }
    };
    
    public RpcServer(String host) {
        this.server = new NettyServer(host, channelInboundHandler);
    }
    
    public RpcServer(String host, Serializer serializer) {
        this(host);
        this.serializer = serializer;
    }

    public <T> void registerService(Class<T> serviceApi, T serviceImpl) {
        Map<Method, String> methodSignatures = ReflectUtil.getMethodSignatureMap(serviceApi, true);
        methodSignatures.forEach((method, methodSignature) -> {
            Function<Object[], Object> methodProxy = ProxyUtil.newMethodProxy(method, serviceImpl);
            services.putIfAbsent(methodSignature, methodProxy);
            if (isCompressSignature && servicesWithCompressSignature.putIfAbsent(methodSignature.hashCode(), methodProxy) != null) {
                throw new IllegalArgumentException("method signature:" + methodSignature.hashCode() + " duplicate");
            }
        });
        if (LifeCycle.class.isInstance(serviceImpl)) {            
            serviceWithLifeCycle.add((LifeCycle) serviceImpl);
        }
    }
    
    public void dispatch(ChannelHandlerContext ctx, RemotingCommand request) throws IOException {
        int code = request.getCode();
        long opaque = request.getOpaque();
        int flag = request.getFlag();
        ByteBuf payload = request.getPayload();
        
        DataInput input = SerializerUtil.newDataInputDelegate(payload);
        RpcRequest rpcRequest = serializer.readObject(input, RpcRequest.class);
        byte[] methodSignature = rpcRequest.getMethodSignature();
        Object[] parameters = rpcRequest.getParameters();
        
        Object result = null;
        try {
            if (isCompressSignature) 
                result = servicesWithCompressSignature.get(ByteUtil.toInt(methodSignature)).apply(parameters);
            else 
                result = services.get(ByteUtil.toString(methodSignature)).apply(parameters);
        } catch (Exception e) {
            logger.debug("", e);
            RpcResponse rpcObject = new RpcResponse(methodSignature, e);
            writeResponse(ctx, code, opaque, flag, rpcObject);
            return;
        } finally {
            payload.release();
        }
        
        if (CompletableFuture.class.isInstance(result)) {
            BiConsumer<Object, Throwable> action = (res, ex) -> {
                RpcResponse rpcObject = new RpcResponse(methodSignature,  ex != null ? ex : res);
                writeResponse(ctx, code, opaque, flag, rpcObject);
            };
            ((CompletableFuture<?>) result).whenComplete(action);
        }
        else {
            RpcResponse rpcObject = new RpcResponse(methodSignature, result);
            writeResponse(ctx, code, opaque, flag, rpcObject);
        }
    }
    
    public void writeResponse(ChannelHandlerContext ctx, int code, long opaque, int flag, RpcResponse rpcObject) {
        ByteBuf payload = Unpooled.buffer();
        DataOutput dataOutput = SerializerUtil.newDataOutputDelegate(payload);
        serializer.writeObject(rpcObject, dataOutput);
        RemotingCommand response = new RemotingCommand(code, opaque, flag, payload);
        ctx.writeAndFlush(response);
    }

    public void startup() throws Exception {
        for (LifeCycle service: serviceWithLifeCycle) {            
            service.startup();
        }
        server.startup();
    }
    
    public void shutdown() throws Exception {
        for (LifeCycle service: serviceWithLifeCycle) {
            service.shutdown();            
        }
        server.shutdown();
    }
    
}
