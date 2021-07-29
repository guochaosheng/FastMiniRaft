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

package org.nopasserby.fastminirpc.remoting;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.internal.SystemPropertyUtil;

public class NettyClient {

    private static Logger logger = LoggerFactory.getLogger(NettyClient.class);
    
    private static int idleTimeout, reconnectInterval, connectTimeout;
    
    static {
        idleTimeout = SystemPropertyUtil.getInt("io.remoting.idle.timeout", 30 * 1000);
        reconnectInterval = SystemPropertyUtil.getInt("io.remoting.reconnect.interval", 10);
        connectTimeout = SystemPropertyUtil.getInt("io.remoting.connect.timeout", 3 * 1000);

        logger.debug("-Dio.remoting.idle.timeout: {}", idleTimeout);
        logger.debug("-Dio.remoting.reconnect.interval: {}", reconnectInterval);
        logger.debug("-Dio.remoting.connect.timeout: {}", connectTimeout);
    }
    
    private Bootstrap bootstrap;
    
    private NioEventLoopGroup eventGroup;
    
    private ChannelInboundHandler channelInboundHandler;
    
    private Map<String, ConnectChannel> channels = new ConcurrentHashMap<>();
    
    public NettyClient(ChannelInboundHandler channelInboundHandler) {
        this.channelInboundHandler = channelInboundHandler;
        this.eventGroup = new NioEventLoopGroup();
        this.bootstrap = configureBootstrap(createClient());
    }
    
    private Bootstrap configureBootstrap(Bootstrap bootstrap) {
        configureProcessingPipeline(bootstrap);
        configureTCPIPSettings(bootstrap);
        return bootstrap;
    }
    
    protected void configureTCPIPSettings(Bootstrap bootstrap) {
    }

    protected void configureProcessingPipeline(Bootstrap bootstrap) {
        bootstrap.channel(NioSocketChannel.class).handler(channelHandler());
    }
    
    ChannelHandler channelHandler() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(RemotingProtocol.encodeHandler())
                             .addLast(RemotingProtocol.decodeHandler())
                             .addLast(new IdleChannelHandler(0, 0, idleTimeout, channels))
                             .addLast(RemotingProtocol.newChannelInboundHandlerDelegate(channelInboundHandler));
            }
        };
    }
    
    private Bootstrap createClient() {
        eventGroup = new NioEventLoopGroup();
        return new Bootstrap().group(eventGroup);
    }

    public void shutdown() throws Exception {
        eventGroup.shutdownGracefully().get();
    }
    
    public Channel connect(String host, long timeout) throws Exception {
        String hostname = host.split(":")[0]; 
        Integer port = Integer.parseInt(host.split(":")[1]);
        
        ChannelFuture cf = bootstrap.connect(hostname, port);
        if (!cf.awaitUninterruptibly(timeout)) {
            throw new Exception("connection to the host:" + host + " failed.", cf.cause());
        }
        
        return cf.channel();
    }
    
    public ChannelFuture send(String host, RemotingCommand remotingCommand) throws Exception {
        ConnectChannel connectChannel = channels.computeIfAbsent(host, channel -> new ConnectChannel(this, host, connectTimeout, reconnectInterval));
        return connectChannel.writeAndFlush(remotingCommand);
    }
    
}

class IdleChannelHandler extends IdleStateHandler {

    Map<String, ConnectChannel> channels;
    
    public IdleChannelHandler(int readerIdleTimeSeconds, int writerIdleTimeSeconds, int allIdleTimeSeconds,
            Map<String, ConnectChannel> channels) {
        super(readerIdleTimeSeconds, writerIdleTimeSeconds, allIdleTimeSeconds);
        this.channels = channels;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        close(ctx.channel());
        ctx.fireChannelInactive();
    }
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        IdleStateEvent event = (IdleStateEvent) evt;
        if (event.state() == IdleState.ALL_IDLE) {            
            close(ctx.channel());
        }
        ctx.fireUserEventTriggered(evt);
    }
    
    private void close(Channel channel) {
        channels.remove(channel.remoteAddress().toString().replaceAll(".*/", ""));
        channel.close();
    }
    
}

class ConnectChannel {
    
    private final NettyClient client;
    
    private final String host;
    
    private volatile Channel channel;
    
    private long reconnectInterval;
    
    private long connectTimeout;
    
    private long lastConnect;
    
    public ConnectChannel(NettyClient client, String host, long connTimeout, long reconnInterval) {
        this.client = client;
        this.host = host;
        this.connectTimeout = connTimeout;
        this.reconnectInterval = reconnInterval;
    }

    public ChannelFuture writeAndFlush(Object object) throws Exception {
        Channel channel = this.channel;
        
        if (isConnect()) {
            channel = connect();
        }
        
        if (!isAvailable(channel)) {            
            throw new ConnectException(host + " connection not available");
        }
        
        return channel.isWritable() ? channel.writeAndFlush(object) : channel.writeAndFlush(object).sync();
    }
    
    public Channel connect() throws Exception {
        synchronized (this) {
            if (!isConnect()) return channel;
            try {               
                channel = client.connect(host, connectTimeout);
            } finally {
                lastConnect = System.currentTimeMillis();
            }
            return channel;
        }
    }
    
    public boolean isAvailable(Channel channel) {
        return channel != null && channel.isActive();
    }

    public boolean isConnect() {
        return !isAvailable(channel) && System.currentTimeMillis() - lastConnect > reconnectInterval;
    }

}
