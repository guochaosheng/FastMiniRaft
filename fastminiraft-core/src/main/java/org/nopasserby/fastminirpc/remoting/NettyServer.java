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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyServer {

    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);
    
    private NioEventLoopGroup parentGroup;
    
    private NioEventLoopGroup childGroup;
    
    private String host;
    
    private ChannelInboundHandler channelInboundHandler;
    
    public NettyServer(String host, ChannelInboundHandler channelInboundHandler) {
        this.host = host;
        this.channelInboundHandler = channelInboundHandler;
    }
    
    public void startup() throws Exception {
        ServerBootstrap bootstrap = createServer();
        configureProcessingPipeline(bootstrap);
        configureTCPIPSettings(bootstrap);
        startServer(bootstrap);
    }
    
    public void shutdown() throws Exception {
        parentGroup.shutdownGracefully().get();
        childGroup.shutdownGracefully().get();
    }
    
    protected ServerBootstrap createServer() {
        parentGroup = new NioEventLoopGroup();
        childGroup = new NioEventLoopGroup();
       
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(parentGroup, childGroup);
        return bootstrap;
    }

    protected void startServer(ServerBootstrap bootstrap) throws InterruptedException {
        String hostname = host.split(":")[0];
        Integer port = Integer.parseInt(host.split(":")[1]);
        
        Channel ch = bootstrap.bind(hostname, port).sync().channel();
        logger.info("remoting server started and listen on {}", ch.localAddress());
        ch.closeFuture().sync();
    }

    protected void configureTCPIPSettings(ServerBootstrap bootstrap) {
    }

    protected void configureProcessingPipeline(ServerBootstrap bootstrap) {
        bootstrap.channel(NioServerSocketChannel.class).childHandler(channelHandler());
    }

    ChannelHandler channelHandler() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(RemotingProtocol.encodeHandler())
                             .addLast(RemotingProtocol.decodeHandler())
                             .addLast(RemotingProtocol.newChannelInboundHandlerDelegate(channelInboundHandler));
            }
        };
    }
    
}
