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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

public class RemotingProtocol {
    
    public static final String CODE = "RAFT";
    
    public static final String VERSION = "0.1.1";
    
    public static int currentVersion() {
        return (Integer.parseInt(VERSION.split("\\.")[0]) << 24) // major version number occupies two hexadecimal digits
             + (Integer.parseInt(VERSION.split("\\.")[1]) << 16) // minor version number occupies two hexadecimal digits
             + (Integer.parseInt(VERSION.split("\\.")[2]));      // revision number occupies four hexadecimal digits
    }
    
    public static ChannelOutboundHandler encodeHandler() {
        return new MessageToByteEncoder<RemotingCommand>() {
            @Override
            protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
                RemotingProtocol.encode(remotingCommand, out);
            }
        };
    }
    
    public static ChannelInboundHandler decodeHandler() {
        return new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 17, 4, 0, 0) {
            protected RemotingCommand decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
                Object decoded = super.decode(ctx, in);
                if (decoded == null) return null;
                RemotingCommand remotingCommand = new RemotingCommand();
                RemotingProtocol.decode(remotingCommand, (ByteBuf) decoded);
                return remotingCommand;
            }
        };
    }
    
    public static void encode(RemotingCommand remotingCommand, ByteBuf buffer) {
        buffer.writeInt(remotingCommand.getCode());
        buffer.writeInt(currentVersion());
        buffer.writeLong(remotingCommand.getOpaque());
        buffer.writeByte(remotingCommand.getFlag());
        buffer.writeInt(remotingCommand.getPayload().readableBytes());
        buffer.writeBytes(remotingCommand.getPayload());
    }
    
    public static void decode(RemotingCommand remotingCommand, ByteBuf buffer) {
        remotingCommand.setCode(buffer.readInt());
        remotingCommand.setVersion(buffer.readInt());
        remotingCommand.setOpaque(buffer.readLong());
        remotingCommand.setFlag(buffer.readByte());
        remotingCommand.setPayload(buffer.readSlice(buffer.readInt()));
    }
    
    public static ChannelInboundHandler newChannelInboundHandlerDelegate(ChannelInboundHandler channelInboundHandler) {
        return new ChannelInboundHandlerDelegate(channelInboundHandler);
    }
    
    public static class ChannelInboundHandlerDelegate implements ChannelInboundHandler {

        private ChannelInboundHandler channelInboundHandler;
        
        public ChannelInboundHandlerDelegate(ChannelInboundHandler channelInboundHandler) {
            this.channelInboundHandler = channelInboundHandler;
        }
        
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.handlerAdded(ctx);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.handlerRemoved(ctx);
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            channelInboundHandler.channelRead(ctx, msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.channelReadComplete(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            channelInboundHandler.userEventTriggered(ctx, evt);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            channelInboundHandler.channelWritabilityChanged(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            channelInboundHandler.exceptionCaught(ctx, cause);
        }
        
    }
    
}
