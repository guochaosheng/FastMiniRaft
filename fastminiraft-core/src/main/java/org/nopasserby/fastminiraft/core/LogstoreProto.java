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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public enum LogstoreProto {

    NOOP(101, new NoopCodec()),
    
    ADD_SER(201, new AddSerCodec()),
    
    DEL_SER(202, new DelSerCodec()),
    
    SET(301, new SetCodec()),
    
    CAS(302, new CasCodec()),
    
    DEL(303, new DelCodec()),
    
    ADD(305, new AddCodec());
    
    private int code;
    
    private Codec codec;
    
    private static Map<Integer, LogstoreProto> map = new HashMap<Integer, LogstoreProto>();
    
    static {
        for (LogstoreProto logstoreProto :LogstoreProto.values()) {            
            map.put(logstoreProto.getCode(), logstoreProto);
        }
    }

    private LogstoreProto(int code, Codec desc) {
        this.code = code;
        this.codec = desc;
    }
    
    public int getCode() {
        return code;
    }

    public Codec getCodec() {
        return codec;
    }
    
    public static LogstoreProto valueOf(int code) {
        return map.get(code);
    }
    
    public static interface Codec {
        
        public byte[] encode(Object... src);
        
        public Object[] decode(byte[] src);
        
    }
    
    public static class NoopCodec implements Codec {
        @Override
        public byte[] encode(Object... src) {
            return new byte[] {};
        }
        @Override
        public Object[] decode(byte[] src) {
            return new Object[] {};
        }
    }
    
    public static class AddSerCodec implements Codec {
        @Override
        public byte[] encode(Object... src) {
            byte[] serverId = ((String) src[0]).getBytes();
            byte[] serverHost = ((String) src[1]).getBytes();
            int len = Short.BYTES + serverId.length + Short.BYTES + serverHost.length;
            byte[] dest = new byte[len];
            ByteBuffer.wrap(dest).putShort((short) serverId.length).put(serverId)
                                 .putShort((short) serverHost.length).put(serverHost);
            return dest;
        }
        @Override
        public Object[] decode(byte[] src) {
            ByteBuffer wrap = ByteBuffer.wrap(src);
            byte[] serverId = new byte[wrap.getShort()];
            wrap.get(serverId);
            byte[] serverHost = new byte[wrap.getShort()];
            wrap.get(serverHost);
            return new Object[] { new String(serverId), new String(serverHost) };
        }
    }
    
    public static class DelSerCodec implements Codec {
        @Override
        public byte[] encode(Object... src) {
            return ((String) src[0]).getBytes();
        }
        @Override
        public Object[] decode(byte[] src) {
            return new Object[] { new String(src) };
        }
    }
    
    public static class SetCodec implements Codec {
        @Override
        public byte[] encode(Object... src) {
            byte[] key = (byte[]) src[0];
            byte[] value = (byte[]) src[1];
            int len = Short.BYTES + key.length + Short.BYTES + value.length;
            byte[] dest = new byte[len];
            ByteBuffer.wrap(dest).putShort((short) key.length).put(key)
                                 .putShort((short) value.length).put(value);
            return dest;
        }
        @Override
        public Object[] decode(byte[] src) {
            ByteBuffer wrap = ByteBuffer.wrap(src);
            byte[] key = new byte[wrap.getShort()];
            wrap.get(key);
            byte[] value = new byte[wrap.getShort()];
            wrap.get(value);
            return new Object[] { key, value };
        }
    }
    
    public static class CasCodec implements Codec {
        @Override
        public byte[] encode(Object... src) {
            byte[] key = (byte[]) src[0];
            byte[] expect = (byte[]) src[1];
            byte[] update = (byte[]) src[2];
            int len = Short.BYTES + key.length + Short.BYTES + expect.length + Short.BYTES + update.length;
            byte[] dest = new byte[len];
            ByteBuffer.wrap(dest).putShort((short) key.length).put(key)
                                 .putShort((short) expect.length).put(expect)
                                 .putShort((short) update.length).put(update);
            return dest;
        }
        @Override
        public Object[] decode(byte[] src) {
            ByteBuffer wrap = ByteBuffer.wrap(src);
            byte[] key = new byte[wrap.getShort()];
            wrap.get(key);
            byte[] expect = new byte[wrap.getShort()];
            wrap.get(expect);
            byte[] update = new byte[wrap.getShort()];
            wrap.get(update);
            return new Object[] { key, expect, update };
        }
    }
    
    public static class DelCodec implements Codec {
        @Override
        public byte[] encode(Object... src) {
            return (byte[]) src[0];
        }
        @Override
        public Object[] decode(byte[] src) {
            return new Object[] { src };
        }
    }
    
    public static class AddCodec implements Codec {
        @Override
        public byte[] encode(Object... src) {
            return (byte[]) src[0];
        }
        @Override
        public Object[] decode(byte[] src) {
            return new Object[] { src };
        }       
    }
}
