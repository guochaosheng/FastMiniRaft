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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.nopasserby.fastminirpc.util.ByteUtil;

import io.netty.buffer.ByteBuf;

public class SerializerUtil {

    public static Serializer newJavaSerializer() {
        return new JavaSerializer();
    }
    
    public static DataInputDelegate newDataInputDelegate(ByteBuf bytebuf) {
        return new DataInputDelegate(bytebuf);
    }
    
    public static DataOutputDelegate newDataOutputDelegate(ByteBuf bytebuf) {
        return new DataOutputDelegate(bytebuf);
    }
    
    static class JavaSerializer implements Serializer {
        
        @Override
        public void writeObject(Object obj, DataOutput output) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(obj);
                oos.close();
                byte[] byteArray = baos.toByteArray();
                output.writeInt(byteArray.length);
                output.write(byteArray);
            } catch (Exception e) {
                throw new IllegalArgumentException(obj.getClass() + " write error", e);
            }
        }

        @Override
        public <T> T readObject(DataInput input, Class<T> clazz) {
            try {
                byte[] byteArray = new byte[input.readInt()];
                input.readFully(byteArray);
                ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
                ObjectInputStream ois = new ObjectInputStream(bais);
                Object obj = ois.readObject();
                ois.close();
                return clazz.cast(obj);
            } catch (Exception e) {
                throw new IllegalArgumentException(clazz + " read error", e);
            }
        }
        
    }
    
    static class DataInputDelegate implements DataInput {
        
        private ByteBuf bytebuf;
        
        public DataInputDelegate(ByteBuf bytebuf) {
            this.bytebuf = bytebuf;
        }
        
        @Override
        public void readFully(byte[] b) throws IOException {
            bytebuf.readBytes(b);
        }
    
        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            bytebuf.readBytes(b, off, len);
        }
    
        @Override
        public int skipBytes(int n) throws IOException {
            bytebuf.skipBytes(n);
            return n;
        }
    
        @Override
        public boolean readBoolean() throws IOException {
            return bytebuf.readBoolean();
        }
    
        @Override
        public byte readByte() throws IOException {
            return bytebuf.readByte();
        }
    
        @Override
        public int readUnsignedByte() throws IOException {
            return bytebuf.readUnsignedByte();
        }
    
        @Override
        public short readShort() throws IOException {
            return bytebuf.readShort();
        }
    
        @Override
        public int readUnsignedShort() throws IOException {
            return bytebuf.readUnsignedShort();
        }
    
        @Override
        public char readChar() throws IOException {
            return bytebuf.readChar();
        }
    
        @Override
        public int readInt() throws IOException {
            return bytebuf.readInt();
        }
    
        @Override
        public long readLong() throws IOException {
            return bytebuf.readLong();
        }
    
        @Override
        public float readFloat() throws IOException {
            return bytebuf.readFloat();
        }
    
        @Override
        public double readDouble() throws IOException {
            return bytebuf.readDouble();
        }
    
        @Override
        public String readLine() throws IOException {
            throw new UnsupportedOperationException("readLine");
        }
    
        @Override
        public String readUTF() throws IOException {
            byte[] b = new byte[bytebuf.readShort()];
            bytebuf.readBytes(b);
            return ByteUtil.toString(b);
        }
        
        public ByteBuf getByteBuf() {
            return bytebuf;
        }
        
        public void clear() {
            bytebuf.clear();
        }
        
    }
    
    static class DataOutputDelegate implements DataOutput {
    
        private ByteBuf bytebuf;
        
        public DataOutputDelegate(ByteBuf bytebuf) {
            this.bytebuf = bytebuf;
        }
        
        @Override
        public void write(int b) throws IOException {
            bytebuf.writeByte(b);
        }
    
        @Override
        public void write(byte[] b) throws IOException {
            bytebuf.writeBytes(b);
        }
    
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            bytebuf.writeBytes(b, off, len);
        }
    
        @Override
        public void writeBoolean(boolean v) throws IOException {
            bytebuf.writeBoolean(v);
        }
    
        @Override
        public void writeByte(int v) throws IOException {
            bytebuf.writeByte(v);
        }
    
        @Override
        public void writeShort(int v) throws IOException {
            bytebuf.writeShort(v);
        }
    
        @Override
        public void writeChar(int v) throws IOException {
            bytebuf.writeChar(v);
        }
    
        @Override
        public void writeInt(int v) throws IOException {
            bytebuf.writeInt(v);
        }
    
        @Override
        public void writeLong(long v) throws IOException {
            bytebuf.writeLong(v);
        }
    
        @Override
        public void writeFloat(float v) throws IOException {
            bytebuf.writeFloat(v);
        }
    
        @Override
        public void writeDouble(double v) throws IOException {
            bytebuf.writeDouble(v);
        }
    
        @Override
        public void writeBytes(String s) throws IOException {
            int len = s.length();
            for (int i = 0; i < len; i ++) {
                write((byte) s.charAt(i));
            }
        }
    
        @Override
        public void writeChars(String s) throws IOException {
            int len = s.length();
            for (int i = 0 ; i < len ; i ++) {
                writeChar(s.charAt(i));
            }
        }
    
        @Override
        public void writeUTF(String s) throws IOException {
            byte[] b = ByteUtil.toByteArray(s);
            bytebuf.writeShort(b.length);
            bytebuf.writeBytes(b);
        }
        
        public ByteBuf getByteBuf() {
            return bytebuf;
        }

        public void clear() {
            bytebuf.clear();
        }
        
    }
    
}
