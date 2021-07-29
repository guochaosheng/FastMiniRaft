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

package org.nopasserby.fastminiraft.store;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class RingBufferTest {

    @Test
    public void writeTest() {
        int unit = 1024, count = 4;
        RingBuffer buffer = new RingBuffer(unit, count);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset < unit * count * 3) {
            offset += buffer.write(ByteBuffer.wrap(body));
            Assert.assertEquals(offset, buffer.position());
        }
        
    }
    
    @Test
    public void writeWithLockTest() {
        int unit = 1024, count = 4;
        RingBuffer ringBuffer = new RingBuffer(unit, count);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset < unit * count * 3) {
            offset += ringBuffer.writeWithLock(ByteBuffer.wrap(body));
            Assert.assertEquals(offset, ringBuffer.position());
            
            ByteBuffer dst = ByteBuffer.allocate(body.length);
            ringBuffer.read(dst, offset - body.length);
            
            Assert.assertEquals(new String(body), new String(dst.array()));
            
            ringBuffer.unLock(offset);
        }
        
    }
    
    @Test
    public void readTest() {
        int unit = 1024, count = 4;
        RingBuffer ringBuffer = new RingBuffer(unit, count);
        
        byte[] body = "hello world!".getBytes();
        
        Assert.assertEquals(-1, ringBuffer.read(ByteBuffer.allocate(body.length), 0));
        
        long offset = 0;
        while (offset + body.length < unit * count) {
            offset += ringBuffer.write(ByteBuffer.wrap(body));
        }
        
        while (offset != 0) {
            ByteBuffer dst = ByteBuffer.allocate(body.length);
            int read = ringBuffer.read(dst, offset - body.length);
            Assert.assertEquals(body.length, read);
            Assert.assertEquals(new String(body), new String(dst.array(), 0, body.length));
            offset -= read;
        }
        
        ringBuffer.write(ByteBuffer.wrap(body));
        
        Assert.assertEquals(-1, ringBuffer.read(ByteBuffer.allocate(body.length), 0));
    }
    
    @Test
    public void sliceAsReadOnlyTest1() {
        int unit = 1024, count = 4;
        RingBuffer ringBuffer = new RingBuffer(unit, count);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset < unit * count * 3) {
            offset += ringBuffer.write(ByteBuffer.wrap(body));
            Assert.assertEquals(offset, ringBuffer.position());
            
            ByteBuffer b = ringBuffer.sliceAsReadOnly(offset - body.length);
            byte[] dst = new byte[b.remaining()];
            b.get(dst);
            Assert.assertEquals(new String(body), new String(dst));
        }
        
    }
    
    @Test
    public void sliceAsReadOnlyTest2() {
        int unit = 1024, count = 1;
        RingBuffer ringBuffer = new RingBuffer(unit, count);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset < unit * count * 3) {
            offset += ringBuffer.write(ByteBuffer.wrap(body));
            Assert.assertEquals(offset, ringBuffer.position());
            
            ByteBuffer b = ringBuffer.sliceAsReadOnly(offset - body.length);
            byte[] dst = new byte[b.remaining()];
            b.get(dst);
            Assert.assertEquals(new String(body), new String(dst));
        }
        
    }
    
}
