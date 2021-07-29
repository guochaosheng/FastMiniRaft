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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminiraft.util.FileUtil;

public class BufferedStoreTest {

    @Test
    public void writeAndReadTest() throws IOException {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path); // cleanup history
        
        SegmentFileStore segmentFileStore = new SegmentFileStore(path, 16 * 1024) {
            @Override
            public Store createSegmentStore(String path) throws IOException {
                return new MmapFileStore(path, 16 * 1024);
            }
        };
        
        int bufferCapacity = 64 * 1024;
        int bufferThreshold = 8 * 1024;
        BufferedStore bufferedStore = new BufferedStore(segmentFileStore, bufferCapacity, bufferThreshold, false);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        int testBufferSize = 4 * 1024; // < BufferedStore.NOT_WRITTEN_THRESHOLD
        while (offset <= testBufferSize) {
            offset += bufferedStore.write(ByteBuffer.wrap(body));    
            Assert.assertEquals(offset, bufferedStore.position());
            
            ByteBuffer dst = ByteBuffer.allocate(body.length);            
            int read = bufferedStore.read(dst, offset - body.length);
            Assert.assertEquals(body.length, read);
            Assert.assertEquals(new String(body), new String(dst.array()));
        }
        
        Assert.assertEquals(0, segmentFileStore.position());
        
        bufferedStore.flush();
        
        Assert.assertEquals(testBufferSize / body.length * body.length + body.length, segmentFileStore.position());
        
        int rng = ThreadLocalRandom.current().nextInt(4);
        testBufferSize = bufferCapacity + bufferCapacity * rng; // > bufferCapacity
        while (offset <= testBufferSize) {
            offset += bufferedStore.write(ByteBuffer.wrap(body));    
            Assert.assertEquals(offset, bufferedStore.position());
            
            ByteBuffer dst = ByteBuffer.allocate(body.length);            
            int read = bufferedStore.read(dst, offset - body.length);
            Assert.assertEquals(body.length, read);
            Assert.assertEquals(new String(body), new String(dst.array()));
        }
        
        bufferedStore.flush();
        
        Assert.assertEquals(testBufferSize / body.length * body.length + body.length, segmentFileStore.position());
        
        bufferedStore.delete();
        FileUtil.delete(path); // cleanup history
    }
    
    @Test
    public void truncateTest() throws IOException {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path); // cleanup history
        
        SegmentFileStore segmentFileStore = new SegmentFileStore(path, 16 * 1024);
        
        BufferedStore bufferedStore = new BufferedStore(segmentFileStore, 16 * 1024, 8 * 1024, false);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset <= segmentFileStore.getSegmentSize() * 3) {
            offset += bufferedStore.write(ByteBuffer.wrap(body));            
            Assert.assertEquals(offset, bufferedStore.position());
            bufferedStore.flush();
        }
        
        bufferedStore.truncate(segmentFileStore.getSegmentSize());
        Assert.assertEquals(segmentFileStore.getSegmentSize(), bufferedStore.position());
        Assert.assertEquals(segmentFileStore.getSegmentSize(), segmentFileStore.position());
        
        bufferedStore.truncate(0);
        Assert.assertEquals(0, bufferedStore.position());
        Assert.assertEquals(0, segmentFileStore.position());
        
        bufferedStore.delete();
        FileUtil.delete(path); // cleanup history
    }
    
    @Test
    public void closeTest() throws IOException {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path); // cleanup history
        
        SegmentFileStore segmentFileStore = new SegmentFileStore(path, 16 * 1024);
        
        Store bufferedStore = new BufferedStore(segmentFileStore, 16 * 1024, 8 * 1024, false);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset <= segmentFileStore.getSegmentSize() * 3) {
            offset += bufferedStore.write(ByteBuffer.wrap(body));            
            Assert.assertEquals(offset, bufferedStore.position());
            bufferedStore.flush();
        }
        
        bufferedStore.close();
        bufferedStore = new BufferedStore(segmentFileStore, 16 * 1024, 8 * 1024, false);
        bufferedStore.close();
        bufferedStore = new BufferedStore(segmentFileStore, 16 * 1024, 8 * 1024, false);
        
        Assert.assertEquals(offset, bufferedStore.position());
        
        bufferedStore.delete();
        FileUtil.delete(path); // cleanup history
    }
    
}
