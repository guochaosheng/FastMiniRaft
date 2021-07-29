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

import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminiraft.util.FileUtil;

public class SegmentFileStoreTest {

    @Test
    public void writeAndReadTest() throws IOException, InterruptedException {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path); // cleanup history
        
        SegmentFileStore segmentFileStore = new SegmentFileStore(path, 1024 * 16);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset <= segmentFileStore.getSegmentSize() * 3) {
            offset += segmentFileStore.write(ByteBuffer.wrap(body));            
            Assert.assertEquals(offset, segmentFileStore.position());
        }
        
        while (offset > 0) {
            ByteBuffer dst = ByteBuffer.allocate(body.length);            
            offset -= segmentFileStore.read(dst, offset - body.length);
            Assert.assertEquals(new String(body), new String(dst.array()));
        }
        
        segmentFileStore.delete();
        FileUtil.delete(path); // cleanup history
    }
    
    @Test
    public void truncateTest() throws IOException {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path); // cleanup history
        
        SegmentFileStore segmentFileStore = new SegmentFileStore(path, 1024 * 16);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset <= segmentFileStore.getSegmentSize() * 3) {
            offset += segmentFileStore.write(ByteBuffer.wrap(body));            
            Assert.assertEquals(offset, segmentFileStore.position());
        }
        
        long truncateIndex = segmentFileStore.getSegmentSize() / body.length * body.length + body.length;
        segmentFileStore.truncate(truncateIndex);
        Assert.assertEquals(truncateIndex, segmentFileStore.position());
        
        while (offset > 0) {
            ByteBuffer dst = ByteBuffer.allocate(body.length);            
            int read = segmentFileStore.read(dst, offset - body.length);
            if (offset > truncateIndex) {
                Assert.assertEquals(-1, read);
            } else {                
                Assert.assertEquals(body.length, read);
                Assert.assertEquals(new String(body), new String(dst.array()));
            }
            offset -= body.length;
        }
        
        
        segmentFileStore.truncate(0);
        Assert.assertEquals(0, segmentFileStore.position());
        
        segmentFileStore.delete();
        FileUtil.delete(path); // cleanup history
    }
    
    @Test
    public void closeTest() throws IOException {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path); // cleanup history
        
        SegmentFileStore segmentFileStore = new SegmentFileStore(path, 1024 * 16);
        
        byte[] body = "hello world!".getBytes();
        
        long offset = 0;
        while (offset <= segmentFileStore.getSegmentSize() * 3) {
            offset += segmentFileStore.write(ByteBuffer.wrap(body));            
            Assert.assertEquals(offset, segmentFileStore.position());
        }
        
        segmentFileStore.close();
        segmentFileStore = new SegmentFileStore(path, 1024 * 16);
        segmentFileStore.close();
        segmentFileStore = new SegmentFileStore(path, 1024 * 16);
        
        Assert.assertEquals(offset, segmentFileStore.position());
        
        segmentFileStore.delete();
        FileUtil.delete(path); // cleanup history
    }
    
}
