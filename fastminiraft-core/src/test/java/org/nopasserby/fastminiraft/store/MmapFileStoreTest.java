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

public class MmapFileStoreTest {

    @Test
    public void writeAndReadTest() throws IOException {
        String dataPath = System.getProperty("user.dir") + "/mmap.data";
        FileUtil.delete(dataPath); // cleanup history
        
        MmapFileStore mmapFileStore = new MmapFileStore(dataPath, 10 * 1024);
        
        byte[] body = "hello world!".getBytes();
        
        mmapFileStore.write(ByteBuffer.wrap(body));
        
        Assert.assertEquals(body.length, mmapFileStore.position());
        
        byte[] dst = new byte[body.length];
        mmapFileStore.read(ByteBuffer.wrap(dst), 0);
        
        Assert.assertEquals(new String(body), new String(dst));
        
        mmapFileStore.delete();
        FileUtil.delete(dataPath); // cleanup history
    }
    
}