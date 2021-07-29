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

public class SimpleFileStoreTest {

    @Test
    public void writeAndReadTest() throws IOException {
        String path = System.getProperty("user.dir") + "/simple.data";
        FileUtil.delete(path); // cleanup history
        
        SimpleFileStore simpleFileStore = new SimpleFileStore(path);
        
        byte[] body = "hello world!".getBytes();
        
        simpleFileStore.write(ByteBuffer.wrap(body));
        simpleFileStore.flush();
        
        Assert.assertEquals(body.length, simpleFileStore.position());
        
        byte[] dst = new byte[body.length];
        simpleFileStore.read(ByteBuffer.wrap(dst), 0);
        
        Assert.assertEquals(new String(body), new String(dst));
        
        simpleFileStore.delete();
        FileUtil.delete(path); // cleanup history
    }
    
}
