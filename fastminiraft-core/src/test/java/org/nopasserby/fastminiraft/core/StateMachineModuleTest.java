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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminiraft.util.FileUtil;

public class StateMachineModuleTest {

    @Test
    public void getTest1() throws IOException {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path);
        
        DBStateMathine dbStateMathine = new DBStateMathine(path);
        String key = "hello", value = "world";
        
        Assert.assertNull(dbStateMathine.get(key.getBytes()));
        
        dbStateMathine.put(key.getBytes(), value.getBytes());
        
        Assert.assertEquals(value, new String(dbStateMathine.get(key.getBytes())));
        
        dbStateMathine.shutdown();
        FileUtil.delete(path);
    }
    
    @Test
    public void getTest2() throws Exception {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path);
        
        DBStateMathine dbStateMathine = new DBStateMathine(path);
        String key = "hello", value = "world";
        dbStateMathine.put(key.getBytes(), value.getBytes());
        
        dbStateMathine.dump();
        dbStateMathine.execute();
        
        Assert.assertEquals(value, new String(dbStateMathine.get(key.getBytes())));
        
        dbStateMathine.shutdown();
        FileUtil.delete(path);
    }
    
    @Test
    public void deleteTest() throws Exception {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path);
        
        DBStateMathine dbStateMathine = new DBStateMathine(path);
        
        String key = "hello", value = "world";
        dbStateMathine.put(key.getBytes(), value.getBytes());
        
        Assert.assertEquals(value, new String(dbStateMathine.get(key.getBytes())));
        
        dbStateMathine.del(key.getBytes());
        
        Assert.assertNull(dbStateMathine.get(key.getBytes()));
        
        dbStateMathine.dump();
        dbStateMathine.execute();
        
        Assert.assertNull(dbStateMathine.get(key.getBytes()));
        
        dbStateMathine.shutdown();
        FileUtil.delete(path);
    }
    
    @Test
    public void casTest() throws Exception {
        String path = System.getProperty("user.dir") + "/data";
        FileUtil.delete(path);
        
        DBStateMathine dbStateMathine = new DBStateMathine(path);
        
        String key = "hello", expect = "hi", update = "world";
        boolean result = dbStateMathine.cas(key.getBytes(), expect.getBytes(), update.getBytes());
        
        Assert.assertFalse(result);
        
        dbStateMathine.put(key.getBytes(), expect.getBytes());
        
        result = dbStateMathine.cas(key.getBytes(), expect.getBytes(), update.getBytes());
        
        dbStateMathine.put(key.getBytes(), update.getBytes());
        
        result = dbStateMathine.cas(key.getBytes(), expect.getBytes(), update.getBytes());
        
        Assert.assertFalse(result);
        
        dbStateMathine.shutdown();
        FileUtil.delete(path);
    }
    
}
