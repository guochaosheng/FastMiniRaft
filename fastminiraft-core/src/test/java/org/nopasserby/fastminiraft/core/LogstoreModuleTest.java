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

import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminiraft.api.Entry;
import org.nopasserby.fastminiraft.api.Logstore;
import org.nopasserby.fastminiraft.util.FileUtil;
import org.nopasserby.fastminirpc.core.RpcClient;

public class LogstoreModuleTest {

    @Test
    public void appendTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
        
        options.setBufferCapacityOfEntry(8192);
        options.setBufferCapacityOfIndex(8192);
        Node node = new Node(options, new RpcClient());
        Logstore logstore = new LogstoreModule(node);
        int currentTerm = 10;
        long lastLogIndex = node.getLastLogIndex();
        
        byte[] body = "Hello world!".getBytes();
        int size = 4 * 8192;
        int written = 0;
        long index = lastLogIndex;
        while (written < size) {
            Entry entry = new Entry();
            entry.setTerm(currentTerm);
            entry.setIndex(index + 1);
            entry.setBody(body);
            logstore.append(entry);
            written += 36 + body.length; // entry_index_unit=36
            index ++;
        }
        
        while (index > lastLogIndex) {
            Entry entry = logstore.get(index);
            Assert.assertEquals(currentTerm, entry.getTerm());
            Assert.assertEquals(new String(body), new String(entry.getBody()));
            index --;
        }
        
        logstore.flush();
        logstore.delete();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
    }
    
    @Test
    public void getTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
        
        options.setBufferCapacityOfEntry(8192);
        options.setBufferCapacityOfIndex(8192);
        Node node = new Node(options, new RpcClient());
        Logstore logstore = new LogstoreModule(node);
        
        int currentTerm = 10;
        long lastLogIndex = node.getLastLogIndex();
        
        byte[] body = "Hello world!".getBytes();
        long offset = 0;
        while (offset < 2048) {
            Entry entry = new Entry();
            entry.setTerm(currentTerm);
            entry.setIndex(++lastLogIndex);
            entry.setBody(body);
            logstore.append(entry);
            offset += 36 + body.length; // entry_index_unit=36
            logstore.flush();
        }
        
        Entry entry = logstore.get(0);
        Assert.assertEquals(new String(body), new String(entry.getBody()));
        
        logstore.flush();
        logstore.delete();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
    }
    
    @Test
    public void getEntryTermTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
        
        options.setBufferCapacityOfEntry(8192);
        options.setBufferCapacityOfIndex(8192);
        Node node = new Node(options, new RpcClient());
        Logstore logstore = new LogstoreModule(node);
        
        int currentTerm = 10;
        long lastLogIndex = node.getLastLogIndex();
        
        byte[] body = "Hello world!".getBytes();
        long offset = 0;
        while (offset < 2048) {
            Entry entry = new Entry();
            entry.setTerm(currentTerm);
            entry.setIndex(++lastLogIndex);
            entry.setBody(body);
            logstore.append(entry);
            offset += 36 + body.length; // entry_index_unit=36
            logstore.flush();
        }
        
        Entry entry = logstore.get(0);
        Assert.assertEquals(currentTerm, entry.getTerm());
        
        logstore.flush();
        logstore.delete();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
    }
    
    @Test
    public void truncateTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
        
        options.setBufferCapacityOfEntry(8192);
        options.setBufferCapacityOfIndex(8192);
        Node node = new Node(options, new RpcClient());
        Logstore logstore = new LogstoreModule(node);
        
        int currentTerm = 10;
        long lastLogIndex = node.getLastLogIndex();
        
        byte[] body = "Hello world!".getBytes();
        
        Entry entry;
        
        entry = new Entry();
        entry.setTerm(currentTerm);
        entry.setIndex(lastLogIndex + 1);
        entry.setBody(body);
        logstore.append(entry);
        
        entry = new Entry();
        entry.setTerm(currentTerm);
        entry.setIndex(lastLogIndex + 2);
        entry.setBody(body);
        logstore.append(entry);
        
        entry = logstore.get(lastLogIndex + 2);
        Assert.assertNotNull(entry);
        entry = logstore.get(lastLogIndex + 1);
        Assert.assertNotNull(entry);
        
        logstore.truncate(1);
        
        entry = logstore.get(lastLogIndex + 2);
        Assert.assertNull(entry);
        entry = logstore.get(lastLogIndex + 1);
        Assert.assertNotNull(entry);
        
        logstore.flush();
        logstore.delete();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
    }
    
    @Test
    public void closeTest() throws Exception {
        Options options = new Options();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
        
        options.setBufferCapacityOfEntry(8192);
        options.setBufferCapacityOfIndex(8192);
        options.setFlushSyncDisk(true);
        Node node = new Node(options, new RpcClient());
        Logstore logstore = new LogstoreModule(node);
        
        int currentTerm = 10;
        long lastLogIndex = node.getLastLogIndex();
        
        byte[] body = "Hello world!".getBytes();
        int size = 4 * 1024;
        int written = 0;
        long index = lastLogIndex;
        while (written < size) {        
            Entry entry = new Entry();
            entry.setTerm(currentTerm);
            entry.setIndex(index + 1);
            entry.setBody(body);
            logstore.append(entry);
            written += 36 + body.length; // entry_index_unit=36
            index ++;
        }
        logstore.flush();
        
        logstore.close();
        logstore = new LogstoreModule(node);
        
        logstore.close();
        logstore = new LogstoreModule(node);
        
        while (index > lastLogIndex) {
            Entry entry = logstore.get(index);
            Assert.assertEquals(currentTerm, entry.getTerm());
            Assert.assertEquals(new String(body), new String(entry.getBody()));
            index --;
        }
        
        logstore.flush();
        logstore.delete();
        FileUtil.delete(options.getServerDataPath()); // cleanup history
    }
    
}
