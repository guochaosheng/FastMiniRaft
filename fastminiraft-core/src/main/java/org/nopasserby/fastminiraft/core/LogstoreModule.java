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
import java.nio.ByteBuffer;
import java.util.NavigableMap;

import org.nopasserby.fastminiraft.api.Logstore;
import org.nopasserby.fastminiraft.core.Node.Role;
import org.nopasserby.fastminiraft.api.Entry;
import org.nopasserby.fastminiraft.store.BufferedStore;
import org.nopasserby.fastminiraft.store.SegmentFileStore;
import org.nopasserby.fastminiraft.store.Store;
import org.nopasserby.fastminiraft.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogstoreModule implements Logstore {

    private Logger logger = LoggerFactory.getLogger(LogstoreModule.class);

    private static final int MAGIC_MASK = 0xA1B2;
    
    private static final int ENTRY_INDEX_UNIT = 36;
    
    private static final int ENTRY_INDEX_MAGIC = 0;
    private static final int ENTRY_INDEX_OFFSET = 4; 
    private static final int ENTRY_INDEX_INDEX = 12; 
    private static final int ENTRY_INDEX_TERM = 20;
    private static final int ENTRY_INDEX_BODY_TYPE = 28; 
    private static final int ENTRY_INDEX_BODY_SIZE = 32;
    
    private Node node;
    
    private Store entryStore;
    
    private Store indexStore;
    
    private volatile long lastLogIndex = -1;
    
    private volatile long lastLogTerm = -1; 
    
    private long lastFlushOfIndex;
    
    private Node.RoleChangeListener noopAppend = new Node.RoleChangeListener() {
        @Override
        public void changed(Role oldRole, Role newRole) {
            // Upon becoming leader, append no-op entry to log (§ 6.4)
            if (newRole == Role.LEADER) {
                append(newNoopEntry()); 
                flush();
            }
        }
    };
    
    public Entry newNoopEntry() {
        Entry entry = new Entry();
        long term = node.getCurrentTerm();
        entry.setTerm(term);
        entry.setIndex(lastLogIndex + 1);
        entry.setBodyType(LogstoreProto.NOOP.getCode());
        entry.setBody(LogstoreProto.NOOP.getCodec().encode());
        return entry;
    }

    public LogstoreModule(Node node) {
        this.node = node;
        this.node.addRoleChangeListener(noopAppend);
        this.init();
    }
    
    public void init() {
        try {
            Options options = node.getOptions();
            String entryStorePath = options.getServerDataPath() + "/data.entry";
            SegmentFileStore entryStore = new SegmentFileStore(entryStorePath, 1024 * 1024 * 1024);
            
            String indexStorePath = options.getServerDataPath() + "/data.index";
            SegmentFileStore indexStore = new SegmentFileStore(indexStorePath, 1024 * 1024 * 1024);
            
            this.reload(entryStore, indexStore);
            
            long bufferCapacityOfEntry = options.getBufferCapacityOfEntry();
            long bufferCapacityOfIndex = options.getBufferCapacityOfIndex();
            int bufferThreshold = options.getBufferThreshold();
            boolean flushSyncDisk = options.getFlushSyncDisk();
            
            this.entryStore = new BufferedStore(entryStore, bufferCapacityOfEntry, bufferThreshold, flushSyncDisk);
            this.indexStore = new BufferedStore(indexStore, bufferCapacityOfIndex, bufferThreshold, flushSyncDisk);
        } catch (IOException e) {
            throw new IllegalStateException("log store module: init excetpion", e);
        }
    }
    
    public void reload(SegmentFileStore entryStore, SegmentFileStore indexStore) {
        long offset = 0, index = 0;
        
        NavigableMap<Long, Store> segments = entryStore.getSegments();
        Long lastOffset = segments.lastKey();
        if (lastOffset != null) {
            long safeOffset = lastOffset - node.getOptions().getRestoreSafeDistance();
            long minOffset = segments.firstKey();
            offset = segments.floorKey(Math.max(safeOffset, minOffset));
        }
        logger.debug("log store module reload checkpoint offset: {}", offset);
        long start = DateUtil.now();
        try {
            lastLogIndex = lastLogTerm = -1; // reset
            boolean truncated = false;
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_INDEX_UNIT);
            while (true) {
                ByteBuffer headerIndex = buffer.duplicate();
                
                if (offset >= entryStore.position() || entryStore.read(headerIndex, offset) != ENTRY_INDEX_UNIT) break;
                
                int magic = headerIndex.getInt(ENTRY_INDEX_MAGIC);
                long entryOffset = headerIndex.getLong(ENTRY_INDEX_OFFSET);
                long entryIndex = headerIndex.getLong(ENTRY_INDEX_INDEX);
                long entryTerm = headerIndex.getLong(ENTRY_INDEX_TERM);
                int bodyType = headerIndex.getInt(ENTRY_INDEX_BODY_TYPE);
                int bodySize = headerIndex.getInt(ENTRY_INDEX_BODY_SIZE);
                // TODO Entry CRC 
                
                if (magic != MAGIC_MASK || (index > 0 && entryIndex != index) || offset != entryOffset) break;
                
                if (!truncated) {
                    ByteBuffer dst = buffer.duplicate();
                    int read = indexStore.read(dst, entryIndex * ENTRY_INDEX_UNIT);
                    
                    if (read != ENTRY_INDEX_UNIT
                            || magic != dst.getInt(ENTRY_INDEX_MAGIC)
                            || entryOffset != dst.getLong(ENTRY_INDEX_OFFSET)
                            || entryIndex != dst.getLong(ENTRY_INDEX_INDEX)
                            || entryTerm != dst.getLong(ENTRY_INDEX_TERM)
                            || bodyType != dst.getInt(ENTRY_INDEX_BODY_TYPE)
                            || bodySize != dst.getInt(ENTRY_INDEX_BODY_SIZE)) {                    
                        indexStore.truncate(entryIndex * ENTRY_INDEX_UNIT);
                        truncated = true;
                    }
                }
                
                if (truncated) {
                    headerIndex.flip();
                    indexStore.write(headerIndex);
                }
                
                index = entryIndex + 1;
                offset += ENTRY_INDEX_UNIT + bodySize;
                
                lastLogTerm = entryTerm;
                lastLogIndex = entryIndex;
            }
        } catch (IOException e) {
            logger.error("", e);
        }
        try {
            indexStore.truncate(index * ENTRY_INDEX_UNIT);
            entryStore.truncate(offset);
            
            logger.info("log store module truncate index: {}, offset: {}", index, offset);
            
            node.updateLastLogIndex(lastLogTerm, lastLogIndex);
        } catch (IOException e) {
            throw new IllegalStateException("log store module: reload excetpion", e);
        }
        long end = DateUtil.now();
        logger.info("log store module restore use time: {} ms", end - start);
    }

    @Override
    public long getEntryTerm(long entryIndex) {
        try {
            if (entryIndex < 0 || entryIndex > lastLogIndex) {
                return -1; // available term > -1, need to throw an exception ? 
            }
            
            if (entryIndex == lastLogIndex) {
                return lastLogTerm;
            }
            
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_INDEX_UNIT);
            indexStore.read(buffer, entryIndex * ENTRY_INDEX_UNIT);
            
            return buffer.remaining() == 0 ? buffer.getLong(ENTRY_INDEX_TERM) : -1;
        } catch (IOException e) {
            throw new IllegalStateException("log store module: get entry term excetpion", e);
        }
    }

    @Override
    public void append(Entry entry) {
        long start = DateUtil.now();
        try {
            byte[] body = entry.getBody();
            ByteBuffer encodedEntry = ByteBuffer.allocate(ENTRY_INDEX_UNIT + body.length);
            encodedEntry.putInt(MAGIC_MASK);
            encodedEntry.putLong(entryStore.position());
            encodedEntry.putLong(entry.getIndex());
            encodedEntry.putLong(entry.getTerm());
            encodedEntry.putInt(entry.getBodyType());
            encodedEntry.putInt(body.length);
            
            ByteBuffer encodedEntryIndex = encodedEntry.asReadOnlyBuffer();
            
            encodedEntry.put(body);
            encodedEntry.flip();
            entryStore.write(encodedEntry);
            encodedEntryIndex.flip();
            indexStore.write(encodedEntryIndex);
            
            lastLogIndex = entry.getIndex();
            lastLogTerm = entry.getTerm();
        } catch (IOException e) {
            throw new IllegalStateException("log store module: append entry excetpion", e);
        }
        long end = DateUtil.now();
        long usetime = end - start;
        if (usetime > 50) {
            logger.debug("log store append use time: {} ms", usetime);        
        }
    }
    
    @Override
    public Entry get(long entryIndex) {
        try {
            if (entryIndex < 0 || entryIndex > lastLogIndex) {
                return null;
            }
            
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_INDEX_UNIT);
            if (indexStore.read(buffer, entryIndex * ENTRY_INDEX_UNIT) == 0) {
                return null;
            }
            
            long entryOffset = buffer.getLong(ENTRY_INDEX_OFFSET);
            long entryTerm = buffer.getLong(ENTRY_INDEX_TERM);
            int bodyType = buffer.getInt(ENTRY_INDEX_BODY_TYPE);
            int bodySize = buffer.getInt(ENTRY_INDEX_BODY_SIZE);
            
            ByteBuffer bodyWrap = ByteBuffer.allocate(bodySize);
            if (entryStore.read(bodyWrap, entryOffset + ENTRY_INDEX_UNIT) != bodySize) {
                throw new ArrayStoreException("entry index:" + entryIndex);
            }
            
            Entry entry = new Entry();
            entry.setIndex(entryIndex);
            entry.setTerm(entryTerm);
            entry.setBodyType(bodyType);
            entry.setBody(bodyWrap.array());
            
            return entry;
        } catch (Exception e) {
            throw new IllegalStateException("log store module: get entry excetpion", e);
        }
    }

    @Override
    public void truncate(long entryIndex) {
        try {
            if (entryIndex < 0 || entryIndex > lastLogIndex) {
                return;
            }
            
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_INDEX_UNIT);
            indexStore.read(buffer, entryIndex * ENTRY_INDEX_UNIT);
            
            long offset = buffer.getLong(ENTRY_INDEX_OFFSET);
            long index = buffer.getLong(ENTRY_INDEX_INDEX);
            
            if (index != entryIndex) {
                throw new IllegalStateException("log store module: entry[index:" + entryIndex + "] error");
            }
            
            this.entryStore.truncate(offset);
            this.indexStore.truncate(entryIndex * ENTRY_INDEX_UNIT);
            
            
            this.lastLogTerm = getEntryTerm(entryIndex - 1);
            this.lastLogIndex = entryIndex - 1;
        } catch (IOException e) {
            throw new IllegalStateException("log store module: truncate excetpion", e);
        }
    }

    @Override
    public Entry getLastLog() {
        return get(lastLogIndex);
    }

    @Override
    public long getLastLogIndex() {
        return lastLogIndex;
    }
    
    @Override
    public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public void flush() {
        long start = DateUtil.now();
        try {
            entryStore.flush();
            
            long now = DateUtil.now();
            if (now - lastFlushOfIndex > 100) {
                indexStore.flush();     
                lastFlushOfIndex = now;
            }
            
            node.updateLastLogIndex(lastLogTerm, lastLogIndex);
        } catch (IOException e) {
            throw new IllegalStateException("log store module: flush excetpion", e);
        }
        long end = DateUtil.now();
        long usetime = end - start;
        if (usetime > 50) {
            logger.debug("log store flush use time: {} ms", usetime);            
        }
    }

    @Override
    public void close() {
        try {
            this.entryStore.close();
            this.indexStore.close();
        } catch (IOException e) {
            throw new IllegalStateException("log store module: close excetpion", e);
        }
    }

    @Override
    public void delete() {
        try {
            this.entryStore.delete();
            this.indexStore.delete();
        } catch (IOException e) {
            throw new IllegalStateException("log store module: delete excetpion", e);
        }
    }

}
