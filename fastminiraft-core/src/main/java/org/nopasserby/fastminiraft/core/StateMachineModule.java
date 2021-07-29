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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.nopasserby.fastminiraft.api.StateMachine;
import org.nopasserby.fastminiraft.api.Entry;
import org.nopasserby.fastminirpc.util.ByteUtil;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class StateMachineModule implements StateMachine {
    
    private Node node;
    
    private DBStateMathine db;
    
    public StateMachineModule(Node node) {
        this.node = node;
        this.db = new DBStateMathine(node.getOptions().getServerDataPath());
        this.db.startup();
    }
    
    @Override
    public long getLastApplied() {
        return db.getLastAppliedIndex();
    }

    @Override
    public synchronized void apply(Entry entry, BiConsumer<Object, Throwable> action) {
        long entryIndex = entry.getIndex();
        long lastApplied = getLastApplied();
        if (lastApplied > entryIndex) {
            return;
        }
        if (entryIndex != lastApplied + 1) {
            throw new IllegalStateException("last applied index: " + lastApplied + " and applying index: " + entryIndex);
        }
        
        Object result = null;
        Throwable throwable = null;
        try {            
            result = apply(entry);
        } catch (Exception exception) {
            throwable = exception;
        }
        if (action != null) {
            /**
             *Leaders:
             *    • If command received from client: append entry to local log,
             *    respond after entry applied to state machine (§5.3)
             */
            action.accept(result, throwable);
        }
        
        db.advanceAppliedIndex(entry.getIndex());
    }
    
    public Object apply(Entry entry) {
        LogstoreProto logstoreProto = LogstoreProto.valueOf(entry.getBodyType());
        Object[] args = logstoreProto.getCodec().decode(entry.getBody());
        Object result = null;
        
        if (LogstoreProto.ADD == logstoreProto) {
            result = entry.getIndex();
        }
        else if (LogstoreProto.SET == logstoreProto) {
            db.put((byte[]) args[0], (byte[]) args[1]);
        }
        else if (LogstoreProto.CAS == logstoreProto) {
            result = db.cas((byte[]) args[0], (byte[]) args[1], (byte[]) args[2]);
        }
        else if (LogstoreProto.DEL == logstoreProto) {
            db.del((byte[]) args[0]);
        }
        else if (LogstoreProto.ADD_SER == logstoreProto) {
            node.addServer((String) args[0], (String) args[1]);
            db.put("inner.key.name.cluster".getBytes(), node.getServerCluster().getBytes());
        } 
        else if (LogstoreProto.DEL_SER == logstoreProto) {
            node.deleteServer((String) args[0]);
            db.put("inner.key.name.cluster".getBytes(), node.getServerCluster().getBytes());
        }
        return result;
    }
    
    public DB getDB() {
        return db;
    }
    
    public void shutdown() {
        db.shutdown();
    }

    public String getServerCluster() {
        byte[] serverCluster = db.get("inner.key.name.cluster".getBytes());
        return serverCluster == null ? null : new String(serverCluster);
    }
    
}

interface DB {
    
    public void put(byte[] key, byte[] value);
    
    public boolean cas(byte[] key, byte[] expect, byte[] value);
    
    public void del(byte[] key);
    
    public byte[] get(byte[] key);
    
}

class DBStateMathine extends LoopExecutor implements DB {
    
    private int hotspotBFKey = 10 * 1024;
    
    private int noCheckPointLimit = 10 * 1024;
    
    private long checkPointPeriod = 3 * 1000;
    
    private ByteBuffer deletedFlag;
    
    private long lastAppliedCheckPoint = -1;
    
    private volatile long lastApplied;
    
    private RocksDB db;
    
    private Map<ByteBuffer, ByteBuffer> hotspotBFTable = new ConcurrentHashMap<ByteBuffer, ByteBuffer>(); // 3273531 op/sec
    
    private BlockingQueue<DumpPoint> queue = new ArrayBlockingQueue<DumpPoint>(1024);
    
    public DBStateMathine(String dataPath) {
        db = createDB(dataPath + "/data.map");
        deletedFlag = ByteBuffer.wrap("inner.key.name.deleted".getBytes());
        byte[] checkPoint = get("inner.key.name.checkpoint".getBytes());
        lastAppliedCheckPoint = checkPoint == null ? -1 : ByteUtil.toLong(checkPoint);
        lastApplied = lastAppliedCheckPoint;
    }
    
    public RocksDB createDB(String dataPath) {
        File dir = new File(dataPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            db = RocksDB.open(options, dataPath);
        } catch (RocksDBException e) {
            throw new IllegalStateException("db open error", e);
        }
        return db;
    }
    
    @Override
    public void put(byte[] key, byte[] value) {
        hotspotBFDump();
        hotspotBFTable.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }
    
    @Override
    public boolean cas(byte[] key, byte[] expect, byte[] update) {
        hotspotBFDump();
        byte[] value = get(key);
        if (value != null && Arrays.equals(value, expect)) {
            put(key, update);
            return true;
        }
        return false;
    }
    
    @Override
    public void del(byte[] key) {
        hotspotBFDump();
        hotspotBFTable.put(ByteBuffer.wrap(key), deletedFlag);
    }
    
    @Override
    public byte[] get(byte[] key) {
        ByteBuffer wrapper = hotspotBFTable.get(ByteBuffer.wrap(key));
        
        if (wrapper == null) {
            try {
                return db.get(key);
            } catch (RocksDBException e) {
                throw new IllegalStateException("db get error", e);
            }
        }
        
        if (wrapper != null && wrapper == deletedFlag) {
            return null;
        }
        
        byte[] value = new byte[wrapper.remaining()];
        wrapper.duplicate().get(value);
        return value;
    }
    
    public void hotspotBFDump() {
        if (hotspotBFTable.size() >= hotspotBFKey) {
            dump();
        }
    }
    
    public synchronized void dump() {
        DumpPoint dumpPoint = new DumpPoint();
        dumpPoint.hotspotBFTable = hotspotBFTable;
        dumpPoint.lastApplied = lastApplied;
        if (queue.offer(dumpPoint)) {
            hotspotBFTable = new HashMap<ByteBuffer, ByteBuffer>();
        }
    }
    
    @Override
    public void execute() throws Exception {
        DumpPoint dumpPoint = queue.poll(checkPointPeriod, TimeUnit.MILLISECONDS);
        if (dumpPoint != null) {
            dumpToDB(dumpPoint);
            byte[] value = ByteUtil.toByteArray(dumpPoint.lastApplied);
            db.put("inner.key.name.checkpoint".getBytes(), value);
            lastAppliedCheckPoint = dumpPoint.lastApplied;
        }
        
        if (queue.isEmpty() && lastApplied - lastAppliedCheckPoint > noCheckPointLimit) {
            dump();
        }
    }
    
    public void dumpToDB(DumpPoint dumpPoint) throws Exception {
        for (Map.Entry<ByteBuffer, ByteBuffer> entry: dumpPoint.hotspotBFTable.entrySet()) {
            ByteBuffer key = entry.getKey();
            ByteBuffer value = entry.getValue();
            if (value == deletedFlag) {
                db.delete(key.array(), key.position(), key.remaining());
            } else {
                db.put(key.array(), key.position(), key.remaining(), value.array(), value.position(), value.remaining());
            }
        }
    }
    
    public void advanceAppliedIndex(long applyIndex) {
        this.lastApplied = applyIndex;
    }
    
    public long getLastAppliedIndex() {
        return lastApplied;
    }
    
    @Override
    public void shutdown() {
        super.shutdown();
        db.close();
    }
    
    public static class DumpPoint {
        Map<ByteBuffer, ByteBuffer> hotspotBFTable;
        long lastApplied;
    }
    
}
