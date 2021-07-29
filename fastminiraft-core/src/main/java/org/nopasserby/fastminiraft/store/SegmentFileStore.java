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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class SegmentFileStore implements Store {

    private NavigableMap<Long, Store> segments = new ConcurrentSkipListMap<Long, Store>();
    
    private Queue<Store> nonFlushedstores = new LinkedList<Store>();
    
    private long position;
    
    private Store segment;
    
    private long segmentSize;
    
    private File dataDir;
    
    public SegmentFileStore(String path, long segmentSize) throws IOException {
        this.segmentSize = segmentSize;
        dataDir = new File(path);
        dataDir.mkdirs();
        
        for (String filename: dataDir.list()) {
            appendSegment(Long.parseLong(filename));
        }
        if (!segments.isEmpty()) {
            segment = segments.get(segments.lastKey());
            position = segments.lastKey() + segment.position();
        }
        if (!segments.containsKey(position)) {
            appendSegment(position);
        }
    }

    private void appendSegment(long position) throws IOException {
        segment = createSegmentStore(dataDir.getPath() + File.separator + String.format("%020d", position));
        segments.put(position, segment);
    }
    
    public Store createSegmentStore(String path) throws IOException {
        return new SimpleFileStore(path);
    }
    
    public long getSegmentSize() {
        return segmentSize;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written = src.remaining();
        if (written > segmentSize) {
            // MMAP file store error, when position equals 0
            throw new IllegalArgumentException(
                    "segment size " + segmentSize + ". write buffer must be smaller than or equal to the segment size");
        }
        boolean needExpanded = segment == null || (written + segment.position()) > segmentSize;
        
        if (needExpanded) {
            this.nonFlushedstores.add(segment);
            this.appendSegment(position);
        }
        
        written = segment.write(src);
        position += written;
        return written;
    }
    
    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        Entry<Long, Store> segmentPair = segments.floorEntry(position);
        if (segmentPair == null) {
            return -1;
        }
        
        Long baseOffset = segmentPair.getKey();
        Store segment = segmentPair.getValue();
        
        int length = segment.read(dst, position - baseOffset);
        position += length;
        
        if (dst.hasRemaining() && length > 0 && position < this.position) {
            length += read(dst, position);
        }
        return length;
    }

    @Override
    public void close() throws IOException {
        for (Store segment: segments.values()) segment.close();
    }

    @Override
    public void flush() throws IOException {
        if (segment != null) {
            segment.flush();
            Store store;
            while ((store = nonFlushedstores.poll()) != null) store.flush();
        }
    }

    @Override
    public void truncate(long size) throws IOException {
        if (position == size) {
            return;
        }
        
        Long baseOffset = segments.lowerKey(size);
        
        if (baseOffset != null) {
            segment = segments.get(baseOffset);
            segment.truncate(size - baseOffset);
            position = size;
        } else {
            segment = null;
            position = 0;
        }
        
        Long higherKey;
        while ((higherKey = segments.ceilingKey(size)) != null) {
            segments.remove(higherKey).delete();
        }
    }

    @Override
    public void delete() throws IOException {
        truncate(0);
        dataDir.delete();
    }
    
    public NavigableMap<Long, Store> getSegments() {
        return segments;
    }
    
}
