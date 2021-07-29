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
import java.util.concurrent.locks.LockSupport;

public class BufferedStore implements Store {

    private long threshold;
    
    private boolean flushSync;
    
    private RingBuffer buffer;
    
    private long bufferedPosition;
    
    private long storePosition;
    
    private Store store;
    
    public BufferedStore(Store store, long bufferCapacity, int bufferThreshold, boolean flushSync) {
        if (Long.bitCount(bufferCapacity) != 1) 
            throw new IllegalArgumentException("buffer capacity must be powers of 2");
        if (bufferThreshold > bufferCapacity) 
            throw new IllegalArgumentException("buffer threshold should not be greater than buffer capacity");
        
        int bufferUnit = (int) Math.min(128 * 1024 * 1024, bufferCapacity);
        int bufferCount = (int) bufferCapacity / bufferUnit;
        
        this.buffer = new RingBuffer(bufferUnit, bufferCount);
        this.threshold = bufferThreshold;
        this.store = store;
        this.bufferedPosition = store.position();
        this.storePosition = store.position();
        this.flushSync = flushSync;
    }
    
    @Override
    public long position() {
        return bufferedPosition + buffer.position();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        if (len > buffer.getCapacity()) {
            throw new IllegalArgumentException("write buffer too long:" + len + " bytes");
        }
        this.spillWithThreshold(threshold - len);
        return buffer.write(src);
    }
    
    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        long relativePosition = position - bufferedPosition;
        if (relativePosition < 0) {
            return store.read(dst, position);
        }
        
        int read = buffer.read(dst, relativePosition);
        return read >= 0 ? read : store.read(dst, position);
    }

    @Override
    public void close() throws IOException {
        this.spillWithThreshold(0);
        this.buffer.release();
        this.store.close();
    }

    @Override
    public void flush() throws IOException {
        this.spillWithThreshold(0);
        if (flushSync) {
            this.store.flush();
        }
    }
    
    void spillWithThreshold(long threshold) throws IOException {
        long writtenPosition = storePosition - bufferedPosition;
        if (buffer.position() - writtenPosition > threshold) {
            storePosition += store.write(buffer.sliceAsReadOnly(writtenPosition));
        }
    }

    @Override
    public void truncate(long size) throws IOException {
        store.truncate(size);
        boolean truncated = buffer.truncate(size - bufferedPosition);
        if (!truncated) {
            buffer.reset();
            bufferedPosition = store.position();
        }
        storePosition = store.position();
    }

    @Override
    public void delete() throws IOException {
        this.spillWithThreshold(0);
        this.buffer.release();
        this.store.delete();
    }

}

/**
 * 1. Both write and read allow concurrent operations
 * 2. Both sliceasreadonly and read allow concurrent operations
 * 3. Both sliceasreadonly and write do not allow concurrent operations
 * 4. Write or read operations are not allowed during truncate, reset and release operations
 * 
 * */
class RingBuffer {

    private byte[][] buffers;
    
    private int unit;
    
    private int count;
    
    private long capacity;
        
    /**
     * Index number of the next position to be read (index number starts from 0)
     * */
    private volatile long readPos;
    
    /**
     * The index number of the next position to be written (the index number starts from 0), 
     * and the variable value is equal to the number of bytes written
     * */
    private volatile long writePos;
    
    private volatile long minReadablePos;
    
    private volatile long maxReadyWritePos;
    
    private volatile long cachedValue;
    
    public RingBuffer(int unit, int count) {
        if (Long.bitCount(unit) != 1 || Long.bitCount(count) != 1) 
            throw new IllegalArgumentException("unit and count must be powers of 2");
        this.unit = unit;
        this.count = count;
        this.capacity = (long) unit * (long) count;
        
        buffers = new byte[count][];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = new byte[unit];
        }
    }
    
    public int writeWithLock(ByteBuffer src) {
        int remaining = src.remaining();
        
        long currentPos = writePos;
        long nextPos = currentPos + remaining;
        
        long wrapPoint = nextPos - capacity;
        long cachedValue = this.cachedValue;

        if (wrapPoint > cachedValue || cachedValue > currentPos) {
            while (wrapPoint > (cachedValue = readPos)) {
                LockSupport.parkNanos(1L);
            }

            this.cachedValue = cachedValue;
        }
        return write(src);
    }
    
    public void unLock(long position) {
        readPos = position;
    }
    
    public int write(ByteBuffer src) {
        int remaining = src.remaining();
        int length = remaining;
        
        maxReadyWritePos += length;
        
        long currentPos = writePos;
        int bufferIndex = bufferIndex(currentPos);
        int bufferPos = bufferPos(currentPos);
        
        while (remaining > 0) {
            byte[] dest = buffers[bufferIndex];
            int localLength = Math.min(remaining, dest.length - bufferPos);
            src.get(dest, bufferPos, localLength);
            bufferPos += localLength;
            remaining -= localLength;
            if (bufferPos == dest.length) {
                bufferIndex = (bufferIndex + 1) & (buffers.length - 1);// next buffer index
                bufferPos = 0;
            }
        }
        
        writePos += length;
        return length;
    }
    
    int bufferIndex(long position) {
        return (int) ((position / unit) & (count - 1));
    }
    
    int bufferPos(long position) {
        return (int) (position & (unit - 1));
    }

    public long position() {
        return writePos;
    }

    public int read(ByteBuffer dst, long position) {
        long writePos = this.writePos;
        if (position >= writePos || position < minReadablePos) {
            return -1;
        }
        
        int backup = dst.position();
        
        int bufferIndex = bufferIndex(position);
        int bufferPos = bufferPos(position);
        long readPos = position;
        
        int remaining = dst.remaining();
        int length = remaining;
        while (remaining > 0 && readPos < writePos) {
            byte[] src = buffers[bufferIndex];
            int localLength = Math.min(remaining, src.length - bufferPos);
            dst.put(src, bufferPos, localLength);
            bufferPos += localLength;
            readPos += localLength;
            remaining -= localLength;
            if (bufferPos == src.length) {
                bufferIndex = (bufferIndex + 1) & (buffers.length - 1);// next buffer index
                bufferPos = 0;
            }
        }
        // check available
        long minReadablePos = Math.max(this.minReadablePos, maxReadyWritePos - capacity);
        if (minReadablePos > position) {
            dst.position(backup);
            return -1;
        }
        this.minReadablePos = minReadablePos;
        return length - remaining;
    }
    
    public ByteBuffer sliceAsReadOnly(long startPos) {
        long endPos = writePos;
        long minReadablePos = Math.max(this.minReadablePos, maxReadyWritePos - capacity);
        if (startPos >= endPos || startPos < minReadablePos) {
            throw new IndexOutOfBoundsException(Long.toString(startPos));
        }
        
        int length = (int) (endPos - startPos);
        int bufferIndex = bufferIndex(startPos);
        int bufferPos = bufferPos(startPos);
        
        if (bufferIndex == bufferIndex(endPos) && bufferPos < bufferPos(endPos)) {
            ByteBuffer dst = ByteBuffer.wrap(buffers[bufferIndex]);
            dst.position(bufferPos).limit(bufferPos + length);
            return dst.asReadOnlyBuffer();
        }
        
        ByteBuffer dst = ByteBuffer.allocate(length);
        read(dst, startPos);
        dst.flip();
        return dst.asReadOnlyBuffer();
    }

    public boolean truncate(long position) {
        long minReadablePos = Math.max(this.minReadablePos, maxReadyWritePos - capacity);
        if (position < minReadablePos) {
            return false;
        }
        maxReadyWritePos = position;
        writePos = position;
        return true;
    }
    
    public void release() {
        this.reset();
        this.buffers = null;
    }
    
    public void reset() {
        writePos = 0;
        readPos = 0;
        minReadablePos = 0;
        maxReadyWritePos = 0;
        cachedValue = 0;
    }
    
    public long getCapacity() {
        return capacity;
    }
    
}

