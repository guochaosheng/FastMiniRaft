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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class MmapFileStore implements Store {

    private RandomAccessFile raf;
    
    private FileChannel fc;
    
    private File file;
    
    private long position;
    
    private MappedByteBuffer mmap;
    
    public MmapFileStore(String path, int size) throws IOException {
        this.file = newFile(path);
        this.raf = new RandomAccessFile(newFile(path), "rw");
        this.fc = raf.getChannel();
        this.position = raf.length();
        this.mmap = fc.map(MapMode.READ_WRITE, 0, size);
        this.mmap.position((int) position);
    }
    
    private File newFile(String path) throws IOException {
        File file = new File(path);
        File parentFile = file.getParentFile();
        if (!parentFile.exists()) parentFile.mkdirs();
        file.createNewFile();
        return file;
    }

    @Override
    public void close() throws IOException {
        ((sun.nio.ch.DirectBuffer) mmap).cleaner().clean(); // sun JDK
        fc.close();
        raf.close();
    }
    
    @Override
    public long position() {
        return position;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        mmap.put(src);
        int written = remaining - src.remaining();
        position += written;
        return written;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int remaining = dst.remaining();
        ByteBuffer src = mmap.asReadOnlyBuffer();
        src.position((int) position);
        src.limit((int) (position + remaining));
        dst.put(src);
        return remaining - dst.remaining();
    }
    
    @Override
    public void flush() throws IOException {
        mmap.force();
    }

    @Override
    public void truncate(long size) throws IOException {
        mmap.position((int) size);
        position = size;
    }

    @Override
    public void delete() throws IOException {
        this.close();
        this.file.delete();
    }
    
}
