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
import java.nio.channels.FileChannel;

public class SimpleFileStore implements Store {

    private RandomAccessFile raf;
    
    private FileChannel fc;
    
    private File file;
    
    private long position;
    
    public SimpleFileStore(String path) throws IOException {
        this.file = newFile(path);
        this.raf = new RandomAccessFile(newFile(path), "rw");
        this.fc = raf.getChannel();
        this.position = raf.length();
        this.fc.position(position);
    }
    
    private File newFile(String path) throws IOException {
        File file = new File(path);
        File parentFile = file.getParentFile();
        if (parentFile != null && !parentFile.exists()) {
            parentFile.mkdirs();
        }
        file.createNewFile();
        return file;
    }

    @Override
    public void close() throws IOException {
        fc.close();
        raf.close();
    }
    
    @Override
    public long position() {
        return position;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written = fc.write(src);
        position += written;
        return written;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return fc.read(dst, position);
    }
    
    @Override
    public void flush() throws IOException {
        fc.force(true);
    }

    @Override
    public void truncate(long size) throws IOException {
        fc.truncate(size);
        position = fc.position();
    }

    @Override
    public void delete() throws IOException {
        this.close();
        this.file.delete();
    }

}
