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

package org.nopasserby.fastminirpc.util;

import java.nio.charset.Charset;

public class ByteUtil {

    private final static Charset DEFAULT_CHARSET = Charset.forName("utf-8");
    
    public static byte[] toByteArray(int value) {
        byte[] b = new byte[4];
        b[0] = (byte) (value >>> 24);
        b[1] = (byte) (value >>> 16);
        b[2] = (byte) (value >>> 8);
        b[3] = (byte) value;
        return b;
    }
    
    public static int toInt(byte[] b) {
        return ((b[0])         << 24) | 
                ((b[1] & 0xff) << 16) | 
                ((b[2] & 0xff) <<  8) | 
                ((b[3] & 0xff));
    }
    
    public static byte[] toByteArray(long value) {
        byte[] b = new byte[8];
        b[0] = (byte) (value >>> 56);
        b[1] = (byte) (value >>> 48);
        b[2] = (byte) (value >>> 40);
        b[3] = (byte) (value >>> 32);
        b[4] = (byte) (value >>> 24);
        b[5] = (byte) (value >>> 16);
        b[6] = (byte) (value >>> 8);
        b[7] = (byte) value;
        return b;
    }
    
    public static long toLong(byte[] b) {
        return (((long) b[0] & 0xff) << 56) |
                (((long) b[1] & 0xff) << 48) |
                (((long) b[2] & 0xff) << 40) |
                (((long) b[3] & 0xff) << 32) |
                (((long) b[4] & 0xff) << 24) |
                (((long) b[5] & 0xff) << 16) |
                (((long) b[6] & 0xff) <<  8) |
                (((long) b[7] & 0xff));
    }

    public static String toString(byte[] b) {
        return new String(b, DEFAULT_CHARSET);
    }
    
    public static byte[] toByteArray(String b) {
        return b.getBytes(DEFAULT_CHARSET);
    }
    
}
