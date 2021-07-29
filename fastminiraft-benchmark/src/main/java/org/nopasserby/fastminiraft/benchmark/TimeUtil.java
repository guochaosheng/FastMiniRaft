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

package org.nopasserby.fastminiraft.benchmark;

public class TimeUtil {

    /**
     * @see https://github.com/twitter/snowflake
     * */
    public static synchronized long uniqueTimestamp() {
        long timestamp = System.currentTimeMillis();
        long timestampLeftShift = 22L;
        long twepoch = 1288834974657L;
        long sequenceMask = -1L ^ (-1L << timestampLeftShift);

        if (timestamp < ID_OBJECT.lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds",
                    ID_OBJECT.lastTimestamp - timestamp));
        }

        if (ID_OBJECT.lastTimestamp == timestamp) {
            ID_OBJECT.sequence = (ID_OBJECT.sequence + 1) & sequenceMask;
            if (ID_OBJECT.sequence == 0) {
                while ((timestamp = System.currentTimeMillis()) <= ID_OBJECT.lastTimestamp) {
                }
            }
        } else {
            ID_OBJECT.sequence = 0;
        }

        ID_OBJECT.lastTimestamp = timestamp;
        
        return ((timestamp - twepoch) << timestampLeftShift) | ID_OBJECT.sequence;
    }
    
    private final static IdObject ID_OBJECT = new IdObject();
    
    private static class IdObject {
        long sequence;
        long lastTimestamp;
    }
    
}
