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

package org.nopasserby.fastminiraft.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateUtil {
    
    private final static String YYYYMMDD_HHMMSS_SSS = "yyyy-MM-dd HH:mm:ss:SSS";
    
    private static DateTimeFormatter datetimeFormatter = DateTimeFormatter.ofPattern(YYYYMMDD_HHMMSS_SSS);
   
    public static String formatToLongDate(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()).format(datetimeFormatter);
    }

    public static String nowOfLongDate() {
        return LocalDateTime.now().format(datetimeFormatter);
    }
    
    public static long formatToMilli(String datetime) {
        return LocalDateTime.parse(datetime, datetimeFormatter).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
    
}
