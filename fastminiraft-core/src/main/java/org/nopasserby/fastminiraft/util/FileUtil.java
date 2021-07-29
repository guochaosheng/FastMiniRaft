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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class FileUtil {
    
    public static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");
    
    public static void writeStringToFile(final String str, final String filepath) throws IOException {
        String tmpFile = filepath + ".tmp";
        writeStringToFileNotSafe(str, tmpFile);

        String bakFile = filepath + ".bak";
        String oldstr = readStringFromFile(filepath);
        if (oldstr != null) {
            writeStringToFileNotSafe(oldstr, bakFile); // backup, prevent write interrupt and other file damage
        }

        File file = new File(filepath);
        file.delete();

        file = new File(tmpFile);
        file.renameTo(new File(filepath));
    }
    
    public static void writeStringToFileNotSafe(final String str, final String filepath) throws IOException {
        File file = new File(filepath);
        File parentFile = file.getParentFile();
        if (parentFile != null && !parentFile.exists()) {
            parentFile.mkdirs();
        }
        try (FileWriter fw = new FileWriter(file);) {
            fw.write(str);
        }
    }
    
    public static String readStringFromFile(final String filepath) throws IOException {
        return readStringFromFile(new File(filepath));
    }

    public static String readStringFromFile(final File file) throws IOException {
        if (file.exists()) {
            byte[] data = new byte[(int) file.length()];
            int len;
            try (FileInputStream is = new FileInputStream(file);) {
                len = is.read(data);
            }
            if (len == data.length) {
                return new String(data);
            }
        }
        return null;
    }
    
    public static void writeMapToFile(Map<String, String> map, String filepath) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key != null && value != null) {
                sb.append(key + "=" + value).append(System.lineSeparator());
            }
        }
        writeStringToFile(sb.toString(), filepath);
    }

    public static Map<String, String> readMapFromFile(String filepath) throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        
        String str = readStringFromFile(filepath);
        if (str == null) {
            return map;
        }
        for (String entry : str.split(System.lineSeparator())) {
            map.put(entry.split("=")[0], entry.split("=")[1]);
        }
        return map;
    }
    
    public static void delete(String filepath) {
        File file = new File(filepath);
        if (file.isFile()) {
            file.delete();
            return;
        }
        
        File[] files = file.listFiles();
        if (files == null) {
            file.delete();
            return;
        } 
            
        for (int i = 0; i < files.length; i++) {
            delete(files[i].getAbsolutePath());
        }
        file.delete();
    }

}
