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

public class AssertUtil {

    public static void assertTrue(boolean condition, RuntimeException ex) {
        if (!condition) {
            throw ex;
        }
    }
    
    public static void assertNull(Object object, RuntimeException ex) {
        assertTrue(object == null, ex);
    }
    
    public static void assertNotNull(Object object, RuntimeException ex) {
        assertTrue(object != null, ex);
    }
    
}
