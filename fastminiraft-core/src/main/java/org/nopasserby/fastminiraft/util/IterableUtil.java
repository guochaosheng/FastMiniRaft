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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class IterableUtil {

    public static <T> Iterable<T> newReadOnlyIterable(Collection<T> collection) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return collection.iterator();
            }
        };
    }
    
    public static <T> List<T> newList(Iterable<T> iterable) {
        List<T> list = new ArrayList<T>();
        for (T t: iterable) {
            list.add(t);
        }
        return list;
    }
    
}
