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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class CollectionUtil {

    public static long min(Collection<Long> collection) {
        Iterator<Long> iterator = collection.iterator();
        long min = iterator.next();
        while (iterator.hasNext()) {
            min = Math.min(min, iterator.next());
        }
        return min;
    }

    public static long median(Collection<Long> collection) {
        List<Long> list = List.class.isInstance(collection) ?  (List<Long>) collection : new ArrayList<Long>(collection);
        Collections.sort(list, Comparator.reverseOrder());
        return list.get(list.size() / 2);
    }
    
}
