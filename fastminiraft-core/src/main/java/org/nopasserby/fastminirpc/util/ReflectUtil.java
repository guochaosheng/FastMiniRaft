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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReflectUtil {
    
    /**
     * If the short mode is false, the method signature consists of the full name of the class, 
     * the method name, and a list of parameters.
     * If the short mode is true, the method signature consists of the full name of the class, 
     * the method name, except for overloaded methods.
     * 
     * Example.
     * If the short pattern is false, the method signature format is as in the following example
     * org.nopasserby.fastminiraft.api.StoreService.set(byte[], byte[])
     * If the short pattern is true, the method signature format is as in the following example
     * org.nopasserby.fastminiraft.api.StoreService.set
     * */
    public static Map<Method, String> getMethodSignatureMap(Class<?> clazz, boolean shortMode) {
        Map<Method, String> services = new HashMap<>();
        List<String> overloadMethods = Arrays.asList(clazz.getMethods()).stream().map(Method::getName).collect(Collectors.toList());
        overloadMethods.stream().collect(Collectors.toSet()).forEach(methodname -> {
            overloadMethods.remove(methodname);
        });
        for (Method method: clazz.getMethods()) {
            String methodSignature = clazz.getCanonicalName() + "." + method.getName(); // full name
            if (!shortMode || overloadMethods.contains(method.getName())) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                for (int i = 0; i < parameterTypes.length; i++) {
                    if (i == 0) {
                        methodSignature += "(";                        
                    }
                    methodSignature += parameterTypes[i].getCanonicalName();
                    if (i != parameterTypes.length - 1) {
                        methodSignature += ",";
                    } else {                        
                        methodSignature += ")";
                    }
                }    
            }
            services.put(method, methodSignature);
        }
        return services;
    }
    
}
