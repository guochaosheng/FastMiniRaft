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

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

public class ProxyUtil {

    private static final String LS = System.lineSeparator();
    
    private static Map<Class<?>, Class<?>> classProxyCache = new HashMap<Class<?>, Class<?>>();
    
    private static Map<Method, Class<?>> methodProxyCache = new HashMap<Method, Class<?>>();
    
    private static class JavaStringTemplate {
        
        static final String TEMPLATE_1;
        static final String TEMPLATE_2;
        static final String TEMPLATE_3;
        static final String TEMPLATE_4;
        static final String TEMPLATE_5;
        
        static {                                                                                        
            TEMPLATE_1 = "import java.lang.reflect.InvocationHandler;                                   " + LS
                       + "import java.lang.reflect.Method;                                              " + LS
                       + "public class #{classname} implements #{interfaceClassname} {                  " + LS
                       + "    Method[] methods;                                                         " + LS
                       + "    InvocationHandler invocationHandler;                                      " + LS
                       + "                                                                              " + LS
                       + "    #{methodArea}                                                             " + LS
                       + "}                                                                             " + LS;
        }                                                                                                  
                                                                                                          
        static {                                                                                          
            TEMPLATE_2 = "public #{returnType} #{methodname}(#{funArgsString}) {                        " + LS
                       + "    Object[] args = new Object[] { #{argsString} };                           " + LS
                       + "    try {                                                                     " + LS
                       + "        InvocationHandler h = this.invocationHandler;                         " + LS
                       + "        return (#{returnType}) h.invoke(this, methods[#{methodsIndex}], args);" + LS
                       + "    } catch(Throwable e) {                                                    " + LS
                       + "          throw new RuntimeException(e);                                      " + LS
                       + "    }                                                                         " + LS
                       + "}                                                                             " + LS;
        }                                                                                                    
                                                                                                            
        static {                                                                                            
            TEMPLATE_3 = "public void #{methodname}(#{funArgsString}) {                                 " + LS
                       + "    Object[] args = new Object[] { #{argsString} };                           " + LS
                       + "    try {                                                                     " + LS
                       + "        InvocationHandler h = this.invocationHandler;                         " + LS
                       + "        h.invoke(this, methods[#{number}], args);                             " + LS
                       + "    } catch(Throwable e) {                                                    " + LS
                       + "        throw new RuntimeException(e);                                        " + LS
                       + "    }                                                                         " + LS
                       + "}                                                                             " + LS;
        }                                                                                                   
                                                                                                            
        static {                                                                                            
            TEMPLATE_4 = "import java.util.function.Function;                                           " + LS
                       + "public class #{classname} implements Function<Object[], Object> {             " + LS
                       + "     #{declaringClassname} obj;                                               " + LS
                       + "     public Object apply(Object[] args) {                                     " + LS
                       + "         return obj.#{methodname}(#{argsString});                             " + LS
                       + "     }                                                                        " + LS
                       + "}                                                                             " + LS;
        }                                                                                                    
                                                                                                             
        static {                                                                                             
            TEMPLATE_5 = "import java.util.function.Function;                                           " + LS
                       + "public class #{classname} implements Function<Object[], Object> {             " + LS
                       + "     #{declaringClassname} obj;                                               " + LS
                       + "     public Object apply(Object[] args) {                                     " + LS
                       + "         obj.#{methodname}(#{argsString});                                    " + LS
                       + "         return null;                                                         " + LS
                       + "     }                                                                        " + LS
                       + "}                                                                             " + LS;
        }                                                                                                  
        
    }
    
    /**
    * public class interfaceClassname$Proxy0 implements interfaceClassname {
    *     private Method[] methods;
    *     private InvocationHandler h;
    *     
    *     public T xxx(A a, B b, ...) {
    *         Object[] args = new Object[] { a, b, ... };
    *         try {
    *             return (T) h.invoke(this, methods[0], args);
    *         } catch(Throwable e) {
    *             throw new RuntimeException(e);
    *         }
    *     }
    *     public T xxxx(A a, B b, C c...) {
    *         Object[] args = new Object[] { a, b, c, ... };
    *         try {
    *             return (T) h.invoke(this, methods[1], args);
    *         } catch(Throwable e) {
    *             throw new RuntimeException(e);
    *         }
    *     }
    * }
    */
    public static <T> String generateInterfaceProxyJavaSource(Class<T> interfaceClass) {
        BiFunction<Class<?>[], Boolean, String> fmt = (args, requireClass) -> {
            StringBuilder strBuilder = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                strBuilder.append((requireClass ? args[i].getCanonicalName() : "") + (" arg" + i));
                if (i != args.length - 1) {
                    strBuilder.append(",");
                }
            }
            return strBuilder.toString();
        };
        
        String methodArea = "";
        Method[] methods = interfaceClass.getMethods();
        for (int i = 0; i < methods.length; i++) {
            boolean noreturn = methods[i].getReturnType().isAssignableFrom(void.class);
            
            String returnType = methods[i].getReturnType().getName();
            String methodname = methods[i].getName();
            String funArgsString = fmt.apply(methods[i].getParameterTypes(), true);
            String argsString = fmt.apply(methods[i].getParameterTypes(), false);
            
            String methodTemplate = new String(noreturn ? JavaStringTemplate.TEMPLATE_3 : JavaStringTemplate.TEMPLATE_2); 
            methodArea += methodTemplate.replaceAll("\\#\\{returnType\\}", returnType)
                                        .replaceAll("\\#\\{methodname\\}", methodname)
                                        .replaceAll("\\#\\{funArgsString\\}", funArgsString)
                                        .replaceAll("\\#\\{argsString\\}", argsString)
                                        .replaceAll("\\#\\{methodsIndex\\}", i + "");
        }
        methodArea = methodArea.replaceAll(LS, LS + "    "); // append line feed indent
        
        String classname = interfaceClass.getSimpleName();
        String interfacename = interfaceClass.getCanonicalName();
        
        String clsTemplate = new String(JavaStringTemplate.TEMPLATE_1);
        clsTemplate = clsTemplate.replaceAll("\\#\\{classname\\}", classname + "\\$0")
                                 .replaceAll("\\#\\{interfaceClassname\\}", interfacename)
                                 .replaceAll("\\#\\{methodArea\\}", methodArea);
        return clsTemplate;
    }
    
    /**
     * import java.util.function.Function;
     * public class Function$0 implements Function<Object[], Object> {
     *     private interfaceClassname obj;
     *     public Object apply(Object[] args) {
     *         obj.xxx((parameterType0) args[0], (parameterType1) args[1], ...); 
     *         return null;
     *     }
     * }
     * 
     * OR
     * 
     * import java.util.function.Function;
     * public class Function$0 implements Function<Object[], Object> {
     *     private interfaceClassname obj;
     *     public Object apply(Object[] args) {
     *         return obj.xxx((parameterType0) args[0], (parameterType1) args[1], ...);
     *     }
     * }
     */
    public static <T> String generateMethodProxyJavaSource(Method method) {
        Function<Class<?>[], String> fmt = args -> {
            StringBuilder strBuilder = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                strBuilder.append("(" + args[i].getCanonicalName() + ")" + (" args[" + i + "]"));
                if (i != args.length - 1) {
                    strBuilder.append(",");
                }
            }
            return strBuilder.toString();
        };
        
        boolean noreturn = method.getReturnType().isAssignableFrom(void.class);
        
        String methodname = method.getName();
        String declaringClassname = method.getDeclaringClass().getCanonicalName();
        String classname = method.getDeclaringClass().getSimpleName();
        String argsString = fmt.apply(method.getParameterTypes());
        
        String methodTemplate = new String(noreturn ? JavaStringTemplate.TEMPLATE_5 : JavaStringTemplate.TEMPLATE_4); 
        
        return methodTemplate.replaceAll("\\#\\{declaringClassname\\}", declaringClassname)
                             .replaceAll("\\#\\{classname\\}", classname + "\\$" + methodname + "\\$0")
                             .replaceAll("\\#\\{methodname\\}", methodname)
                             .replaceAll("\\#\\{argsString\\}", argsString);
    }
    
    public static <T> T newInterfaceProxy(Class<T> interfaceClass, InvocationHandler invocationHandler) {
        Class<?> proxy = classProxyCache.computeIfAbsent(interfaceClass, key -> {
            return loadProxyClass(generateInterfaceProxyJavaSource(interfaceClass));
        });
        
        try {
            Object object = proxy.newInstance();
            Field methodsField = proxy.getDeclaredField("methods");
            methodsField.setAccessible(true);
            methodsField.set(object, interfaceClass.getMethods());
            Field hField = proxy.getDeclaredField("invocationHandler");
            hField.setAccessible(true);
            hField.set(object, invocationHandler);
            return interfaceClass.cast(object);
        } catch (Exception e) {
            throw new RuntimeException(proxy.getName() + " instantiation failed.", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static Function<Object[], Object> newMethodProxy(Method method, Object obj) {
        Class<?> proxy = methodProxyCache.computeIfAbsent(method, key -> {
            return loadProxyClass(generateMethodProxyJavaSource(method));
        });
        
        try {
            Function<Object[], Object> funObject = (Function<Object[], Object>) proxy.newInstance();
            Field objField = proxy.getDeclaredField("obj");
            objField.setAccessible(true);
            objField.set(funObject, obj);
            return funObject;
        } catch (Exception e) {
            throw new RuntimeException(proxy.getName() + " instantiation failed.", e);
        }
    }
    
    private static Class<?> loadProxyClass(String source) {
        String classname = source.substring(source.indexOf("class") + "class".length(), source.indexOf("implements")).trim();
        String javaclass = classname + ".java";
        
        JavaStringCompiler javaCompiler = new JavaStringCompiler();
        Map<String, byte[]> results = javaCompiler.compile(javaclass, source);
        
        try {
            return javaCompiler.loadClass(classname, results);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(javaclass + " class not found.", e);
        } catch (IOException e) {
            throw new RuntimeException(javaclass + " load class failed.", e);
        }
    }
    
    /**
     * @see https://github.com/michaelliao/compiler/blob/master/src/main/java/com/itranswarp/compiler/JavaStringCompiler.java
     */
    private static class JavaStringCompiler {

        JavaCompiler compiler;
        StandardJavaFileManager stdManager;

        public JavaStringCompiler() {
            this.compiler = ToolProvider.getSystemJavaCompiler();
            this.stdManager = compiler.getStandardFileManager(null, null, null);
        }

        public Map<String, byte[]> compile(String fileName, String source) {
            try (MemoryJavaFileManager manager = new MemoryJavaFileManager(stdManager)) {
                JavaFileObject javaFileObject = manager.makeStringSource(fileName, source);
                CompilationTask task = compiler.getTask(new StringWriter(), manager, null, null, null, Arrays.asList(javaFileObject));
                Boolean result = task.call();
                if (result == null || !result.booleanValue()) {
                    throw new RuntimeException("Compilation failed.");
                }
                return manager.getClassBytes();
            }
        }

        public Class<?> loadClass(String name, Map<String, byte[]> classBytes) throws ClassNotFoundException, IOException {
            try (MemoryClassLoader classLoader = new MemoryClassLoader(classBytes)) {
                return classLoader.loadClass(name);
            }
        }
    }
    
    /**
     * @see https://github.com/michaelliao/compiler/blob/master/src/main/java/com/itranswarp/compiler/MemoryClassLoader.java
     * */
    private static class MemoryClassLoader extends URLClassLoader {

        Map<String, byte[]> classBytes = new HashMap<String, byte[]>();

        public MemoryClassLoader(Map<String, byte[]> classBytes) {
            super(new URL[0], MemoryClassLoader.class.getClassLoader());
            this.classBytes.putAll(classBytes);
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            byte[] buf = classBytes.get(name);
            if (buf == null) {
                return super.findClass(name);
            }
            classBytes.remove(name);
            return defineClass(name, buf, 0, buf.length);
        }

    }
    
    /**
     * @see https://github.com/michaelliao/compiler/blob/master/src/main/java/com/itranswarp/compiler/MemoryJavaFileManager.java
     * */
    private static class MemoryJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {

        final Map<String, byte[]> classBytes = new HashMap<String, byte[]>();

        MemoryJavaFileManager(JavaFileManager fileManager) {
            super(fileManager);
        }

        public Map<String, byte[]> getClassBytes() {
            return new HashMap<String, byte[]>(this.classBytes);
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void close() {
            classBytes.clear();
        }

        @Override
        public JavaFileObject getJavaFileForOutput(JavaFileManager.Location location, String className, Kind kind,
                FileObject sibling) throws IOException {
            if (kind == Kind.CLASS) {
                return new MemoryOutputJavaFileObject(className);
            } else {
                return super.getJavaFileForOutput(location, className, kind, sibling);
            }
        }

        JavaFileObject makeStringSource(String name, String code) {
            return new MemoryInputJavaFileObject(name, code);
        }

        static class MemoryInputJavaFileObject extends SimpleJavaFileObject {

            final String code;

            MemoryInputJavaFileObject(String name, String code) {
                super(URI.create("string:///" + name), Kind.SOURCE);
                this.code = code;
            }

            @Override
            public CharBuffer getCharContent(boolean ignoreEncodingErrors) {
                return CharBuffer.wrap(code);
            }
        }

        class MemoryOutputJavaFileObject extends SimpleJavaFileObject {
            final String name;

            MemoryOutputJavaFileObject(String name) {
                super(URI.create("string:///" + name), Kind.CLASS);
                this.name = name;
            }

            @Override
            public OutputStream openOutputStream() {
                return new FilterOutputStream(new ByteArrayOutputStream()) {
                    @Override
                    public void close() throws IOException {
                        out.close();
                        ByteArrayOutputStream bos = (ByteArrayOutputStream) out;
                        classBytes.put(name, bos.toByteArray());
                    }
                };
            }

        }
    }
    
}
