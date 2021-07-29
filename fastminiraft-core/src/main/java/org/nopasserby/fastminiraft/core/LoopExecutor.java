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

package org.nopasserby.fastminiraft.core;

import java.util.concurrent.atomic.AtomicBoolean;
import org.nopasserby.fastminirpc.core.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LoopExecutor implements LifeCycle {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    
    private AtomicBoolean running = new AtomicBoolean();
    
    private String name = getClass().getSimpleName();
    
    public LoopExecutor() {
    }
    
    public LoopExecutor(String name) {
        this.name = name;
    }
    
    public static LoopExecutor newLoopExecutor(String name, Runnable runnable) {
        return new LoopExecutor(name) {
            @Override
            protected void execute() throws Exception {
                runnable.run();
            }
        };
    }
    
    @Override
    public void startup() {
        if (running.compareAndSet(false, true)) {   
            Thread thread = new Thread(this::loop);
            thread.setName(name);
            thread.start();
        }
    }
    
    void loop() {
        while (running.get()) {
            try {
                execute();
            } catch (Exception e) {
                exceptionCaught(e);
            }
        }
    }
    
    @Override
    public void shutdown() {
        while (running.compareAndSet(true, false)) {
            Thread.yield();
        }
    }

    protected abstract void execute() throws Exception;
    
    protected void exceptionCaught(Throwable e) {
        logger.warn("", e);
    }
    
}
