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

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Options {
    
    static Logger logger = LoggerFactory.getLogger(Options.class);
    
    public final static String APP_NAME = "FastMiniRaft";
    
    public final static String APP_DIR = System.getProperty("app.dir", System.getProperty("user.dir"));
    
    @Resource(name = "app.data.path")
    private String appDataPath = APP_DIR + File.separator + APP_NAME;
    
    // example : n1-192.168.0.1:6001;n2-192.168.0.2:6001;n3-192.168.0.3:6001;
    @Resource(name = "server.cluster")
    private String serverCluster = "default-0.0.0.0:6001;"; 
    
    @Resource(name = "server.id")
    private String serverId = "default";
    
    @Resource(name = "min.election.timeout")
    private Integer minElectionTimeout = 2000;
    
    @Resource(name = "max.election.timeout")
    private Integer maxElectionTimeout = 4000;
    
    @Resource(name = "heart.beat.period")
    private Integer heartbeatPeriod = 500;
    
    @Resource(name = "quorum.timeout")
    private Long quorumTimeout = 2500L;
    
    @Resource(name = "replica.timeout")
    private Long replicaTimeout = 1000L;
    
    @Resource(name = "requests.queue.depth")
    private Integer queueDepthOfRequests = 64 * 1024;
    
    @Resource(name = "restore.safe.distance")
    private Long restoreSafeDistance = 4L * 1024 * 1024 * 1024; // 4GB
    
    @Resource(name = "flush.max.entries")
    private Integer flushMaxEntries = 8000;
    
    @Resource(name = "flush.sync.disk")
    private Boolean flushSyncDisk = false;
    
    @Resource(name = "entry.buffer.capacity")
    private Long bufferCapacityOfEntry = 1024L * 1024 * 1024; // 1GB
    
    @Resource(name = "index.buffer.capacity")
    private Long bufferCapacityOfIndex = 256L * 1024 * 1024;  // 256MB

    @Resource(name = "buffer.threshold")
    private Integer bufferThreshold = 8 * 1024; // 8K
    
    public Options() throws Exception {
        this.reload();
    }
    
    public void reload() throws Exception {
        for (Field field: Options.class.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Resource.class)) {
                continue;
            }
            String option = field.getAnnotation(Resource.class).name();
            String value = System.getProperty(option);
            if (value == null) {
                continue;
            }
            field.setAccessible(true);
            Object valueOfFieldType = field.getType().getConstructor(String.class).newInstance(value);
            field.set(this, valueOfFieldType);
        }
    }
    
    public String getServerCluster() {
        return serverCluster;
    }

    public void setServerCluster(String serverCluster) {
        this.serverCluster = serverCluster;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public int getMinElectionTimeout() {
        return minElectionTimeout;
    }

    public void setMinElectionTimeout(int minElectionTimeout) {
        this.minElectionTimeout = minElectionTimeout;
    }

    public int getMaxElectionTimeout() {
        return maxElectionTimeout;
    }

    public void setMaxElectionTimeout(int maxElectionTimeout) {
        this.maxElectionTimeout = maxElectionTimeout;
    }

    public int getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public void setHeartbeatPeriod(int heartbeatPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
    }

    public long getQuorumTimeout() {
        return quorumTimeout;
    }

    public void setQuorumTimeout(long quorumTimeout) {
        this.quorumTimeout = quorumTimeout;
    }

    public long getReplicaTimeout() {
        return replicaTimeout;
    }

    public void setReplicaTimeout(long replicaTimeout) {
        this.replicaTimeout = replicaTimeout;
    }

    public int getQueueDepthOfRequests() {
        return queueDepthOfRequests;
    }

    public void setQueueDepthOfRequests(int queueDepthOfRequests) {
        this.queueDepthOfRequests = queueDepthOfRequests;
    }

    public long getRestoreSafeDistance() {
        return restoreSafeDistance;
    }

    public void setRestoreSafeDistance(long restoreSafeDistance) {
        this.restoreSafeDistance = restoreSafeDistance;
    }
    
    public Integer getFlushMaxEntries() {
        return flushMaxEntries;
    }

    public void setFlushMaxEntries(Integer flushMaxEntries) {
        this.flushMaxEntries = flushMaxEntries;
    }

    public boolean getFlushSyncDisk() {
        return flushSyncDisk;
    }

    public void setFlushSyncDisk(boolean flushSyncDisk) {
        this.flushSyncDisk = flushSyncDisk;
    }
    
    public long getBufferCapacityOfEntry() {
        return bufferCapacityOfEntry;
    }

    public void setBufferCapacityOfEntry(long bufferCapacityOfEntry) {
        this.bufferCapacityOfEntry = bufferCapacityOfEntry;
    }

    public long getBufferCapacityOfIndex() {
        return bufferCapacityOfIndex;
    }

    public void setBufferCapacityOfIndex(long bufferCapacityOfIndex) {
        this.bufferCapacityOfIndex = bufferCapacityOfIndex;
    }

    public int getBufferThreshold() {
        return bufferThreshold;
    }

    public void setBufferThreshold(int bufferThreshold) {
        this.bufferThreshold = bufferThreshold;
    }
    
    public String getAppDataPath() {
        return appDataPath;
    }

    public void setAppDataPath(String appDataPath) {
        this.appDataPath = appDataPath;
    }

    public String getServerDataPath() {
        return appDataPath.toLowerCase() + "/" + serverId;
    }
    
    public String getServerStatePersistPath() {
        return appDataPath.toLowerCase() + "/" + serverId + "/node_state.properties";
    }
    
    public String getServerHost() {
        return Arrays.asList(serverCluster.split(";")).stream()
                .collect(Collectors.toMap(s -> s.split("-")[0], s -> s.split("-")[1])).get(serverId);
    }
    
    public Properties getProperties() throws Exception {
        Properties properties = new Properties();
        for (Field field: Options.class.getDeclaredFields()) {
            Resource resource = field.getAnnotation(Resource.class);
            if (resource == null) continue;
            properties.setProperty(resource.name(), field.get(this).toString());
        }
        return properties;
    }
    
}
