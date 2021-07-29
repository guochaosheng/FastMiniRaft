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

package org.nopasserby.fastminiraft.boot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.nopasserby.fastminiraft.api.AppendEntriesRequest;
import org.nopasserby.fastminiraft.api.AppendEntriesResponse;
import org.nopasserby.fastminiraft.api.Entry;
import org.nopasserby.fastminiraft.api.VoteRequest;
import org.nopasserby.fastminiraft.api.VoteResponse;
import org.nopasserby.fastminiraft.core.ExceptionTable;
import org.nopasserby.fastminiraft.core.ExceptionTable.OperationException;
import org.nopasserby.fastminirpc.core.RpcRequest;
import org.nopasserby.fastminirpc.core.RpcResponse;
import org.nopasserby.fastminirpc.core.Serializer;
import org.nopasserby.fastminirpc.util.ByteUtil;

import io.netty.util.internal.SystemPropertyUtil;

public class ServiceSerializer implements Serializer {

    private static final boolean COMPRESS_SIGNATURE = SystemPropertyUtil.getBoolean("io.rpc.compress.signature", false);
    
    private Map<Integer, SimpleEntry<DataSerializer<RpcRequest>, DataSerializer<RpcResponse>>> serializersWithCompressSignature = new HashMap<>();
    private Map<String, SimpleEntry<DataSerializer<RpcRequest>, DataSerializer<RpcResponse>>> serializers = new HashMap<>();
    
    private final static String GET_LEADER = "org.nopasserby.fastminiraft.api.ClientService.getLeaderId";
    private final static String GET_CLUSTER = "org.nopasserby.fastminiraft.api.ClientService.getServerCluster";
    private final static String ADD_SERVER = "org.nopasserby.fastminiraft.api.ClientService.addServer";
    private final static String DELETE_SERVER = "org.nopasserby.fastminiraft.api.ClientService.deleteServer";
    
    private final static String ADD = "org.nopasserby.fastminiraft.api.StoreService.add";
    private final static String SET = "org.nopasserby.fastminiraft.api.StoreService.set";
    private final static String CAS = "org.nopasserby.fastminiraft.api.StoreService.cas";
    private final static String GET_BY_INDEX = "org.nopasserby.fastminiraft.api.StoreService.get(long)";
    private final static String GET_BY_KEY = "org.nopasserby.fastminiraft.api.StoreService.get(byte[])";
    private final static String DEL = "org.nopasserby.fastminiraft.api.StoreService.del";
    
    private final static String VOTE = "org.nopasserby.fastminiraft.api.ConsensusService.requestVote";
    private final static String APPEND_ENTRIES = "org.nopasserby.fastminiraft.api.ConsensusService.appendEntries";
    
    {
        registerService(GET_LEADER, new GetLeaderRequestSerializer(), new GetLeaderResponseSerializer());
        registerService(GET_CLUSTER, new GetClusterRequestSerializer(), new GetClusterResponseSerializer());
        registerService(ADD_SERVER, new AddServerRequestSerializer(), new AddServerResponseSerializer());
        registerService(DELETE_SERVER, new DeleteServerRequestSerializer(), new DeleteServerResponseSerializer());
        
        registerService(ADD, new AddRequestSerializer(), new AddResponseSerializer());
        registerService(SET, new SetRequestSerializer(), new SetResponseSerializer());
        registerService(CAS, new CasRequestSerializer(), new CasResponseSerializer());
        registerService(GET_BY_INDEX, new GetByIndexRequestSerializer(), new GetByIndexResponseSerializer());
        registerService(GET_BY_KEY, new GetByKeyRequestSerializer(), new GetByKeyResponseSerializer());
        registerService(DEL, new DelRequestSerializer(), new DelResponseSerializer());
        
        registerService(VOTE, new VoteRequestSerializer(), new VoteResponseSerializer());
        registerService(APPEND_ENTRIES, new AppendEntriesRequestSerializer(), new AppendEntriesResponseSerializer());
    }
    
    @Override
    public void writeObject(Object obj, DataOutput output) {
        try {            
            if (COMPRESS_SIGNATURE)
                writeObjectWhenCompressSignature(obj, output);
            else 
                writeObjectWhenSignature(obj, output);
        } catch (Exception e) {
            throw new IllegalArgumentException("write object error", e);
        }
    }
    
    @Override
    public <T> T readObject(DataInput input, Class<T> clazz) {
        try {            
            return COMPRESS_SIGNATURE ? readObjectWhenCompressSignature(input, clazz) : readObjectWhenSignature(input, clazz);
        } catch (Exception e) {
            throw new IllegalArgumentException("read object error", e);
        }
    }
    
    private void writeObjectWhenSignature(Object obj, DataOutput output) throws IOException {
        if (RpcRequest.class.isInstance(obj)) {
            RpcRequest request = (RpcRequest) obj;
            byte[] signature = request.getMethodSignature();
            output.writeShort(signature.length);
            output.write(signature);
            serializers.get(ByteUtil.toString(signature)).getKey().writeObject(request, output);
        }
        else if (RpcResponse.class.isInstance(obj)) {
            RpcResponse response = (RpcResponse) obj;
            byte[] signature = response.getMethodSignature();
            output.writeShort(signature.length);
            output.write(signature);
            serializers.get(ByteUtil.toString(signature)).getValue().writeObject(response, output);
        }
    }
    
    private void writeObjectWhenCompressSignature(Object obj, DataOutput output) throws IOException {
        if (RpcRequest.class.isInstance(obj)) {            
            RpcRequest request = (RpcRequest) obj;
            Integer signature = ByteUtil.toInt(request.getMethodSignature());
            output.writeInt(signature);
            serializersWithCompressSignature.get(signature).getKey().writeObject(request, output);
        }
        else if (RpcResponse.class.isInstance(obj)) {
            RpcResponse response = (RpcResponse) obj;
            Integer signature = ByteUtil.toInt(response.getMethodSignature());
            output.writeInt(signature);
            serializersWithCompressSignature.get(signature).getValue().writeObject(response, output);
        }
    }
    
    private <T> T readObjectWhenCompressSignature(DataInput input, Class<T> clazz) throws IOException {
        Integer signature = input.readInt();
        if (RpcRequest.class.isAssignableFrom(clazz)) {
            RpcRequest request = new RpcRequest(ByteUtil.toByteArray(signature));
            serializersWithCompressSignature.get(signature).getKey().readObject(input, request);
            return clazz.cast(request);
        }
        else if (RpcResponse.class.isAssignableFrom(clazz)) {
            RpcResponse response = new RpcResponse(ByteUtil.toByteArray(signature));
            serializersWithCompressSignature.get(signature).getValue().readObject(input, response);
            return clazz.cast(response);
        }
        return null;
    }
    
    private <T> T readObjectWhenSignature(DataInput input, Class<T> clazz) throws IOException {
        byte[] signature = new byte[input.readShort()];
        input.readFully(signature);
        if (RpcRequest.class.isAssignableFrom(clazz)) {
            RpcRequest request = new RpcRequest(signature);
            serializers.get(ByteUtil.toString(signature)).getKey().readObject(input, request);
            return clazz.cast(request);
        }
        else if (RpcResponse.class.isAssignableFrom(clazz)) {
            RpcResponse response = new RpcResponse(signature);
            serializers.get(ByteUtil.toString(signature)).getValue().readObject(input, response);
            return clazz.cast(response);
        }
        return null;
    }
    
    private void registerService(String signature, DataSerializer<RpcRequest> requestSerializer, DataSerializer<RpcResponse> responseSerializer) {
        SimpleEntry<DataSerializer<RpcRequest>, DataSerializer<RpcResponse>> entry = new SimpleEntry<>(requestSerializer, responseSerializer);
        serializersWithCompressSignature.put(signature.hashCode(), entry);
        serializers.put(signature, entry);
    }
    
    private static interface DataSerializer<T> {
        public void writeObject(T obj, DataOutput output) throws IOException;
        public void readObject(DataInput input, T obj) throws IOException;
    }
    
    private static class AddRequestSerializer implements DataSerializer<RpcRequest> {
        @Override
        public void writeObject(RpcRequest request, DataOutput output) throws IOException {
            byte[] body = (byte[]) request.getParameters()[0];
            output.writeInt(body.length);
            output.write(body);
        }
        @Override
        public void readObject(DataInput input, RpcRequest request) throws IOException {
            byte[] body = new byte[input.readInt()];
            input.readFully(body);
            request.setParameters(new Object[] { body });
        }
    }
    
    private static class AddResponseSerializer extends AbstractResponseSerializer {
        @Override
        public void writeObject0(RpcResponse response, DataOutput output) throws IOException {
            output.writeLong((Long) response.getReturnObject());
        }
        @Override
        public void readObject0(DataInput input, RpcResponse response) throws IOException {
            long returnObject = input.readLong();
            response.setReturnObject(returnObject);
        }
    }
    
    private static class SetRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            byte[] key = (byte[]) rpcRequest.getParameters() [0];
            byte[] value = (byte[]) rpcRequest.getParameters() [1];
            output.writeInt(key.length);
            output.write(key);
            output.writeInt(value.length);
            output.write(value);
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            byte[] key = new byte[input.readInt()];
            input.readFully(key);
            byte[] value = new byte[input.readInt()];
            input.readFully(value);
            rpcRequest.setParameters(new Object[] { key, value });
        }
        
    }
    
    private static class SetResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
        }
        
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
        }
        
    }
    
    private static class CasRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            byte[] key = (byte[]) rpcRequest.getParameters() [0];
            byte[] expect = (byte[]) rpcRequest.getParameters() [1];
            byte[] update = (byte[]) rpcRequest.getParameters() [2];
            output.writeInt(key.length);
            output.write(key);
            output.writeInt(expect.length);
            output.write(expect);
            output.writeInt(update.length);
            output.write(update);
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            byte[] key = new byte[input.readInt()];
            input.readFully(key);
            byte[] expect = new byte[input.readInt()];
            input.readFully(expect);
            byte[] update = new byte[input.readInt()];
            input.readFully(update);
            rpcRequest.setParameters(new Object[] { key, expect, update });    
        }
        
    }
    
    private static class CasResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
            output.writeBoolean((Boolean) rpcResponse.getReturnObject());
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
            rpcResponse.setReturnObject(input.readBoolean());
        }
        
    }
    
    private static class GetByIndexRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            long index = (long) rpcRequest.getParameters()[0];
            output.writeLong(index);
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            long index = input.readLong();
            rpcRequest.setParameters(new Object[] { index });
        }
        
    }
    
    private static class GetByIndexResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
            byte[] value = (byte[]) rpcResponse.getReturnObject();
            int len = value == null ? -1 : value.length;
            output.writeInt(len);
            if (len > 0) {
                output.write(value);
            }
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
            byte[] value = null;
            int len = input.readInt();
            if (len > 0) {
                value = new byte[len];
                input.readFully(value);
            }
            rpcResponse.setReturnObject(value);
        }
        
    }
    
    private static class GetByKeyRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            byte[] key = (byte[]) rpcRequest.getParameters()[0];
            output.writeInt(key.length);
            output.write(key);
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            byte[] key = new byte[input.readInt()];
            input.readFully(key);
            rpcRequest.setParameters(new Object[] { key });
        }
        
    }
    
    private static class GetByKeyResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
            byte[] value = (byte[]) rpcResponse.getReturnObject();
            int len = value == null ? -1 : value.length;
            output.writeInt(len);
            if (len > 0) {
                output.write(value);
            }
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
            byte[] value = null;
            int len = input.readInt();
            if (len > 0) {
                value = new byte[len];
                input.readFully(value);
            }
            rpcResponse.setReturnObject(value);
        }
        
    }
    
    private static class DelRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            byte[] key = (byte[]) rpcRequest.getParameters()[0];
            output.writeInt(key.length);
            output.write(key);
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            byte[] key = new byte[input.readInt()];
            input.readFully(key);
            rpcRequest.setParameters(new Object[] { key });
        }
        
    }
    
    private static class DelResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
        }
        
    }
    
    private static class AppendEntriesRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            AppendEntriesRequest request = (AppendEntriesRequest) rpcRequest.getParameters()[0];
            
            List<Entry> entries = request.getEntries();
            int entryCount = entries == null ? -1 : entries.size(); // -1 mean null, 0 mean empty
            
            output.writeLong(request.getTerm());
            output.writeUTF(request.getLeaderId() == null ? "" : request.getLeaderId());
            output.writeLong(request.getPrevLogIndex());
            output.writeLong(request.getPrevLogTerm());
            output.writeLong(request.getLeaderCommitIndex());
            output.writeInt(entryCount);
            if (entryCount >= 0) {
                for (Entry logEntry: entries) {
                    output.writeLong(logEntry.getTerm());
                    output.writeLong(logEntry.getIndex());
                    output.writeLong(logEntry.getOffset());
                    output.writeInt(logEntry.getBodyType());
                    byte[] body = logEntry.getBody();
                    output.writeInt(body.length);
                    output.write(body);
                }
            }
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            AppendEntriesRequest request = new AppendEntriesRequest();
            request.setTerm(input.readLong());
            String leaderId = input.readUTF();
            if (!"".equals(leaderId)) {                
                request.setLeaderId(leaderId);
            }
            request.setPrevLogIndex(input.readLong());
            request.setPrevLogTerm(input.readLong());
            request.setLeaderCommitIndex(input.readLong());
            
            int entryCount = input.readInt();
            if (entryCount >= 0) {
                List<Entry> entries = new ArrayList<Entry>();
                for (int i = 0; i < entryCount; i++) {
                    Entry logEntry = new Entry();
                    logEntry.setTerm(input.readLong());
                    logEntry.setIndex(input.readLong());
                    logEntry.setOffset(input.readLong());
                    logEntry.setBodyType(input.readInt());
                    int length = input.readInt();
                    byte[] body = new byte[length];
                    input.readFully(body);
                    logEntry.setBody(body);
                    entries.add(logEntry);
                }
                request.setEntries(entries);
            }
            
            rpcRequest.setParameters(new Object[] { request });
        }
        
    }

    private static class AppendEntriesResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
            AppendEntriesResponse response = (AppendEntriesResponse) rpcResponse.getReturnObject();
            output.writeLong(response.getTerm());
            output.writeLong(response.getIndex());
            output.writeBoolean(response.isSuccess());
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
            AppendEntriesResponse returnObject = new AppendEntriesResponse();
            returnObject.setTerm(input.readLong());
            returnObject.setIndex(input.readLong());
            returnObject.setSuccess(input.readBoolean());
            rpcResponse.setReturnObject(returnObject);
        }
        
    }
    
    private static class GetLeaderRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest obj, DataOutput output) throws IOException {
        }

        @Override
        public void readObject(DataInput input, RpcRequest obj) throws IOException {
        }
        
    }
    
    private static class GetLeaderResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
            String leaderId = (String) rpcResponse.getReturnObject();
            output.writeUTF(leaderId == null ? "" : leaderId);
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
            String leaderId = input.readUTF();
            rpcResponse.setReturnObject("".equals(leaderId) ? null : leaderId);
        }
        
    }
    
    private static class GetClusterRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest obj, DataOutput output) throws IOException {
        }

        @Override
        public void readObject(DataInput input, RpcRequest obj) throws IOException {
        }
        
    }
    
    private static class GetClusterResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
            String serverCluster = (String) rpcResponse.getReturnObject();
            output.writeUTF(serverCluster == null ? "" : serverCluster);
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
            String serverCluster = input.readUTF();
            rpcResponse.setReturnObject("".equals(serverCluster) ? null : serverCluster);
        }
        
    }
    
    private static class AddServerRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            String serverId = (String) rpcRequest.getParameters() [0];
            String serverHost = (String) rpcRequest.getParameters() [1];
            output.writeUTF(serverId);
            output.writeUTF(serverHost);
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            String serverId = input.readUTF();
            String serverHost = input.readUTF();
            rpcRequest.setParameters(new Object[] { serverId, serverHost });
        }
        
    }
    
    private static class AddServerResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
        }
        
    }
    
    private static class DeleteServerRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            String serverId = (String) rpcRequest.getParameters() [0];
            output.writeUTF(serverId);
        }

        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            String serverId = input.readUTF();
            rpcRequest.setParameters(new Object[] { serverId });
        }
        
    }
    
    private static class DeleteServerResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
        }
        
    }
    
    private static class VoteRequestSerializer implements DataSerializer<RpcRequest> {

        @Override
        public void writeObject(RpcRequest rpcRequest, DataOutput output) throws IOException {
            VoteRequest request = (VoteRequest) rpcRequest.getParameters()[0];
            output.writeLong(request.getTerm());
            String candidateId = request.getCandidateId();
            output.writeUTF(candidateId == null ? "" : candidateId);
            output.writeLong(request.getLastLogIndex());
            output.writeLong(request.getLastLogTerm());
            output.writeBoolean(request.isPrepare());
        }
        
        @Override
        public void readObject(DataInput input, RpcRequest rpcRequest) throws IOException {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setTerm(input.readLong());
            String candidateId = input.readUTF();
            if (!"".equals(candidateId)) {
                voteRequest.setCandidateId(candidateId);                
            }
            voteRequest.setLastLogIndex(input.readLong());
            voteRequest.setLastLogTerm(input.readLong());
            voteRequest.setPrepare(input.readBoolean());
            rpcRequest.setParameters(new Object[] { voteRequest });
        }
        
    }
    
    private static class VoteResponseSerializer extends AbstractResponseSerializer {

        @Override
        public void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException {
            VoteResponse response = (VoteResponse) rpcResponse.getReturnObject();
            output.writeLong(response.getTerm());
            output.writeBoolean(response.isVoteGranted());
        }

        @Override
        public void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException {
            VoteResponse returnObject = new VoteResponse();
            returnObject.setTerm(input.readLong());
            returnObject.setVoteGranted(input.readBoolean());
            rpcResponse.setReturnObject(returnObject);
        }
        
    }
    
    private static abstract class AbstractResponseSerializer implements DataSerializer<RpcResponse> {

        @Override
        public void writeObject(RpcResponse rpcResponse, DataOutput output) throws IOException {
            boolean hasException = rpcResponse.getException() != null;
            boolean hasOperationException = hasException && OperationException.class.isInstance(rpcResponse.getException());
            output.writeBoolean(hasException);
            output.writeBoolean(hasOperationException);
            if (hasOperationException) {
                output.writeInt(((OperationException) rpcResponse.getException()).getCode());
            } else if (hasException) {
                StringWriter ex = new StringWriter();
                rpcResponse.getException().printStackTrace(new PrintWriter(ex));
                output.writeUTF(ex.toString());
            } else {
                writeObject0(rpcResponse, output);
            }
        }
        
        @Override
        public void readObject(DataInput input, RpcResponse rpcResponse) throws IOException {
            boolean hasException = input.readBoolean();
            boolean hasOperationException = input.readBoolean();
            if (hasOperationException) {
                rpcResponse.setException(ExceptionTable.getException(input.readInt()));
            } else if (hasException) {
                String ex = input.readUTF();
                rpcResponse.setException(new RuntimeException(ex));
            } else {
                readObject0(input, rpcResponse);
            }
        }
        
        public abstract void writeObject0(RpcResponse rpcResponse, DataOutput output) throws IOException;
        
        public abstract void readObject0(DataInput input, RpcResponse rpcResponse) throws IOException;
        
    }
    
}
