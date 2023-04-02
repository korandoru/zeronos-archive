/*
 * Copyright 2022 Korandoru Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.korandoru.zeronos.client;

import io.korandoru.zeronos.core.config.ClusterConfig;
import io.korandoru.zeronos.core.proto.GetRequest;
import io.korandoru.zeronos.core.proto.GetResponse;
import io.korandoru.zeronos.core.proto.PutRequest;
import io.korandoru.zeronos.core.proto.PutResponse;
import java.util.Optional;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public class ZeronosClient implements AutoCloseable {

    private final RaftClient client;

    public ZeronosClient(ClusterConfig clusterConfig) {
        final var groupId = RaftGroupId.valueOf(clusterConfig.groupId());
        final var properties = new RaftProperties();
        final var rpc = new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);
        final var peers = clusterConfig.peers().stream()
                .map(model -> RaftPeer.newBuilder().setId(model.id()).setAddress(model.address()).build())
                .toList();
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(RaftGroup.valueOf(groupId, peers))
                .setClientRpc(rpc)
                .build();
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }

    public Optional<String> get(String key) throws Exception {
        final var request = GetRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).build();
        final var reply = this.client.io().sendReadOnly(Message.valueOf(request.toByteString()));
        final var response = GetResponse.parseFrom(reply.getMessage().getContent());
        if (!response.getFound()) {
            return Optional.empty();
        }
        return Optional.of(response.getValue().toStringUtf8());
    }

    public void put(String key, String value) throws Exception {
        final var request = PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build();
        final var reply = client.io().send(Message.valueOf(request.toByteString()));
        PutResponse.parseFrom(reply.getMessage().getContent());
    }

}
