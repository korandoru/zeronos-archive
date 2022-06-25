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

package io.korandoru.dryad.server;

import io.korandoru.dryad.proto.GetRequest;
import io.korandoru.dryad.proto.GetResponse;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.NetUtils;

@Slf4j
public class HashMapStatemachine extends BaseStateMachine {
    private final Map<String, String> dataMap = new ConcurrentHashMap<>();
    {
        dataMap.putIfAbsent("name", "dataMap");
        dataMap.putIfAbsent("type", "hashMap");
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final var responseBuilder = GetResponse.newBuilder();
        final var requestBuilder = GetRequest.newBuilder();
        try {
            requestBuilder.mergeFrom(GetRequest.parseFrom(request.getContent()));
        } catch (InvalidProtocolBufferException e) {
            log.warn("Receiving invalid message: {}", request, e);
            responseBuilder.setFound(false);
            return CompletableFuture.completedFuture(Message.valueOf(responseBuilder.build().toByteString()));
        }

        final var k = requestBuilder.getKey().toStringUtf8();
        final var v = dataMap.get(k);
        if (v == null) {
            responseBuilder.setFound(false);
        } else {
            responseBuilder.setFound(true);
            responseBuilder.setKey(requestBuilder.getKey());
            responseBuilder.setValue(ByteString.copyFromUtf8(v));
        }

        return CompletableFuture.completedFuture(Message.valueOf(responseBuilder.build().toByteString()));
    }

    public static void main(String[] args) throws Exception {
        final var peer = RaftPeer.newBuilder().setAddress("127.0.0.1:10024").setId("n0").build();
        final var port = NetUtils.createSocketAddr(peer.getAddress()).getPort();

        final var properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);

        final var groupId = RaftGroupId.valueOf(UUID.randomUUID());
        final var stateMachine = new HashMapStatemachine();
        final var server = RaftServer.newBuilder()
            .setGroup(RaftGroup.valueOf(groupId, peer))
            .setProperties(properties)
            .setServerId(peer.getId())
            .setStateMachine(stateMachine)
            .build();
        try (server) {
            server.start();
            // exit when any input entered
            new Scanner(System.in).nextLine();
        }
    }
}
