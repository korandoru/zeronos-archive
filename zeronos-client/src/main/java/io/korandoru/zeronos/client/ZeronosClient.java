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

import io.korandoru.zeronos.core.proto.GetRequest;
import io.korandoru.zeronos.core.proto.GetResponse;
import io.korandoru.zeronos.core.proto.PutRequest;
import io.korandoru.zeronos.core.proto.PutResponse;
import java.util.HashMap;
import java.util.UUID;
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
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;

public class ZeronosClient {

    public static void main(String[] args) throws Exception {
        final var properties = new RaftProperties();

        final var groupId = RaftGroupId.valueOf(UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1"));
        final var rpc = new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);

        final var client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(RaftGroup.valueOf(
                        groupId,
                        RaftPeer.newBuilder().setAddress("127.0.0.1:21096").setId("n0").build()
//                    RaftPeer.newBuilder().setAddress("127.0.0.1:211   96").setId("n1").build(),
//                    RaftPeer.newBuilder().setAddress("127.0.0.1:21296").setId("n2").build()
                ))
                .setClientRpc(rpc)
                .build();

        try (client) {
            final var dataMap = new HashMap<String, String>();
            for (int i = 0; i < 128; i++) {
                dataMap.put("k" + i, UUID.randomUUID().toString());
            }

            final var printer = TextFormat.printer();

            for (var k : dataMap.keySet()) {
                final var request = GetRequest.newBuilder().setKey(ByteString.copyFromUtf8(k)).build();
                final var response = client.io().sendReadOnly(Message.valueOf(request.toByteString()));
                final var v = GetResponse.parseFrom(response.getMessage().getContent());
                System.out.printf("Get key %s return value %s\n" , k, printer.printToString(v));
            }

            for (var e : dataMap.entrySet()) {
                final var request = PutRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(e.getKey()))
                        .setValue(ByteString.copyFromUtf8(e.getValue()))
                        .build();
                final var response = client.io().send(Message.valueOf(request.toByteString()));
                final var r = PutResponse.parseFrom(response.getMessage().getContent());
                System.out.printf("Put key %s with value %s, returns %s\n", e.getKey(), e.getValue(), printer.printToString(r));
            }

            for (var k : dataMap.keySet()) {
                final var request = GetRequest.newBuilder().setKey(ByteString.copyFromUtf8(k)).build();
                final var response = client.io().sendReadOnly(Message.valueOf(request.toByteString()));
                final var v = GetResponse.parseFrom(response.getMessage().getContent());
                System.out.printf("Get key %s return value %s\n" , k, printer.printToString(v));
            }
        }
    }

}
