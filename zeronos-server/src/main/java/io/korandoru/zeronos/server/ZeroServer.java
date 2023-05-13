/*
 * Copyright 2023 Korandoru Contributors
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

package io.korandoru.zeronos.server;

import io.korandoru.zeronos.proto.DeleteRangeRequest;
import io.korandoru.zeronos.proto.PutRequest;
import io.korandoru.zeronos.proto.RangeRequest;
import io.korandoru.zeronos.proto.RequestOp;
import io.korandoru.zeronos.proto.TxnRequest;
import io.korandoru.zeronos.proto.TxnResponse;
import io.korandoru.zeronos.server.state.ZeroStateMachine;
import java.io.IOException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.client.GrpcClientRpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.NetUtils;

@Slf4j
public class ZeroServer implements AutoCloseable {

    private final RaftServer server;

    public ZeroServer() throws IOException {
        final RaftPeer peer =
                RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:21096").build();
        final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
        final RaftProperties properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);
        final RaftGroupId groupId = RaftGroupId.valueOf(new UUID(0, 1));
        final RaftGroup group = RaftGroup.valueOf(groupId, peer);
        this.server = RaftServer.newBuilder()
                .setGroup(group)
                .setProperties(properties)
                .setServerId(peer.getId())
                .setStateMachine(new ZeroStateMachine())
                .build();
    }

    public ZeroServer start() throws IOException {
        this.server.start();
        return this;
    }

    @Override
    public void close() throws IOException {
        this.server.close();
    }

    public static void main(String[] args) throws IOException {
        try (final var ignore = new ZeroServer().start()) {
            final RaftPeer peer = RaftPeer.newBuilder()
                    .setId("n0")
                    .setAddress("127.0.0.1:21096")
                    .build();
            final RaftProperties properties = new RaftProperties();
            final GrpcClientRpc rpc =
                    new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);
            final RaftGroupId groupId = RaftGroupId.valueOf(new UUID(0, 1));
            final RaftGroup group = RaftGroup.valueOf(groupId, peer);
            final RaftClient client = RaftClient.newBuilder()
                    .setProperties(properties)
                    .setRaftGroup(group)
                    .setClientRpc(rpc)
                    .build();
            try (client) {
                var resp = client.io()
                        .send(Message.valueOf(TxnRequest.newBuilder()
                                .addSuccess(RequestOp.newBuilder()
                                        .setRequestPut(PutRequest.newBuilder()
                                                .setKey(ByteString.copyFromUtf8("foo"))
                                                .setValue(ByteString.copyFromUtf8("bar"))
                                                .build())
                                        .build())
                                .addSuccess(RequestOp.newBuilder()
                                        .setRequestPut(PutRequest.newBuilder()
                                                .setKey(ByteString.copyFromUtf8("foz"))
                                                .setValue(ByteString.copyFromUtf8("baz"))
                                                .build())
                                        .build())
                                .build()));
                System.out.println(TxnResponse.parseFrom(resp.getMessage().getContent()));

                resp = client.io()
                        .sendReadOnly(Message.valueOf(TxnRequest.newBuilder()
                                .addSuccess(RequestOp.newBuilder()
                                        .setRequestRange(RangeRequest.newBuilder()
                                                .setKey(ByteString.copyFromUtf8("foo"))
                                                .setRangeEnd(ByteString.copyFrom(new byte[] {0}))
                                                .build())
                                        .build())
                                .build()));
                System.out.println(TxnResponse.parseFrom(resp.getMessage().getContent()));

                resp = client.io()
                        .send(Message.valueOf(TxnRequest.newBuilder()
                                .addSuccess(RequestOp.newBuilder()
                                        .setRequestDeleteRange(DeleteRangeRequest.newBuilder()
                                                .setKey(ByteString.copyFromUtf8("foo"))
                                                .setRangeEnd(ByteString.copyFrom(new byte[] {0}))
                                                .build())
                                        .build())
                                .build()));
                System.out.println(TxnResponse.parseFrom(resp.getMessage().getContent()));

                resp = client.io()
                        .sendReadOnly(Message.valueOf(TxnRequest.newBuilder()
                                .addSuccess(RequestOp.newBuilder()
                                        .setRequestRange(RangeRequest.newBuilder()
                                                .setKey(ByteString.copyFromUtf8("foo"))
                                                .build())
                                        .build())
                                .build()));
                System.out.println(TxnResponse.parseFrom(resp.getMessage().getContent()));
            }
        }
    }
}
