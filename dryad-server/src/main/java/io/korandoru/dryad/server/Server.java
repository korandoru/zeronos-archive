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

import io.korandoru.dryad.config.ClusterConfig;
import io.korandoru.dryad.config.ServerConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

public class Server implements AutoCloseable {

    private final RaftServer server;

    public Server(ServerConfig serverConfig, ClusterConfig clusterConfig) throws Exception {
        final var peers = clusterConfig.peers();
        final var peer = peers.stream()
                .filter(p -> p.id().equals(serverConfig.selector()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("This server doesn't belong to a peer."));
        final var raftPeer = RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.address()).build();
        final var port = NetUtils.createSocketAddr(raftPeer.getAddress()).getPort();

        final var properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);

        final var basedir = new File(serverConfig.storageBasedir(), serverConfig.selector());
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(basedir));

        final var groupId = RaftGroupId.valueOf(clusterConfig.groupId());
        final var stateMachine = new DataMapStatemachine();
        this.server = RaftServer.newBuilder()
                .setGroup(RaftGroup.valueOf(groupId, raftPeer))
                .setProperties(properties)
                .setServerId(raftPeer.getId())
                .setStateMachine(stateMachine)
                .build();
    }

    public void start() throws IOException {
        this.server.start();
    }

    @Override
    public void close() throws Exception {
        this.server.close();
    }

    public static void main(String[] args) throws Exception {
        final var server = new Server(ServerConfig.defaultConfig(), ClusterConfig.defaultConfig());
        try (server) {
            server.start();
            // exit when any input entered
            new Scanner(System.in).nextLine();
        }
    }
}
