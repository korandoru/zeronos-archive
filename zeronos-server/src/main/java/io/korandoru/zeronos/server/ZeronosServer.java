/*
 * Copyright 2022 Korandoru Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.korandoru.zeronos.server;

import io.korandoru.zeronos.core.config.ClusterConfig;
import io.korandoru.zeronos.core.config.ServerConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

@Slf4j
public class ZeronosServer implements AutoCloseable {

    private final RaftServer server;

    public ZeronosServer(ServerConfig serverConfig, ClusterConfig clusterConfig, String id) throws Exception {
        Validate.notEmpty(id);
        Validate.notNull(serverConfig);
        Validate.notNull(clusterConfig);

        final var peers = clusterConfig.peers().stream()
                .map(model -> RaftPeer.newBuilder().setId(model.id()).setAddress(model.address()).build())
                .toList();
        final var peer = peers.stream()
                .filter(p -> p.getId().toString().equals(id))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("This server doesn't belong to a peer."));
        final var port = NetUtils.createSocketAddr(peer.getAddress()).getPort();

        final var properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);

        final var basedir = new File(serverConfig.storageBasedir(), id);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(basedir));

        if (serverConfig.snapshotAutoTriggerEnabled()) {
            RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
            RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, serverConfig.snapshotAutoTriggerThreshold());
        }

        final var groupId = RaftGroupId.valueOf(clusterConfig.groupId());
        final var group = RaftGroup.valueOf(groupId, peers);
        final var stateMachine = new DataMapStateMachine();
        this.server = RaftServer.newBuilder()
                .setGroup(group)
                .setProperties(properties)
                .setServerId(peer.getId())
                .setStateMachine(stateMachine)
                .build();

        log.info("Create RaftServer with group: {}", group);
    }

    public void start() throws IOException {
        this.server.start();
    }

    @Override
    public void close() throws IOException {
        this.server.close();
    }

}
