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
import io.korandoru.zeronos.core.config.ServerConfig;
import io.korandoru.zeronos.server.Server;
import java.util.HashMap;
import java.util.UUID;
import lombok.Cleanup;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class ZeronosClientTest {

    @Test
    public void testPutGet() throws Exception {
        final var serverConfig = ServerConfig.defaultConfig();
        final var clusterConfig = ClusterConfig.defaultConfig();
        @Cleanup final var server = new Server(serverConfig, clusterConfig, "n0");
        server.start();
        @Cleanup final var client = new ZeronosClient(clusterConfig);

        final var dataMap = new HashMap<String, String>();
        for (int i = 0; i < 128; i++) {
            dataMap.put("k" + i, UUID.randomUUID().toString());
        }

        for (var e : dataMap.entrySet()) {
            client.put(e.getKey(), e.getValue());
        }

        for (var e : dataMap.entrySet()) {
            final var v = client.get(e.getKey());
            Assertions.assertThat(v).isNotEmpty();
            Assertions.assertThat(v.get()).isEqualTo(e.getValue());
        }
    }

}
