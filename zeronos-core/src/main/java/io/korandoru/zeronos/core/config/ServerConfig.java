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

package io.korandoru.zeronos.core.config;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.IOException;
import java.net.URL;
import java.util.Optional;

public class ServerConfig {

    private final DocumentContext context;

    public static ServerConfig defaultConfig() throws IOException {
        return readConfig(ServerConfig.class.getResource("/server.toml"));
    }

    public static ServerConfig readConfig(URL source) throws IOException {
        final var mapper = new TomlMapper();
        final var root = mapper.readTree(source);
        final var config = Configuration.builder().mappingProvider(new JacksonMappingProvider()).build();
        final var context = JsonPath.using(config).parse(root.toPrettyString());
        return new ServerConfig(context);
    }

    private ServerConfig(DocumentContext context) {
        this.context = context;
    }

    public String storageBasedir() {
        return context.read("$.storage.basedir");
    }

    public boolean snapshotAutoTriggerEnabled() {
        return findSnapshotAutoTriggerThreshold().isPresent();
    }

    public long snapshotAutoTriggerThreshold() {
        return findSnapshotAutoTriggerThreshold().orElse(400000L);
    }

    private Optional<Long> findSnapshotAutoTriggerThreshold() {
        return Optional.ofNullable(context.read("$.snapshot.auto-trigger-threshold", Long.class));
    }

    @Override
    public String toString() {
        return context.jsonString();
    }

}
