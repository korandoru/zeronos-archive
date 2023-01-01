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
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.korandoru.zeronos.core.config.model.RaftPeerModel;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.UUID;

public class ClusterConfig {

    private final DocumentContext context;

    public static ClusterConfig defaultConfig() throws IOException {
        return readConfig(ClusterConfig.class.getResource("/cluster.toml"));
    }

    public static ClusterConfig readConfig(URL source) throws IOException {
        final var mapper = new TomlMapper();
        final var root = mapper.readTree(source);
        final var config = Configuration.builder().mappingProvider(new JacksonMappingProvider()).build();
        final var context = JsonPath.using(config).parse(root.toPrettyString());
        return new ClusterConfig(context);
    }

    private ClusterConfig(DocumentContext context) {
        this.context = context;
    }

    public UUID groupId() {
        return UUID.fromString(context.read("$.group.id"));
    }

    public List<RaftPeerModel> peers() {
        return context.read("$.group.peer", new TypeRef<>() {});
    }

    @Override
    public String toString() {
        return context.jsonString();
    }

}
