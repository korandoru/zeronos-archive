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

package io.korandoru.dryad.config;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.net.URL;

public class DryadConfig {

    private final DocumentContext context;

    public static DryadConfig defaultConfig() throws IOException {
        return readConfig(DryadConfig.class.getResource("/dryad.toml"));
    }

    public static DryadConfig readConfig(URL source) throws IOException {
        final var mapper = new TomlMapper();
        final var root = mapper.readTree(source);
        final var context = JsonPath.parse(root.toPrettyString());
        return new DryadConfig(context);
    }

    private DryadConfig(DocumentContext context) {
        this.context = context;
    }

    public String storageBasedir() {
        return context.read("$.storage.basedir");
    }

    public long snapshotThreshold() {
        return context.read("$.snapshot.threshold", Long.TYPE);
    }

    @Override
    public String toString() {
        return context.jsonString();
    }

}
