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
