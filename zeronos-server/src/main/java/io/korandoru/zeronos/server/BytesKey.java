package io.korandoru.zeronos.server;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import org.jetbrains.annotations.NotNull;

public record BytesKey(byte[] key) implements Comparable<BytesKey> {

    public BytesKey(String key) {
        this(key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int compareTo(@NotNull BytesKey o) {
        return Arrays.compare(key, o.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BytesKey bytesKey = (BytesKey) o;

        return Arrays.equals(key, bytesKey.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }

    @Override
    public String toString() {
        return HexFormat.of().formatHex(key);
    }
}
