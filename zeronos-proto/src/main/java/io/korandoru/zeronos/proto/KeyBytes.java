package io.korandoru.zeronos.proto;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class KeyBytes implements Comparable<KeyBytes> {

    private final byte[] key;

    public KeyBytes(String key) {
        this(key.getBytes(StandardCharsets.UTF_8));
    }

    public KeyBytes(byte[] key) {
        this.key = key;
    }

    @Override
    public int compareTo(@NotNull KeyBytes o) {
        return Arrays.compare(key, o.key);
    }

    public boolean isInfinite() {
        return Arrays.equals(this.key, new byte[0]);
    }

    @Override
    public String toString() {
        return HexFormat.of().formatHex(key);
    }
}
