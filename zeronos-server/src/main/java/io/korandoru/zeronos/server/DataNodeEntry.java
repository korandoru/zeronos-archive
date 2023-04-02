package io.korandoru.zeronos.server;

import lombok.Data;

@Data
public class DataNodeEntry {
    private final long revision;
    private final byte[] data;
}
