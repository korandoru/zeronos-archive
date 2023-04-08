package io.korandoru.zeronos.server.record;

import io.korandoru.zeronos.server.Revision;
import lombok.Data;

@Data
public class IndexGetResult {

    private final Revision modified;
    private final Revision created;
    private final long version;

}
