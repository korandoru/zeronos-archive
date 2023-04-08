package io.korandoru.zeronos.server.record;

import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.Revision;
import java.util.List;
import lombok.Data;

@Data
public class IndexRangeResult {

    private final List<Revision> revisions;
    private final List<KeyBytes> keys;
    private final long total;

}
