package io.korandoru.zeronos.server.record;

import io.korandoru.zeronos.server.Revision;
import java.util.List;
import lombok.Data;

@Data
public class IndexRevisionsResult {

    private final List<Revision> revisions;
    private final int total;

}
