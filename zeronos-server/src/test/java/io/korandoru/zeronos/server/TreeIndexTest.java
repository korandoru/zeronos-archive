package io.korandoru.zeronos.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import io.korandoru.zeronos.server.record.IndexGetResult;
import org.junit.jupiter.api.Test;

class TreeIndexTest {

    @Test
    void testGet() {
        final TreeIndex treeIndex = new TreeIndex();
        final KeyBytes key = new KeyBytes("foo");
        final Revision created = new Revision(2, 0);
        final Revision modified = new Revision(4, 0);
        final Revision deleted = new Revision(6, 0);
        treeIndex.put(key.getKey(), created);
        treeIndex.put(key.getKey(), modified);
        treeIndex.tombstone(key.getKey(), deleted);

        assertThrows(ZeronosServerException.RevisionNotFound.class, () -> treeIndex.get(key.getKey(), 0));
        assertThrows(ZeronosServerException.RevisionNotFound.class, () -> treeIndex.get(key.getKey(), 1));
        assertThat(treeIndex.get(key.getKey(), 2)).isEqualTo(new IndexGetResult(created, created, 1));
        assertThat(treeIndex.get(key.getKey(), 3)).isEqualTo(new IndexGetResult(created, created, 1));
        assertThat(treeIndex.get(key.getKey(), 4)).isEqualTo(new IndexGetResult(modified, created, 2));
        assertThat(treeIndex.get(key.getKey(), 5)).isEqualTo(new IndexGetResult(modified, created, 2));
        assertThrows(ZeronosServerException.RevisionNotFound.class, () -> treeIndex.get(key.getKey(), 6));
    }

}
