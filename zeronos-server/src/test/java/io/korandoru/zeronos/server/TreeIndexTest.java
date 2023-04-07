package io.korandoru.zeronos.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import org.junit.jupiter.api.Test;

class TreeIndexTest {

    @Test
    void testGet() {
        final TreeIndex treeIndex = new TreeIndex();
        final BytesKey key = new BytesKey("foo");
        treeIndex.put(key.key(), new Revision(2, 0));
        treeIndex.put(key.key(), new Revision(4, 0));
        treeIndex.tombstone(key.key(), new Revision(6, 0));

        assertThrows(ZeronosServerException.class, () -> treeIndex.get(key.key(), 0));
        assertThrows(ZeronosServerException.class, () -> treeIndex.get(key.key(), 1));
        assertThat(treeIndex.get(key.key(), 2)).isEqualTo(new Revision(2, 0));
        assertThat(treeIndex.get(key.key(), 3)).isEqualTo(new Revision(2, 0));
        assertThat(treeIndex.get(key.key(), 4)).isEqualTo(new Revision(4, 0));
        assertThat(treeIndex.get(key.key(), 5)).isEqualTo(new Revision(4, 0));
        assertThrows(ZeronosServerException.class, () -> treeIndex.get(key.key(), 6));
    }

}
