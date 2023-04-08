package io.korandoru.zeronos.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import org.junit.jupiter.api.Test;

class TreeIndexTest {

    @Test
    void testGet() {
        final TreeIndex treeIndex = new TreeIndex();
        final KeyBytes key = new KeyBytes("foo");
        treeIndex.put(key.getKey(), new Revision(2, 0));
        treeIndex.put(key.getKey(), new Revision(4, 0));
        treeIndex.tombstone(key.getKey(), new Revision(6, 0));

        assertThrows(ZeronosServerException.class, () -> treeIndex.get(key.getKey(), 0));
        assertThrows(ZeronosServerException.class, () -> treeIndex.get(key.getKey(), 1));
        assertThat(treeIndex.get(key.getKey(), 2)).isEqualTo(new Revision(2, 0));
        assertThat(treeIndex.get(key.getKey(), 3)).isEqualTo(new Revision(2, 0));
        assertThat(treeIndex.get(key.getKey(), 4)).isEqualTo(new Revision(4, 0));
        assertThat(treeIndex.get(key.getKey(), 5)).isEqualTo(new Revision(4, 0));
        assertThrows(ZeronosServerException.class, () -> treeIndex.get(key.getKey(), 6));
    }

}
