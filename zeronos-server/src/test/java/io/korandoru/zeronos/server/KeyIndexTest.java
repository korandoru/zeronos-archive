package io.korandoru.zeronos.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import java.util.List;
import org.junit.jupiter.api.Test;

class KeyIndexTest {

    @Test
    void testTombstone() {
        final KeyIndex keyIndex0 = KeyIndex.of("foo");
        keyIndex0.put(new Revision(5, 0));
        keyIndex0.tombstone(new Revision(7, 0));

        {
            final KeyIndex keyIndex1 = KeyIndex.of("foo");
            keyIndex1.setModified(new Revision(7, 0));
            keyIndex1.getGenerations().add(new Generation(2, new Revision(5, 0), List.of(new Revision(5, 0), new Revision(7, 0))));
            assertThat(keyIndex0).isEqualTo(keyIndex1);
        }

        keyIndex0.put(new Revision(8, 0));
        keyIndex0.put(new Revision(9, 0));
        keyIndex0.tombstone(new Revision(15, 0));

        {
            final KeyIndex keyIndex1 = KeyIndex.of("foo");
            keyIndex1.setModified(new Revision(15, 0));
            keyIndex1.getGenerations().add(new Generation(2, new Revision(5, 0), List.of(new Revision(5, 0), new Revision(7, 0))));
            keyIndex1.getGenerations().add(new Generation(3, new Revision(8, 0), List.of(new Revision(8, 0), new Revision(9, 0), new Revision(15, 0))));
            assertThat(keyIndex0).isEqualTo(keyIndex1);
        }

        assertThrows(ZeronosServerException.class, () -> keyIndex0.tombstone(new Revision(16, 0)));
    }

    @Test
    void testFindGeneration() {
        final KeyIndex keyIndex = createTestKeyIndex();
        final Generation generation0 = keyIndex.getGenerations().get(0);
        final Generation generation1 = keyIndex.getGenerations().get(1);
        assertThat(keyIndex.findGeneration(0)).isNull();
        assertThat(keyIndex.findGeneration(1)).isNull();
        assertThat(keyIndex.findGeneration(2)).isEqualTo(generation0);
        assertThat(keyIndex.findGeneration(3)).isEqualTo(generation0);
        assertThat(keyIndex.findGeneration(4)).isEqualTo(generation0);
        assertThat(keyIndex.findGeneration(5)).isEqualTo(generation0);
        assertThat(keyIndex.findGeneration(6)).isNull();
        assertThat(keyIndex.findGeneration(7)).isNull();
        assertThat(keyIndex.findGeneration(8)).isEqualTo(generation1);
        assertThat(keyIndex.findGeneration(9)).isEqualTo(generation1);
        assertThat(keyIndex.findGeneration(10)).isEqualTo(generation1);
        assertThat(keyIndex.findGeneration(11)).isEqualTo(generation1);
        assertThat(keyIndex.findGeneration(12)).isNull();
        assertThat(keyIndex.findGeneration(13)).isNull();
    }

    private static KeyIndex createTestKeyIndex() {
        final KeyIndex keyIndex = KeyIndex.of("foo");
        keyIndex.put(new Revision(2, 0));
        keyIndex.put(new Revision(4, 0));
        keyIndex.tombstone(new Revision(6, 0));
        keyIndex.put(new Revision(8, 0));
        keyIndex.put(new Revision(10, 0));
        keyIndex.tombstone(new Revision(12, 0));
        keyIndex.put(new Revision(14, 0));
        keyIndex.put(new Revision(14, 1));
        keyIndex.tombstone(new Revision(16, 0));
        return keyIndex;
    }

}
