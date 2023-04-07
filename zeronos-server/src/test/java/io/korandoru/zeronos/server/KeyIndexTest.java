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

}
