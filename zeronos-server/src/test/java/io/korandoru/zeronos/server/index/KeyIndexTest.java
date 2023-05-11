/*
 * Copyright 2023 Korandoru Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.korandoru.zeronos.server.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import java.util.List;
import org.junit.jupiter.api.Test;

class KeyIndexTest {

    @Test
    void testTombstone() {
        final KeyIndex keyIndex0 = KeyIndex.of("foo");
        keyIndex0.put(new Revision(5));
        keyIndex0.tombstone(new Revision(7));

        {
            final KeyIndex keyIndex1 = KeyIndex.of("foo");
            keyIndex1.setModified(new Revision(7));
            keyIndex1
                    .getGenerations()
                    .add(new Generation(2, new Revision(5), List.of(new Revision(5), new Revision(7))));
            assertThat(keyIndex0).isEqualTo(keyIndex1);
        }

        keyIndex0.put(new Revision(8));
        keyIndex0.put(new Revision(9));
        keyIndex0.tombstone(new Revision(15));

        {
            final KeyIndex keyIndex1 = KeyIndex.of("foo");
            keyIndex1.setModified(new Revision(15));
            keyIndex1
                    .getGenerations()
                    .add(new Generation(2, new Revision(5), List.of(new Revision(5), new Revision(7))));
            keyIndex1
                    .getGenerations()
                    .add(new Generation(
                            3, new Revision(8), List.of(new Revision(8), new Revision(9), new Revision(15))));
            assertThat(keyIndex0).isEqualTo(keyIndex1);
        }

        assertThrows(ZeronosServerException.RevisionNotFound.class, () -> keyIndex0.tombstone(new Revision(16)));
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
        keyIndex.put(new Revision(2));
        keyIndex.put(new Revision(4));
        keyIndex.tombstone(new Revision(6));
        keyIndex.put(new Revision(8));
        keyIndex.put(new Revision(10));
        keyIndex.tombstone(new Revision(12));
        keyIndex.put(new Revision(14));
        keyIndex.put(new Revision(14, 1));
        keyIndex.tombstone(new Revision(16));
        return keyIndex;
    }
}
