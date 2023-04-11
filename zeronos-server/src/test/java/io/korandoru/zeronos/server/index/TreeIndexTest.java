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
import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import io.korandoru.zeronos.server.record.IndexGetResult;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TreeIndexTest {

    @Test
    void testGet() {
        final TreeIndex treeIndex = new TreeIndex();
        final KeyBytes key = new KeyBytes("foo");
        final Revision created = new Revision(2);
        final Revision modified = new Revision(4);
        final Revision deleted = new Revision(6);
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

    @Test
    void testRevisions() {
        final TreeIndex treeIndex = new TreeIndex();
        treeIndex.put(new KeyBytes("foo").getKey(), new Revision(1));
        treeIndex.put(new KeyBytes("foo1").getKey(), new Revision(2));
        treeIndex.put(new KeyBytes("foo2").getKey(), new Revision(3));
        treeIndex.put(new KeyBytes("foo2").getKey(), new Revision(4));
        treeIndex.put(new KeyBytes("foo1").getKey(), new Revision(5));
        treeIndex.put(new KeyBytes("foo").getKey(), new Revision(6));

        record TestCase(byte[] key, byte[] end, long revision, int limit, List<Revision> revisions, long count) {
            TestCase(String key, String end, long revision, int limit, long count, Revision... revisions) {
                this(
                        key.getBytes(StandardCharsets.UTF_8),
                        end != null ? end.getBytes(StandardCharsets.UTF_8) : null,
                        revision,
                        limit,
                        Arrays.stream(revisions).toList(),
                        count
                );
            }
        }

        final TestCase[] tests = new TestCase[]{
                // single key that not found
                new TestCase("bar", null, 6, 0, 0),
                // single key that found
                new TestCase("foo", null, 6, 0, 1, new Revision(6)),
                // various range keys, fixed atRev, unlimited
                new TestCase("foo", "foo1", 6, 0, 1, new Revision(6)),
                new TestCase("foo", "foo2", 6, 0, 2, new Revision(6), new Revision(5)),
                new TestCase("foo", "fop", 6, 0, 3, new Revision(6), new Revision(5), new Revision(4)),
                new TestCase("foo1", "fop", 6, 0, 2, new Revision(5), new Revision(4)),
                new TestCase("foo2", "fop", 6, 0, 1, new Revision(4)),
                new TestCase("foo3", "fop", 6, 0, 0),
                // fixed range keys, various atRev, unlimited
                new TestCase("foo1", "fop", 1, 0, 0),
                new TestCase("foo1", "fop", 2, 1, 1, new Revision(2)),
                new TestCase("foo1", "fop", 3, 2, 2, new Revision(2), new Revision(3)),
                new TestCase("foo1", "fop", 4, 2, 2, new Revision(2), new Revision(4)),
                new TestCase("foo1", "fop", 5, 2, 2, new Revision(5), new Revision(4)),
                new TestCase("foo1", "fop", 6, 2, 2, new Revision(5), new Revision(4)),
                // fixed range keys, fixed atRev, various limit
                new TestCase("foo", "fop", 6, 1, 3, new Revision(6)),
                new TestCase("foo", "fop", 6, 2, 3, new Revision(6), new Revision(5)),
                new TestCase("foo", "fop", 6, 3, 3, new Revision(6), new Revision(5), new Revision(4)),
                new TestCase("foo", "fop", 3, 1, 3, new Revision(1)),
                new TestCase("foo", "fop", 3, 2, 3, new Revision(1), new Revision(2)),
                new TestCase("foo", "fop", 3, 3, 3, new Revision(1), new Revision(2), new Revision(3)),
        };
        for (TestCase test : tests) {
            assertThat(treeIndex.range(test.key, test.end, test.revision).getTotal()).isEqualTo(test.count);
            assertThat(treeIndex.range(test.key, test.end, test.revision, test.limit).getTotal()).isEqualTo(test.count);
            assertThat(treeIndex.range(test.key, test.end, test.revision, test.limit).getRevisions()).isEqualTo(test.revisions);
        }
    }

    @Test
    void testTombstone() {
        final TreeIndex treeIndex = new TreeIndex();
        final KeyBytes key = new KeyBytes("foo");
        treeIndex.put(key.getKey(), new Revision(1));
        treeIndex.tombstone(key.getKey(), new Revision(2));
        assertThrows(ZeronosServerException.RevisionNotFound.class, () -> treeIndex.get(key.getKey(), 2));
        assertThrows(ZeronosServerException.RevisionNotFound.class, () -> treeIndex.tombstone(key.getKey(), new Revision(3)));
    }

}
