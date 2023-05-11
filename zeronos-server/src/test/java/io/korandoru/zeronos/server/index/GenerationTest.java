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
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class GenerationTest {

    @Test
    void testIsEmpty() {
        assertThat(new Generation(0, new Revision(), List.of()).isEmpty()).isTrue();
        assertThat(new Generation(0, new Revision(), List.of(new Revision(1))).isEmpty())
                .isFalse();
    }

    @Test
    void testWalk() {
        final List<Revision> revisions = List.of(new Revision(2), new Revision(4), new Revision(6));
        final Generation generation = new Generation(0, revisions.get(0), revisions);
        record TestCase(Predicate<Revision> predicate, int result) {}
        final TestCase[] tests = new TestCase[] {
            new TestCase(r -> r.getMain() >= 7, 2),
            new TestCase(r -> r.getMain() >= 6, 1),
            new TestCase(r -> r.getMain() >= 5, 1),
            new TestCase(r -> r.getMain() >= 4, 0),
            new TestCase(r -> r.getMain() >= 3, 0),
            new TestCase(r -> r.getMain() >= 2, -1),
        };
        for (TestCase test : tests) {
            assertThat(generation.walk(test.predicate)).isEqualTo(test.result);
        }
    }
}
