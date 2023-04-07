package io.korandoru.zeronos.server;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class GenerationTest {

    @Test
    void testIsEmpty() {
        assertThat(new Generation(0, new Revision(), List.of()).isEmpty()).isTrue();
        assertThat(new Generation(0, new Revision(), List.of(new Revision(1, 0))).isEmpty()).isFalse();
    }

    @Test
    void testWalk() {
        final List<Revision> revisions = List.of(new Revision(2, 0), new Revision(4, 0), new Revision(6, 0));
        final Generation generation = new Generation(3, revisions.get(0), revisions);
        record TestCase(Predicate<Revision> predicate, int result) {
        }
        final TestCase[] tests = new TestCase[]{
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
