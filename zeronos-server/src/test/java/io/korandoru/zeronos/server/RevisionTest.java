package io.korandoru.zeronos.server;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class RevisionTest {

    @Test
    void testRevision() {
        final Revision[] revisions = new Revision[]{
                new Revision(),
                new Revision(1, 0),
                new Revision(1, 1),
                new Revision(2, 0),
                new Revision(Long.MAX_VALUE, Long.MAX_VALUE),
        };

        for (int i = 0; i < revisions.length - 1; i++) {
            assertThat(revisions[i]).isLessThan(revisions[i + 1]);
        }
    }

}
