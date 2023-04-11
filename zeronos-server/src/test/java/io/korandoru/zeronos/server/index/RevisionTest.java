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
import org.junit.jupiter.api.Test;

class RevisionTest {

    @Test
    void testRevision() {
        final Revision[] revisions = new Revision[]{
                new Revision(),
                new Revision(1),
                new Revision(1, 1),
                new Revision(2),
                new Revision(Long.MAX_VALUE, Long.MAX_VALUE),
        };

        for (int i = 0; i < revisions.length - 1; i++) {
            assertThat(revisions[i]).isLessThan(revisions[i + 1]);
        }
    }

}
