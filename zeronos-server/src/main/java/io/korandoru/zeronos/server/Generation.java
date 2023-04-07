package io.korandoru.zeronos.server;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.Data;

@Data
public class Generation {

    private final long version;

    /**
     * When the generation is created (put in first revision).
     */
    private final Revision created;

    private final List<Revision> revisions;

    public boolean isEmpty() {
        return revisions.isEmpty();
    }

    public int walk(Predicate<Revision> predicate) {
        final int len = this.revisions.size();
        for (int i = 0; i < len; i++) {
            final int idx = len - i - 1;
            if (!predicate.test(this.revisions.get(idx))) {
                return idx;
            }
        }
        return -1;
    }

}
