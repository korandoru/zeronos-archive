package io.korandoru.zeronos.server;

import java.util.List;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;

@AllArgsConstructor
@Data
public class Generation {

    @Setter(AccessLevel.NONE)
    private long version;

    /**
     * When the generation is created (put in first revision).
     */
    private final Revision created;

    private final List<Revision> revisions;

    public boolean isEmpty() {
        return revisions.isEmpty();
    }

    public void increase() {
        version++;
    }

    public int walk(Predicate<Revision> predicate) {
        final int len = revisions.size();
        for (int i = 0; i < len; i++) {
            final int idx = len - i - 1;
            if (!predicate.test(revisions.get(idx))) {
                return idx;
            }
        }
        return -1;
    }

}
