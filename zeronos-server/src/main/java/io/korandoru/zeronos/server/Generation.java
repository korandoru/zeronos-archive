package io.korandoru.zeronos.server;

import java.util.List;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
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

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final List<Revision> revisions;

    public Revision getRevision(int n) {
        return revisions.get(n);
    }

    public Revision getFirstRevision() {
        return revisions.get(0);
    }

    public Revision getLastRevision() {
        return revisions.get(revisions.size() - 1);
    }

    public void addRevision(Revision revision) {
        revisions.add(revision);
        version++;
    }

    public boolean isEmpty() {
        return revisions.isEmpty();
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
