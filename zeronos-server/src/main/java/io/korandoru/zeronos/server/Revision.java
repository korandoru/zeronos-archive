package io.korandoru.zeronos.server;

import java.util.Comparator;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class Revision implements Comparable<Revision> {
    private final long main;
    private final long sub;

    public Revision() {
        this(0);
    }

    public Revision(long main) {
        this(main, 0);
    }

    public Revision(long main, long sub) {
        this.main = main;
        this.sub = sub;
    }

    @Override
    public int compareTo(@NotNull Revision o) {
        return Comparator.comparing(Revision::getMain)
                .thenComparing(Revision::getSub)
                .compare(this, o);
    }
}
