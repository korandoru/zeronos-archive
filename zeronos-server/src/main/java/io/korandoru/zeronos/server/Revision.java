package io.korandoru.zeronos.server;

import java.util.Comparator;
import lombok.Data;

@Data
public class Revision implements Comparable<Revision> {
    private final long main;
    private final long sub;

    public Revision() {
        this(0, 0);
    }

    public Revision(long main, long sub) {
        this.main = main;
        this.sub = sub;
    }

    @Override
    public int compareTo(Revision o) {
        return Comparator.comparing(Revision::getMain)
                .thenComparing(Revision::getSub)
                .compare(this, o);
    }
}
