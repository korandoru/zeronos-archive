package io.korandoru.zeronos.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class KeyIndex {

    public static final Comparator<KeyIndex> KEY_COMPARATOR = (o1, o2) -> Arrays.compare(o1.key, o2.key);

    /**
     * The main rev of the last modification.
     */
    private Revision modified;

    private boolean newGeneration = true;

    private final byte[] key;

    private final List<Generation> generations;

    public void put(Revision revision) {
        if (revision.compareTo(modified) < 0) {
            log.atError()
                    .addKeyValue("given-revision", revision)
                    .addKeyValue("modified-revision", modified)
                    .log("'put' with an unexpected smaller revision");
            throw new ConcurrentModificationException();
        }

        final Generation generation;
        if (newGeneration) {
            newGeneration = !newGeneration;
            generation = new Generation(0, revision, new ArrayList<>());
        } else {
            assert !generations.isEmpty();
            generation = generations.get(generations.size() - 1);
        }
        generation.getRevisions().add(revision);
        generation.increase();

        modified = revision;
    }

}
