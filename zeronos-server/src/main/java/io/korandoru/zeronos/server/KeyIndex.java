package io.korandoru.zeronos.server;

import io.korandoru.zeronos.server.exception.ZeronosServerException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
    private Revision modified = new Revision();

    private boolean newGeneration = true;

    private final byte[] key;

    private final List<Generation> generations;

    public static KeyIndex of(byte[] key) {
        return new KeyIndex(key, new ArrayList<>());
    }

    public static KeyIndex of(String key) {
        return of(key.getBytes(StandardCharsets.UTF_8));
    }

    private KeyIndex(byte[] key, List<Generation> generations) {
        this.key = key;
        this.generations = generations;
    }

    public void put(Revision revision) {
        if (revision.compareTo(modified) < 0) {
            final ZeronosServerException e = new ZeronosServerException();
            log.atError()
                    .addKeyValue("given-revision", revision)
                    .addKeyValue("modified-revision", modified)
                    .log("'put' with an unexpected smaller revision", e);
            throw e;
        }

        if (newGeneration) {
            newGeneration = false;
            generations.add(new Generation(0, revision, new ArrayList<>()));
        }

        assert !generations.isEmpty();
        final Generation generation = generations.get(generations.size() - 1);
        generation.getRevisions().add(revision);
        generation.increase();

        modified = revision;
    }

    public void tombstone(Revision revision) {
        if (newGeneration) {
            final ZeronosServerException e = new ZeronosServerException();
            log.atError()
                    .addKeyValue("key", new String(key))
                    .log("'tombstone' got an unexpected empty keyIndex", e);
            throw e;
        }

        put(revision);

        newGeneration = true;
    }
}
