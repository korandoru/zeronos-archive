package io.korandoru.zeronos.server;

import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

@Data
@Slf4j
public class KeyIndex {

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

    public static KeyIndex of(KeyBytes key) {
        return new KeyIndex(key.getKey(), new ArrayList<>());
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

    public Revision get(long revision) {
        if (generations.isEmpty()) {
            final ZeronosServerException e = new ZeronosServerException();
            log.atError()
                    .addKeyValue("key", new String(key))
                    .log("'get' got an unexpected empty keyIndex", e);
            throw e;
        }

        final Generation generation = findGeneration(revision);
        if (generation == null || generation.isEmpty()) {
            throw new ZeronosServerException();
        }

        final int n = generation.walk(r -> r.getMain() > revision);
        if (n != -1) {
            return generation.getRevisions().get(n);
        }

        throw new ZeronosServerException();
    }

    @Nullable
    @VisibleForTesting
    Generation findGeneration(long revision) {
        for (int idx = generations.size() - 1; idx >= 0; idx--) {
            final Generation generation = generations.get(idx);
            final List<Revision> revisions = generation.getRevisions();
            if (revisions.get(revisions.size() - 1).getMain() <= revision) {
                return null;
            }
            if (generation.getRevisions().get(0).getMain() <= revision) {
                return generation;
            }
            idx--;
        }
        return null;
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
