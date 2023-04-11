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

import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import io.korandoru.zeronos.server.record.IndexGetResult;
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
            final ZeronosServerException e = new ZeronosServerException.FatalError();
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
        generation.addRevision(revision);

        modified = revision;
    }

    public IndexGetResult get(long revision) {
        if (generations.isEmpty()) {
            final ZeronosServerException e = new ZeronosServerException.FatalError();
            log.atError()
                    .addKeyValue("key", new String(key))
                    .log("'get' got an unexpected empty keyIndex", e);
            throw e;
        }

        final Generation generation = findGeneration(revision);
        if (generation == null || generation.isEmpty()) {
            throw new ZeronosServerException.RevisionNotFound();
        }

        final int idx = generation.walk(r -> r.getMain() > revision);
        if (idx != -1) {
            final Revision modified = generation.getRevision(idx);
            final Revision created = generation.getCreated();
            final long version = idx + 1; // shift to 1-base
            return new IndexGetResult(modified, created, version);
        }

        throw new ZeronosServerException.RevisionNotFound();
    }

    @Nullable
    @VisibleForTesting
    Generation findGeneration(long revision) {
        final int lastGeneration = generations.size() - 1;
        for (int idx = lastGeneration; idx >= 0; idx--) {
            final Generation generation = generations.get(idx);
            if (idx < lastGeneration || newGeneration) {
                if (generation.getLastRevision().getMain() <= revision) {
                    return null;
                }
            }
            if (generation.getFirstRevision().getMain() <= revision) {
                return generation;
            }
        }
        return null;
    }

    public void tombstone(Revision revision) {
        if (generations.isEmpty()) {
            final ZeronosServerException e = new ZeronosServerException.FatalError();
            log.atError()
                    .addKeyValue("key", new String(key))
                    .log("'tombstone' got an unexpected empty keyIndex", e);
            throw e;
        }

        if (newGeneration) {
            throw new ZeronosServerException.RevisionNotFound();
        }

        put(revision);

        newGeneration = true;
    }
}
