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
import io.korandoru.zeronos.server.record.IndexRangeResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TreeIndex {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<KeyBytes, KeyIndex> m = new TreeMap<>();

    public void put(byte[] key, Revision revision) {
        final KeyBytes keyBytes = new KeyBytes(key);
        lock.writeLock().lock();
        try {
            final KeyIndex keyIndex = m.computeIfAbsent(keyBytes, KeyIndex::of);
            keyIndex.put(revision);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void tombstone(byte[] key, Revision revision) {
        lock.writeLock().lock();
        try {
            final KeyIndex keyIndex = m.get(new KeyBytes(key));
            if (keyIndex == null) {
                throw new ZeronosServerException.RevisionNotFound();
            }
            keyIndex.tombstone(revision);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public IndexGetResult get(byte[] key, long revision) {
        lock.readLock().lock();
        try {
            return unsafeGet(key, revision);
        } finally {
            lock.readLock().unlock();
        }
    }

    public IndexRangeResult range(byte[] key, byte[] end, long revision) {
        return range(key, end, revision, 0);
    }

    public IndexRangeResult range(byte[] key, byte[] end, long revision, long limit) {
        lock.readLock().lock();
        try {
            final List<Revision> revisions = new ArrayList<>();
            final List<KeyBytes> keys = new ArrayList<>();
            final AtomicInteger count = new AtomicInteger();

            if (end == null) {
                try {
                    final IndexGetResult result = unsafeGet(key, revision);
                    revisions.add(result.getModified());
                    keys.add(new KeyBytes(key));
                    count.incrementAndGet();
                } catch (ZeronosServerException.RevisionNotFound ignore) {
                    // not found - return empty result
                }
            } else {
                unsafeVisit(key, end, keyIndex -> {
                    try {
                        final IndexGetResult result = keyIndex.get(revision);
                        if (limit <= 0 || revisions.size() < limit) {
                            revisions.add(result.getModified());
                            keys.add(new KeyBytes(keyIndex.getKey()));
                        }
                        count.incrementAndGet();
                    } catch (ZeronosServerException.RevisionNotFound ignore) {
                        // not found - skip
                    }
                    return true;
                });
            }

            return new IndexRangeResult(revisions, keys, count.get());
        } finally {
            lock.readLock().unlock();
        }
    }

    private IndexGetResult unsafeGet(byte[] key, long revision) {
        final KeyIndex keyIndex = m.get(new KeyBytes(key));
        if (keyIndex == null) {
            throw new ZeronosServerException.RevisionNotFound();
        }
        return keyIndex.get(revision);
    }

    private void unsafeVisit(byte[] key, byte[] end, Function<KeyIndex, Boolean> f) {
        final KeyBytes keyBytes = new KeyBytes(key);
        final KeyBytes endBytes = new KeyBytes(end);

        for (Map.Entry<KeyBytes, KeyIndex> e : m.tailMap(keyBytes).entrySet()) {
            if (!endBytes.isInfinite() && e.getKey().compareTo(endBytes) >= 0) {
                return;
            }
            if (!f.apply(e.getValue())) {
                return;
            }
        }
    }
}
