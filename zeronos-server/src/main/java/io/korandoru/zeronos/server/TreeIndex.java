package io.korandoru.zeronos.server;

import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import io.korandoru.zeronos.server.record.IndexGetResult;
import io.korandoru.zeronos.server.record.IndexRevisionsResult;
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

    public IndexRevisionsResult revisions(byte[] key, byte[] end, long revision, int limit) {
        lock.readLock().lock();
        try {
            final List<Revision> revisions = new ArrayList<>();

            AtomicInteger count = new AtomicInteger();
            if (end == null) {
                IndexGetResult result = unsafeGet(key, revision);
                revisions.add(result.getModified());
                count.incrementAndGet();
            } else {
                unsafeVisit(key, end, keyIndex -> {
                    final IndexGetResult result = keyIndex.get(revision);
                    if (limit <= 0 || revisions.size() < limit) {
                        revisions.add(result.getModified());
                    }
                    count.incrementAndGet();
                    return true;
                });
            }

            return new IndexRevisionsResult(revisions, count.get());
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

        for (Map.Entry<KeyBytes, KeyIndex> e: m.tailMap(keyBytes).entrySet()) {
            if (!endBytes.isInfinite() && e.getKey().compareTo(endBytes) >= 0) {
                return;
            }
            if (!f.apply(e.getValue())) {
                return;
            }
        }
    }

}
