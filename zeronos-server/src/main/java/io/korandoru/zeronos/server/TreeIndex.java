package io.korandoru.zeronos.server;

import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
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
                throw new ZeronosServerException();
            }
            keyIndex.tombstone(revision);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Revision get(byte[] key, long revision) {
        lock.readLock().lock();
        try {
            return unsafeGet(key, revision);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Revision unsafeGet(byte[] key, long revision) {
        final KeyIndex keyIndex = m.get(new KeyBytes(key));
        if (keyIndex == null) {
            throw new ZeronosServerException();
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
