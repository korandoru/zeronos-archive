package io.korandoru.zeronos.server;

import io.korandoru.zeronos.server.exception.ZeronosServerException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TreeIndex {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<BytesKey, KeyIndex> m = new TreeMap<>();

    public void put(byte[] key, Revision revision) {
        final BytesKey bytesKey = new BytesKey(key);
        lock.writeLock().lock();
        try {
            final KeyIndex keyIndex = m.computeIfAbsent(bytesKey, KeyIndex::of);
            keyIndex.put(revision);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void tombstone(byte[] key, Revision revision) {
        lock.writeLock().lock();
        try {
            final KeyIndex keyIndex = m.get(new BytesKey(key));
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
        final KeyIndex keyIndex = m.get(new BytesKey(key));
        if (keyIndex == null) {
            throw new ZeronosServerException();
        }
        return keyIndex.get(revision);
    }

}
