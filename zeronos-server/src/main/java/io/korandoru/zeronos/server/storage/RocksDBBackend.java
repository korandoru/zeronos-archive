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

package io.korandoru.zeronos.server.storage;

import io.korandoru.zeronos.server.record.BackendRangeResult;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

@Slf4j
public class RocksDBBackend implements Backend, AutoCloseable {

    private final RocksDB db;

    public RocksDBBackend(File dataDir) throws IOException {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            this.db = RocksDB.open(options, dataDir.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public BackendRangeResult unsafeRange(Namespace ns, byte[] key, byte[] end, long limit) {
        final long bound;
        final Predicate<byte[]> isMatch;
        if (end == null) {
            bound = 1;
            isMatch = bytes -> Arrays.equals(bytes, key);
        } else {
            bound = limit > 0 ? limit : Long.MAX_VALUE;
            isMatch = bytes -> Arrays.compare(bytes, end) < 0;
        }

        final List<byte[]> keys = new ArrayList<>();
        final List<byte[]> values = new ArrayList<>();
        final RocksIterator it = db.newIterator();
        final byte[] fixedKey = ns.fixKey(key);
        for (it.seek(fixedKey); it.isValid(); it.next()) {
            final byte[] bytes = ns.unfixKey(it.key());
            if (!isMatch.test(bytes)) {
                break;
            }
            keys.add(bytes);
            values.add(it.value());
            if (keys.size() >= bound) {
                break;
            }
        }
        return new BackendRangeResult(keys, values);
    }

    @Override
    public void unsafePut(Namespace ns, byte[] key, byte[] value) {
        final byte[] bytes = ns.fixKey(key);
        try {
            db.put(bytes, value);
        } catch (RocksDBException e) {
            log.atError().addKeyValue("namespace", ns).log("failed to write to the backend", e);
            throw new UncheckedIOException(new IOException(e));
        }
    }

    @Override
    public void close() {
        db.close();
    }
}
