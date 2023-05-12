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

import java.io.File;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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
    public ReadTxn readTxn() {
        return new RocksDBRWBatch(db);
    }

    @Override
    public WriteTxn writeTxn() {
        return new RocksDBRWBatch(db);
    }

    @Override
    public void close() {
        db.close();
    }
}
