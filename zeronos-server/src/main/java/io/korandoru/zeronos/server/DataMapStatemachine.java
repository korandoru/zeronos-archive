/*
 * Copyright 2022 Korandoru Contributors
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

package io.korandoru.zeronos.server;

import io.korandoru.zeronos.core.proto.GetRequest;
import io.korandoru.zeronos.core.proto.GetResponse;
import io.korandoru.zeronos.core.proto.PutRequest;
import io.korandoru.zeronos.core.proto.PutResponse;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

@Slf4j
public class DataMapStatemachine extends BaseStateMachine {
    private RocksDB db;

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
        super.initialize(raftServer, raftGroupId, storage);
        try (Options options = new Options().setCreateIfMissing(true)) {
            this.db = RocksDB.open(options, storage.getStorageDir().getTmpDir().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        this.db.close();
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final var responseBuilder = GetResponse.newBuilder();
        final var requestBuilder = GetRequest.newBuilder();
        try {
            requestBuilder.mergeFrom(GetRequest.parseFrom(request.getContent()));
        } catch (InvalidProtocolBufferException e) {
            log.error("Receiving invalid message: {}", request, e);
            return CompletableFuture.failedFuture(e);
        }

        try {
            final var k = requestBuilder.getKey();
            final var v = db.get(k.toByteArray());
            if (v == null) {
                responseBuilder.setFound(false);
            } else {
                responseBuilder.setFound(true);
                responseBuilder.setKey(k);
                responseBuilder.setValue(ByteString.copyFrom(v));
            }

            return CompletableFuture.completedFuture(Message.valueOf(responseBuilder.build().toByteString()));
        } catch (RocksDBException e) {
            log.error("Cannot read from RocksDB for request: {}", request, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final var response = Message.valueOf(PutResponse.getDefaultInstance().toByteString());
        final var requestBuilder = PutRequest.newBuilder();
        final var entry = trx.getLogEntry();
        final var request = entry.getStateMachineLogEntry();

        try {
            requestBuilder.mergeFrom(request.getLogData());
        } catch (InvalidProtocolBufferException e) {
            log.error("Receiving invalid message: {}", request, e);
            return CompletableFuture.failedFuture(e);
        }

        final var k = requestBuilder.getKey().toByteArray();
        final var v = requestBuilder.getValue().toByteArray();
        try {
            db.put(k, v);
        } catch (RocksDBException e) {
            return CompletableFuture.failedFuture(e);
        }

        // update the last applied term and index
        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        return CompletableFuture.completedFuture(response);
    }
}
