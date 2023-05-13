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

package io.korandoru.zeronos.server.state;

import io.korandoru.zeronos.proto.DeleteRangeRequest;
import io.korandoru.zeronos.proto.DeleteRangeResponse;
import io.korandoru.zeronos.proto.KeyBytes;
import io.korandoru.zeronos.proto.KeyValue;
import io.korandoru.zeronos.proto.PutRequest;
import io.korandoru.zeronos.proto.PutResponse;
import io.korandoru.zeronos.proto.RangeRequest;
import io.korandoru.zeronos.proto.RangeResponse;
import io.korandoru.zeronos.proto.RequestOp;
import io.korandoru.zeronos.proto.ResponseOp;
import io.korandoru.zeronos.proto.TxnRequest;
import io.korandoru.zeronos.proto.TxnResponse;
import io.korandoru.zeronos.server.exception.ZeronosServerException;
import io.korandoru.zeronos.server.index.Revision;
import io.korandoru.zeronos.server.index.TreeIndex;
import io.korandoru.zeronos.server.record.BackendRangeResult;
import io.korandoru.zeronos.server.record.IndexGetResult;
import io.korandoru.zeronos.server.record.IndexRangeResult;
import io.korandoru.zeronos.server.storage.Backend;
import io.korandoru.zeronos.server.storage.Namespace;
import io.korandoru.zeronos.server.storage.ReadTxn;
import io.korandoru.zeronos.server.storage.RocksDBBackend;
import io.korandoru.zeronos.server.storage.WriteTxn;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public class ZeroStateMachine extends BaseStateMachine {

    private File dataDir;
    private TreeIndex treeIndex;
    private Backend backend;

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
        super.initialize(raftServer, raftGroupId, raftStorage);
        dataDir = new File(raftStorage.getStorageDir().getTmpDir(), "backend");
        reinitialize();
    }

    @Override
    public void reinitialize() throws IOException {
        FileUtils.deleteDirectory(dataDir);
        FileUtils.createParentDirectories(dataDir);
        treeIndex = new TreeIndex();
        backend = new RocksDBBackend(dataDir);
    }

    // >= is encoded in the range end as '\0' because null and new byte[0] is the same via gRPC.
    // If it is a GTE range, then KeyBytes.infinity() is returned to indicate the empty byte
    // string (vs null being no byte string).
    private static byte[] decodeGteRange(ByteString rangeEnd) {
        if (rangeEnd.isEmpty()) {
            return null;
        }
        if (rangeEnd.size() == 1 && rangeEnd.byteAt(0) == 0) {
            return KeyBytes.infinity();
        }
        return rangeEnd.toByteArray();
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final List<RequestOp> requestList;
        try {
            final TxnRequest.Builder req = TxnRequest.newBuilder();
            req.mergeFrom(request.getContent());
            requestList = req.getSuccessList();
        } catch (InvalidProtocolBufferException e) {
            return CompletableFuture.failedFuture(e);
        }

        for (RequestOp requestOp : requestList) {
            if (!requestOp.hasRequestRange()) {
                final String message = "readonly message contains mutations: " + requestOp.getRequestCase();
                return CompletableFuture.failedFuture(new IllegalArgumentException(message));
            }
        }

        final List<ResponseOp> responseOps = new ArrayList<>();
        for (RequestOp requestOp : requestList) {
            final RangeRequest req = requestOp.getRequestRange();
            final byte[] end = decodeGteRange(req.getRangeEnd());

            final TermIndex termIndex = getLastAppliedTermIndex();
            final IndexRangeResult r = treeIndex.range(
                    req.getKey().toByteArray(),
                    end,
                    req.getRevision() > 0 ? req.getRevision() : termIndex.getIndex(),
                    req.getLimit());

            final long bound;
            if (req.getLimit() <= 0 || req.getLimit() > r.getRevisions().size()) {
                bound = r.getRevisions().size();
            } else {
                bound = req.getLimit();
            }

            final RangeResponse.Builder resp = RangeResponse.newBuilder();
            try (final ReadTxn readTxn = backend.readTxn()) {
                for (int i = 0; i < bound; i++) {
                    final byte[] key = r.getRevisions().get(i).toBytes();
                    final BackendRangeResult rr = readTxn.unsafeRange(Namespace.KEY, key, null, 0);
                    assert rr.getValues().size() == 1;
                    resp.addKvs(KeyValue.parseFrom(rr.getValues().get(0)));
                }
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }

            responseOps.add(ResponseOp.newBuilder().setResponseRange(resp).build());
        }

        return CompletableFuture.completedFuture(Message.valueOf(
                TxnResponse.newBuilder().addAllResponses(responseOps).build()));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();

        final List<RequestOp> requestList;
        try {
            final TxnRequest.Builder req = TxnRequest.newBuilder();
            req.mergeFrom(entry.getStateMachineLogEntry().getLogData());
            requestList = req.getSuccessList();
        } catch (InvalidProtocolBufferException e) {
            return CompletableFuture.failedFuture(e);
        }

        final List<ResponseOp> responseOps = new ArrayList<>();
        try (final WriteTxn writeTxn = backend.writeTxn()) {
            final AtomicLong sub = new AtomicLong();
            for (RequestOp op : requestList) {
                ResponseOp responseOp = null;
                switch (op.getRequestCase()) {
                    case REQUEST_PUT -> {
                        final PutRequest req = op.getRequestPut();
                        final Revision revision = new Revision(entry.getIndex(), sub.getAndIncrement());
                        final byte[] key = req.getKey().toByteArray();

                        Revision created = revision;
                        long version = 1;
                        try {
                            final IndexGetResult r = treeIndex.get(key, revision.getMain());
                            created = r.getCreated();
                            version = r.getVersion() + 1;
                        } catch (ZeronosServerException.RevisionNotFound ignore) {
                            // no previous reversions - it is fine
                        }

                        final KeyValue kv = KeyValue.newBuilder()
                                .setKey(req.getKey())
                                .setValue(req.getValue())
                                .setCreateRevision(created.toProtos())
                                .setModifyRevision(revision.toProtos())
                                .setVersion(version)
                                .build();

                        writeTxn.unsafePut(Namespace.KEY, revision.toBytes(), kv.toByteArray());
                        treeIndex.put(key, revision);
                        responseOp = ResponseOp.newBuilder()
                                .setResponsePut(PutResponse.newBuilder().build())
                                .build();
                    }
                    case REQUEST_DELETE_RANGE -> {
                        final DeleteRangeRequest req = op.getRequestDeleteRange();
                        final long revision = entry.getIndex();
                        final byte[] key = req.getKey().toByteArray();
                        final byte[] end = decodeGteRange(req.getRangeEnd());

                        final IndexRangeResult r = treeIndex.range(key, end, revision);
                        final Revision rev = new Revision(revision, sub.getAndIncrement());
                        for (KeyBytes k : r.getKeys()) {
                            final KeyValue kv = KeyValue.newBuilder()
                                    .setKey(k.toByteString())
                                    .build();
                            writeTxn.unsafePut(Namespace.KEY, rev.toBytes(true), kv.toByteArray());
                            treeIndex.tombstone(k.getKey(), rev);
                        }
                        responseOp = ResponseOp.newBuilder()
                                .setResponseDeleteRange(DeleteRangeResponse.newBuilder()
                                        .setDeleted(r.getTotal())
                                        .build())
                                .build();
                    }
                }

                if (responseOp != null) {
                    responseOps.add(responseOp);
                } else {
                    return CompletableFuture.failedFuture(new NullPointerException("message is not initialized"));
                }
            }
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return CompletableFuture.completedFuture(Message.valueOf(
                    TxnResponse.newBuilder().addAllResponses(responseOps).build()));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
