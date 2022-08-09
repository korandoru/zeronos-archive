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

package io.korandoru.dryad.server;

import io.korandoru.dryad.proto.GetRequest;
import io.korandoru.dryad.proto.GetResponse;
import io.korandoru.dryad.proto.PutRequest;
import io.korandoru.dryad.proto.PutResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

@Slf4j
public class DataMapStatemachine extends BaseStateMachine {
    private final Map<String, String> dataMap = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Message> query(Message request) {
        final var responseBuilder = GetResponse.newBuilder();
        final var requestBuilder = GetRequest.newBuilder();
        try {
            requestBuilder.mergeFrom(GetRequest.parseFrom(request.getContent()));
        } catch (InvalidProtocolBufferException e) {
            log.warn("Receiving invalid message: {}", request, e);
            responseBuilder.setFound(false);
            return CompletableFuture.completedFuture(Message.valueOf(responseBuilder.build().toByteString()));
        }

        final var k = requestBuilder.getKey().toStringUtf8();
        final var v = dataMap.get(k);
        if (v == null) {
            responseBuilder.setFound(false);
        } else {
            responseBuilder.setFound(true);
            responseBuilder.setKey(requestBuilder.getKey());
            responseBuilder.setValue(ByteString.copyFromUtf8(v));
        }

        return CompletableFuture.completedFuture(Message.valueOf(responseBuilder.build().toByteString()));
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
            log.warn("Receiving invalid message: {}", request, e);
            return CompletableFuture.completedFuture(response);
        }

        final var k = requestBuilder.getKey().toStringUtf8();
        final var v = requestBuilder.getValue().toStringUtf8();
        dataMap.put(k, v);

        // update the last applied term and index
        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        return CompletableFuture.completedFuture(response);
    }
}
