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

import java.nio.ByteBuffer;
import java.util.Comparator;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class Revision implements Comparable<Revision> {
    private final long main;
    private final long sub;

    public Revision() {
        this(0);
    }

    public Revision(long main) {
        this(main, 0);
    }

    public Revision(long main, long sub) {
        this.main = main;
        this.sub = sub;
    }

    public byte[] toBytes() {
        return toBytes(false);
    }

    public byte[] toBytes(boolean tombstone) {
        final byte[] bytes = new byte[18]; // long(8) + _(1) + long(8) + (optional) t(1)
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(main);
        buffer.put((byte) '_');
        buffer.putLong(sub);
        if (tombstone) {
            buffer.put((byte) 't');
        }
        return bytes;
    }

    public static Revision of(@NotNull io.korandoru.zeronos.proto.Revision revision) {
        return new Revision(revision.getMain(), revision.getSub());
    }

    public io.korandoru.zeronos.proto.Revision toProtos() {
        return io.korandoru.zeronos.proto.Revision.newBuilder()
                .setMain(this.main)
                .setSub(this.sub)
                .build();
    }

    @Override
    public int compareTo(@NotNull Revision o) {
        return Comparator.comparing(Revision::getMain)
                .thenComparing(Revision::getSub)
                .compare(this, o);
    }
}
