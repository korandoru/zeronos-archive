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

package io.korandoru.zeronos.proto;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class KeyBytes implements Comparable<KeyBytes> {

    private final byte[] key;

    public KeyBytes(String key) {
        this(key.getBytes(StandardCharsets.UTF_8));
    }

    public KeyBytes(byte[] key) {
        this.key = key;
    }

    @Override
    public int compareTo(@NotNull KeyBytes o) {
        return Arrays.compare(key, o.key);
    }

    public boolean isInfinite() {
        return Arrays.equals(this.key, new byte[0]);
    }

    @Override
    public String toString() {
        return HexFormat.of().formatHex(key);
    }
}
