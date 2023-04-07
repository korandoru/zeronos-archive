package io.korandoru.zeronos.server;

import java.util.Arrays;
import java.util.Comparator;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class KeyIndex {

    public static final Comparator<KeyIndex> KEY_COMPARATOR = (o1, o2) -> Arrays.compare(o1.key, o2.key);

    private final byte[] key;

    /**
     * The main rev of the last modification.
     */
    private final Revision modified;

}
