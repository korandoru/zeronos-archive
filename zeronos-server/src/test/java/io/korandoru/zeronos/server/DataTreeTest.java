package io.korandoru.zeronos.server;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DataTreeTest {

    @Test
    void testCreateNode() {
        final DataTree tree = new DataTree();
        final AtomicLong revision = new AtomicLong();
        final byte[][] datas = new byte[][]{
                "zero".getBytes(StandardCharsets.UTF_8),
                "one".getBytes(StandardCharsets.UTF_8),
                "two".getBytes(StandardCharsets.UTF_8),
        };
        tree.createNode("/zeronos", datas[0], revision.incrementAndGet());
        tree.createNode("/zeronos/altair", datas[1], revision.incrementAndGet());
        tree.createNode("/zeronos/vega", datas[2], revision.incrementAndGet());
        assertThat(tree.readNode("/zeronos")).isEqualTo(new DataNodeEntry(1, datas[0]));
        assertThat(tree.readNode("/zeronos/altair")).isEqualTo(new DataNodeEntry(2, datas[1]));
        assertThat(tree.readNode("/zeronos/vega")).isEqualTo(new DataNodeEntry(3, datas[2]));
    }

}
