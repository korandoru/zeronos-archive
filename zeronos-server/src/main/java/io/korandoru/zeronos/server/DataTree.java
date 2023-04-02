package io.korandoru.zeronos.server;

import java.util.SortedMap;
import java.util.TreeMap;

public class DataTree {

    private final SortedMap<String, DataNode> nodes = new TreeMap<>();

    public DataTree() {
        this.nodes.put("", new DataNode(new DataNodeEntry(0, new byte[0])));
    }

    public void createNode(String path, byte[] data, long revision) {
        final int slash = path.lastIndexOf('/');
        final String parentPath = path.substring(0, slash);

        final DataNode parent = nodes.get(parentPath);
        if (parent == null) {
            throw new RuntimeException("NoNode");
        }

        final DataNode child = nodes.get(path);
        if (child == null) {
            nodes.put(path, new DataNode(new DataNodeEntry(revision, data)));
        } else {
            child.createNode(new DataNodeEntry(revision, data));
        }
    }

    public DataNodeEntry readNode(String path) {
        final DataNode node = nodes.get(path);
        if (node == null) {
            throw new RuntimeException("NoNode");
        }
        return node.readNodeLatest();
    }
}
