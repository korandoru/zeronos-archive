package io.korandoru.zeronos.server;

import java.util.Deque;
import java.util.LinkedList;

public class DataNode {

    private final Deque<DataNodeEntry> revisions = new LinkedList<>();

    public DataNode(DataNodeEntry entry) {
        this.revisions.addLast(entry);
    }

    public DataNodeEntry readNodeLatest() {
        // TODO - check if removed
        return this.revisions.getLast();
    }

    public void createNode(DataNodeEntry entry) {
        // TODO - check if exist
        this.revisions.addLast(entry);
    }

    public void updateNode(DataNodeEntry entry) {
        // TODO - check if exist
        this.revisions.addLast(entry);
    }

    public void deleteNode(DataNodeEntry entry) {
        // TODO - check if exist
        this.revisions.addLast(entry);
    }
}
