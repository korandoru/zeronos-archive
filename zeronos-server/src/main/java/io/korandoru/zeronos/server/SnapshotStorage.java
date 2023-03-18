/*
 * Copyright 2022 Korandoru Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.korandoru.zeronos.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.file.DirectoryStreamFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;

@Slf4j
public class SnapshotStorage implements StateMachineStorage {

    private File stateMachineDir;

    @Override
    public void init(RaftStorage raftStorage) {
        this.stateMachineDir = raftStorage.getStorageDir().getStateMachineDir();
    }

    @Override
    @SneakyThrows
    public FileListSnapshotInfo getLatestSnapshot() {
        return findLatestSnapshotDir()
                .map(this::findLatestSnapshot)
                .orElse(null);
    }

    @SneakyThrows
    private FileListSnapshotInfo findLatestSnapshot(File latestSnapshotDir) {
        final var termIndex = getTermIndexFromDir(latestSnapshotDir);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(latestSnapshotDir.toPath())) {
            final var fileList = StreamSupport.stream(stream.spliterator(), false)
                    .map(path -> new FileInfo(path, null))
                    .toList();
            return new FileListSnapshotInfo(fileList, termIndex.getTerm(), termIndex.getIndex());
        }
    }

    @SneakyThrows
    public Optional<File> findLatestSnapshotDir() {
        final var snapshots = getSortedSnapshotDirPaths();
        return snapshots.stream().findFirst().map(Path::toFile);
    }

    private List<Path> getSortedSnapshotDirPaths() throws IOException {
        final var filter = new DirectoryStreamFilter(DirectoryFileFilter.INSTANCE);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateMachineDir.toPath(), filter)) {
            return StreamSupport.stream(stream.spliterator(), false)
                    .sorted(Comparator.comparing(SnapshotStorage::getTermIndexFromPath).reversed())
                    .toList();
        }
    }

    @Override
    public void format() {
        // this method is now placeholder-only in upstream
    }

    @Override
    public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
        // no cleanup yet
    }

    File getSnapshotDirectory(TermIndex termIndex) {
        return new File(stateMachineDir, getMetadataFromTermIndex(termIndex));
    }

    static TermIndex getTermIndexFromPath(Path snapshotPath) {
        return getTermIndexFromDir(snapshotPath.toFile());
    }

    static TermIndex getTermIndexFromDir(File snapshotDir) {
        return getTermIndexFromMetadataString(snapshotDir.getName());
    }

    static TermIndex getTermIndexFromMetadataString(String metadata) {
        final var index = UUID.fromString(metadata);
        return TermIndex.valueOf(index.getMostSignificantBits(), index.getLeastSignificantBits());
    }

    static String getMetadataFromTermIndex(TermIndex termIndex) {
        final var index = new UUID(termIndex.getTerm(), termIndex.getIndex());
        return index.toString();
    }
}
