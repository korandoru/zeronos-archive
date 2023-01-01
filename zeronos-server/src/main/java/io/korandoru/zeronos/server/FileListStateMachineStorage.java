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
import java.util.ArrayList;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;

@Slf4j
public class FileListStateMachineStorage implements StateMachineStorage {

    private static final String SNAPSHOT_FILE_PREFIX = "snapshot";

    /* snapshot-<term>-<index> */
    private static final Pattern SNAPSHOT_REGEX = Pattern.compile(SNAPSHOT_FILE_PREFIX + "-(\\d+)-(\\d+)");

    private File smDir;

    @Override
    public void init(RaftStorage raftStorage) {
        this.smDir = raftStorage.getStorageDir().getStateMachineDir();
    }

    @Override
    @SneakyThrows
    public FileListSnapshotInfo getLatestSnapshot() {
        return findLatestSnapshotDirectory()
                .map(this::listSnapshotFiles)
                .orElse(null);
    }

    @SneakyThrows
    private FileListSnapshotInfo listSnapshotFiles(Path latestSnapshotPath) {
        final var termIndex = getTermIndexFromSnapshotDirectory(latestSnapshotPath.toFile());
        final var fileList = new ArrayList<FileInfo>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(latestSnapshotPath)) {
            for (Path path : stream) {
                final var fileDigest = MD5FileUtil.readStoredMd5ForFile(path.toFile());
                fileList.add(new FileInfo(path, fileDigest));
            }
        }
        return new FileListSnapshotInfo(fileList, termIndex.getTerm(), termIndex.getIndex());
    }

    private Optional<Path> findLatestSnapshotDirectory() throws IOException {
        var latestIndex = RaftLog.INVALID_LOG_INDEX;
        var latestSnapshotPath = Optional.<Path>empty();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(smDir.toPath())) {
            for (Path path : stream) {
                final var matcher = SNAPSHOT_REGEX.matcher(path.getFileName().toString());
                if (matcher.matches()) {
                    final long endIndex = Long.parseLong(matcher.group(2));
                    if (endIndex > latestIndex) {
                        latestSnapshotPath = Optional.of(path);
                        latestIndex = endIndex;
                    }
                }
            }
        }
        return latestSnapshotPath;
    }

    @Override
    public void format() {
        // this method is now placeholder-only in upstream
    }

    @Override
    public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
        // no cleanup yet
    }

    public File getSnapshotDirectory(long term, long endIndex) {
        return new File(smDir, getSnapshotFileName(term, endIndex));
    }

    public static TermIndex getTermIndexFromSnapshotDirectory(File directory) {
        final var name = directory.getName();
        final var matcher = SNAPSHOT_REGEX.matcher(name);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(directory + " does not match snapshot filename pattern " + SNAPSHOT_REGEX);
        }
        final var term = Long.parseLong(matcher.group(1));
        final var index = Long.parseLong(matcher.group(2));
        return TermIndex.valueOf(term, index);
    }

    private static String getSnapshotFileName(long term, long endIndex) {
        return SNAPSHOT_FILE_PREFIX + "-" + term + "-" + endIndex;
    }
}
