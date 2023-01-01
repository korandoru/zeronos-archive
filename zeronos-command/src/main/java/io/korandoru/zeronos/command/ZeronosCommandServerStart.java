/*
 * Copyright 2022-2023 Korandoru Contributors
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

package io.korandoru.zeronos.command;

import io.korandoru.zeronos.core.config.ClusterConfig;
import io.korandoru.zeronos.core.config.ServerConfig;
import io.korandoru.zeronos.server.ZeronosServer;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import lombok.Lombok;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(
        name = "start",
        version = CommandConstants.VERSION,
        description = "Starting a Zeronos server",
        mixinStandardHelpOptions = true
)
public class ZeronosCommandServerStart implements Callable<Integer> {

    @CommandLine.Mixin
    private CommandServerOptions serverOptions;

    @CommandLine.Option(
            names = "--cluster-config",
            description = "Path to the config file of the whole cluster."
    )
    private File clusterConfig;

    private static final class ServerProcess implements Runnable {
        private final CompletableFuture<Void> close = new CompletableFuture<>();
        private final CompletableFuture<Void> closed;
        private final ZeronosServer server;

        public ServerProcess(ZeronosServer server) {
            this.server = server;
            this.closed = this.close.thenRun(() -> {
                try {
                    this.server.close();
                } catch (IOException e) {
                    throw Lombok.sneakyThrow(e);
                }
            });
        }

        @Override
        @SneakyThrows
        public void run() {
            this.server.start();
            this.closed.join();
        }
    }

    @Override
    public Integer call() throws Exception {
        final var serverConfig = (serverOptions.serverConfig != null)
                ? ServerConfig.readConfig(serverOptions.serverConfig.toURI().toURL())
                : ServerConfig.defaultConfig();

        final var clusterConfig = (this.clusterConfig != null)
                ? ClusterConfig.readConfig(this.clusterConfig.toURI().toURL())
                : ClusterConfig.defaultConfig();

        final var server = new ZeronosServer(serverConfig, clusterConfig, serverOptions.id);
        final var serverProcess = new ServerProcess(server);
        final var serverThread = new Thread(serverProcess);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            serverProcess.close.complete(null);
            serverProcess.closed.join();
        }));

        serverThread.start();
        serverThread.join();

        return 0;
    }

}
