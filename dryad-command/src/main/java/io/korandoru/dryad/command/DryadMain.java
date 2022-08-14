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

package io.korandoru.dryad.command;

import io.korandoru.dryad.core.config.ClusterConfig;
import io.korandoru.dryad.core.config.ServerConfig;
import io.korandoru.dryad.server.Server;
import java.io.File;
import java.util.Scanner;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
        name = "dryad",
        version = "0.1",
        mixinStandardHelpOptions = true
)
public class DryadMain implements Callable<Integer> {

    @CommandLine.Option(names = "--cluster-config")
    private File clusterConfig;

    @CommandLine.Option(names = "--server-config")
    private File serverConfig;

    @CommandLine.Option(names = "--id", required = true)
    private String id;

    @Override
    public Integer call() throws Exception {
        final var serverConfig = (this.serverConfig != null)
                ? ServerConfig.readConfig(this.serverConfig.toURI().toURL())
                : ServerConfig.defaultConfig();

        final var clusterConfig = (this.clusterConfig != null)
                ? ClusterConfig.readConfig(this.clusterConfig.toURI().toURL())
                : ClusterConfig.defaultConfig();

        final var server = new Server(serverConfig, clusterConfig, this.id);

        try (server) {
            server.start();
            // exit when any input entered
            new Scanner(System.in).nextLine();
        }
        return 0;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new DryadMain()).execute(args));
    }

}
