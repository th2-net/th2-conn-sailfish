/******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2.conn;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.exactpro.th2.conn.configuration.Configuration;

public class ConnectivityGrpsServer {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());
    private final Configuration configuration;
    private final BindableService bindableService;
    private volatile Server server;

    public ConnectivityGrpsServer(Configuration configuration, BindableService bindableService) {
        this.configuration = configuration;
        this.bindableService = bindableService;
    }

    public void start() throws IOException {
        if (server == null) {
            server = ServerBuilder.forPort(configuration.getPort())
                    .addService(bindableService)
                    .build()
                    .start();
            LOGGER.info("Server started, listening on port '{}'", configuration.getPort());
        } else {
            throw new IllegalStateException("gRPC server already started");
        }
    }

    /**
     * Shutdown gRPC server and await termination
     */
    public void stop() throws InterruptedException {
        if (server == null) {
            throw new IllegalStateException("gRPC server isn't started");
        }

        try {
            LOGGER.info("gRPC server shutdown started");
            if (!server.shutdown().awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.info("gRPC server force shutdown");
                server.shutdownNow();
            }
        } finally {
            LOGGER.info("gRPC server shutdown stoped");
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server == null) {
            throw new IllegalStateException("gRPC server isn't started");
        }
        server.awaitTermination();
    }
}
