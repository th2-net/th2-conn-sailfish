/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.connectivity.configuration;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import com.exactpro.th2.configuration.Th2Configuration;

public class Configuration {

    private static final String IN_QUEUE_NAME = "IN_QUEUE_NAME";
    private static final String IN_RAW_QUEUE_NAME = "IN_RAW_QUEUE_NAME";
    private static final String OUT_QUEUE_NAME = "OUT_QUEUE_NAME";
    private static final String OUT_RAW_QUEUE_NAME = "OUT_RAW_QUEUE_NAME";
    private static final String TO_SEND_QUEUE_NAME = "TO_SEND_QUEUE_NAME";
    private static final String TO_SEND_RAW_QUEUE_NAME = "TO_SEND_RAW_QUEUE_NAME";

    private final int port;
    private final String exchangeName;
    private final String sessionAlias;
    private final String inQueueName;
    private final String inRawQueueName;
    private final String outQueueName;
    private final String outRawQueueName;
    private final String toSendQueueName;
    private final String toSendRawQueueName;
    private final Th2Configuration th2Configuration;
    private String rabbitMQHost;
    private String rabbitMQVirtualHost;
    private int rabbitMQPort;
    private String rabbitMQUserName;
    private String rabbitMQPassword;

    public Configuration(int port, String sessionAlias, String exchangeName) {
        this.port = port;
        this.exchangeName = exchangeName;
        this.sessionAlias = sessionAlias;
        this.inQueueName = getEnvOrDefault(IN_QUEUE_NAME, this.sessionAlias + "_in");
        this.inRawQueueName = getEnvOrDefault(IN_RAW_QUEUE_NAME, this.sessionAlias + "_in_raw");
        this.outQueueName = getEnvOrDefault(OUT_QUEUE_NAME, this.sessionAlias + "_out");
        this.outRawQueueName = getEnvOrDefault(OUT_RAW_QUEUE_NAME, this.sessionAlias + "_out_raw");
        this.toSendQueueName = getEnvOrDefault(TO_SEND_QUEUE_NAME, this.sessionAlias + "_to_send");
        this.toSendRawQueueName = getEnvOrDefault(TO_SEND_RAW_QUEUE_NAME, this.sessionAlias + "_to_send_raw");
        this.th2Configuration = new Th2Configuration();
    }

    public int getPort() {
        return port;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public String getInQueueName() {
        return inQueueName;
    }

    public String getInRawQueueName() {
        return inRawQueueName;
    }

    public String getOutQueueName() {
        return outQueueName;
    }

    public String getOutRawQueueName() {
        return outRawQueueName;
    }

    public String getToSendQueueName() {
        return toSendQueueName;
    }

    public String getToSendRawQueueName() {
        return toSendRawQueueName;
    }

    public String getRabbitMQHost() {
        return rabbitMQHost;
    }

    public void setRabbitMQHost(String rabbitMQHost) {
        this.rabbitMQHost = rabbitMQHost;
    }

    public String getRabbitMQVirtualHost() {
        return rabbitMQVirtualHost;
    }

    public void setRabbitMQVirtualHost(String rabbitMQVirtualHost) {
        this.rabbitMQVirtualHost = rabbitMQVirtualHost;
    }

    public int getRabbitMQPort() {
        return rabbitMQPort;
    }

    public void setRabbitMQPort(int rabbitMQPort) {
        this.rabbitMQPort = rabbitMQPort;
    }

    public String getRabbitMQUserName() {
        return rabbitMQUserName;
    }

    public void setRabbitMQUserName(String rabbitMQUserName) {
        this.rabbitMQUserName = rabbitMQUserName;
    }

    public String getRabbitMQPassword() {
        return rabbitMQPassword;
    }

    public void setRabbitMQPassword(String rabbitMQPassword) {
        this.rabbitMQPassword = rabbitMQPassword;
    }

    public Th2Configuration getTh2Configuration() {
        return th2Configuration;
    }

    public static String getEnvSessionAlias() {
        return getEnvOrDefault("SESSION_ALIAS", "demo_fix_client_session");
    }

    private static String getEnvOrDefault(String parameterName, String defaultValue) {
        return defaultIfNull(getenv(parameterName), defaultValue);
    }
}
