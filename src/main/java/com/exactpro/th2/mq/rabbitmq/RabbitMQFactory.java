/*
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
 */
package com.exactpro.th2.mq.rabbitmq;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.mq.ExchnageType;
import com.exactpro.th2.mq.IMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.subscribers.DisposableSubscriber;

@SuppressWarnings("ClassNamePrefixedWithPackageName")
public class RabbitMQFactory implements IMQFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQFactory.class);

    //FIXME: Move to schema API
    private static final int COUNT_TRY_TO_CONNECT = 5;
    private static final int SHUTDOWN_TIMEOUT = 60_000;

    private final AtomicInteger count = new AtomicInteger(0);
    private final Connection connection;

    public RabbitMQFactory(String host, int port, String virtualHost, String username, String password) throws IOException, TimeoutException {
        ConnectionFactory factory = createConnectionFactory(
                requireNonNull(host, "Host can't be null"),
                port,
                requireNonNull(virtualHost, "VirtualHost can't be null"),
                requireNonNull(username, "Username can't be null"),
                requireNonNull(password, "Password can't be null"));

        //FIXME: Move to schema API
        factory.setAutomaticRecoveryEnabled(true);
        factory.setShutdownTimeout(SHUTDOWN_TIMEOUT);
        factory.setConnectionRecoveryTriggeringCondition(s -> {
            if (count.incrementAndGet() < COUNT_TRY_TO_CONNECT) {
                return true;
            }
            LOGGER.error("Can't connect to RabbitMQ. Count tries = {}", count.get());
            // TODO: we should stop the execution of the application. Don't use System.exit!!!
            return false;
        });


        connection = factory.newConnection();
    }

    @Override
    public Subscriber<byte[]> createBytesPublisher(String exchangeName, ExchnageType exchangeType, String routingKey) throws IOException {
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, toBuiltinExchangeType(requireNonNull(exchangeType, "Exchange type can't be null")));
        return new BytesPublisher(channel, exchangeName, requireNonNull(routingKey, "Routing key cna't be null"));
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    private static BuiltinExchangeType toBuiltinExchangeType(ExchnageType exchnageType) {
        switch (exchnageType) {
        case DIRECT:
            return BuiltinExchangeType.DIRECT;
        case FANOUT:
            return BuiltinExchangeType.FANOUT;
        case TOPIC:
            return BuiltinExchangeType.TOPIC;
        case HEADERS:
            return BuiltinExchangeType.HEADERS;
        default:
            throw new IllegalArgumentException("Unknown exchange type '" + exchnageType + '\'');
        }
    }

    private static ConnectionFactory createConnectionFactory(String host, int port, String virtualHost, String username, String password) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setVirtualHost(virtualHost);
        factory.setUsername(username);
        factory.setPassword(password);
        return factory;
    }

    private static class BytesPublisher extends DisposableSubscriber<byte[]> {

        private final Channel channel;
        private final String exchangeName;
        private final String routingKey;

        public BytesPublisher(Channel channel, String exchangeName, String routingKey) {
            this.channel = channel;
            this.exchangeName = exchangeName;
            this.routingKey = routingKey;
        }

        @Override
        protected void onStart() {
            LOGGER.info("Pipline is subscribed '{}'", this);
            super.onStart();
        }

        @SuppressWarnings("ParameterNameDiffersFromOverriddenParameter")
        @Override
        public void onNext(byte[] bytes) {
            try {
                channel.basicPublish(exchangeName, routingKey, null, bytes);
                LOGGER.debug("Data size '{}' is published to queue '{}:{}'", bytes.length, exchangeName, routingKey);
            } catch (IOException e) {
                LOGGER.error("Publication to RabbitMQ failure '{}'", this, e);
                Exceptions.propagate(e);
            }
        }

        @SuppressWarnings("ParameterNameDiffersFromOverriddenParameter")
        @Override
        public void onError(Throwable throwable) {
            LOGGER.error("Upstream threw error '{}'", this, throwable);
            close();
        }

        @Override
        public void onComplete() {
            LOGGER.info("Upstream is completed '{}'", this);
            close();
        }

        @Override
        public String toString() {
            return "BytesPublisher{" +
                    "exchangeName='" + exchangeName + '\'' +
                    ", routingKey='" + routingKey + '\'' +
                    '}';
        }

        private void close() {
            try {
                channel.close();
            } catch (IOException | TimeoutException e) {
                LOGGER.error("RabbitMQ channel closing failure", e);
                Exceptions.propagate(e);
            }
        }
    }
}
