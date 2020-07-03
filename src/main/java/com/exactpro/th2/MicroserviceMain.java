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
package com.exactpro.th2;

import static com.exactpro.cradle.messages.StoredMessageBatch.MAX_MESSAGES_COUNT;
import static com.exactpro.th2.configuration.Configuration.getEnvGRPCPort;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQHost;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPass;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPort;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQUser;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQVhost;
import static com.exactpro.th2.configuration.Th2Configuration.getEnvRabbitMQExchangeNameTH2Connectivity;
import static com.exactpro.th2.connectivity.configuration.Configuration.getEnvSessionAlias;
import static com.exactpro.th2.connectivity.utility.EventStoreExtensions.storeEvent;
import static com.exactpro.th2.connectivity.utility.MetadataProperty.PARENT_EVENT_ID;
import static com.exactpro.th2.connectivity.utility.SailfishMetadataExtensions.contains;
import static com.exactpro.th2.connectivity.utility.SailfishMetadataExtensions.getParentEventID;
import static io.grpc.ManagedChannelBuilder.forAddress;
import static org.apache.commons.lang.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.exactpro.sf.externalapi.*;
import com.exactpro.th2.infra.grpc.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.structures.IDictionaryStructure;
import com.exactpro.sf.configuration.suri.SailfishURI;
import com.exactpro.sf.externalapi.impl.ServiceFactoryException;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration;
import com.exactpro.th2.connectivity.configuration.Configuration;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceBlockingStub;
import com.exactpro.th2.mq.ExchnageType;
import com.exactpro.th2.mq.IMQFactory;
import com.exactpro.th2.mq.rabbitmq.RabbitMQFactory;
import com.google.protobuf.MessageLite;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.annotations.Nullable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.subscribers.DisposableSubscriber;

public class MicroserviceMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroserviceMain.class);
    public static final long NANOSECONDS_IN_SECOND = 1_000_000_000L;
    public static final String PASSWORD_PARAMETER = "password";
    public static final String DEFAULT_PASSWORD_PARAMETER = "default";

    /**
     * Configures External API Sailfish by passed service setting file
     * Runs configured service and listen gRPC requests
     * @param args:
     *  0 - path to workspace with Sailfish plugin
     *  1 - path to service settings file
     * Environment variables:
     *  {@link com.exactpro.th2.configuration.Configuration#ENV_GRPC_PORT}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_HOST}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PORT}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_USER}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PASS}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_VHOST}
     *  {@link Th2Configuration#ENV_RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY}
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Args is empty. Must have at least three parameters " +
                    "with path to workspace folder, service configuration");
        }

        File workspaceFolder = new File(args[0]);
        File servicePath = new File(workspaceFolder, args[1]);

        Map<Direction, AtomicLong> directionToSequence = getActualSequences();

        try {
            Configuration configuration = getConfiguration();
            IServiceFactory serviceFactory = new ServiceFactory(workspaceFolder);
            FlowableProcessor<ConnectivityMessage> processor = UnicastProcessor.create();

            Th2Configuration th2Configuration = configuration.getTh2Configuration();
            EventStoreServiceBlockingStub eventStoreConnector = EventStoreServiceGrpc.newBlockingStub(forAddress(
                    th2Configuration.getTh2EventStorageGRPCHost(), th2Configuration.getTh2EventStorageGRPCPort())
                    .usePlaintext().build());

            String rootEventID = storeEvent(eventStoreConnector, Event.start().endTimestamp()
                    .name("Connectivity '" + configuration.getSessionAlias() + "' " + Instant.now())
                    .type("Microservice")).getId();

            IServiceListener serviceListener = new ServiceListener(directionToSequence, new IMessageToProtoConverter(), configuration.getSessionAlias(), processor, eventStoreConnector, rootEventID);
            IServiceProxy serviceProxy = loadService(serviceFactory, servicePath, serviceListener);
            printServiceSetting(serviceProxy);
            IMessageFactoryProxy messageFactory = serviceFactory.getMessageFactory(serviceProxy.getType());
            SailfishURI dictionaryURI = serviceProxy.getSettings().getDictionary();
            IDictionaryStructure dictionary = serviceFactory.getDictionary(dictionaryURI);

            ConnectivityGrpsServer server = new ConnectivityGrpsServer(configuration,
                    new ConnectivityHandler(configuration));
            MessageSender messageSender = new MessageSender(serviceProxy, configuration, eventStoreConnector,
                    new ProtoToIMessageConverter(messageFactory, dictionary, dictionaryURI));

            configureShutdownHook(processor, serviceProxy, server, messageSender, serviceFactory);

            createPipeline(configuration, processor, eventStoreConnector)
                    .blockingSubscribe(new TermibnationSubscriber<>(serviceProxy, server, messageSender));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static void printServiceSetting(IServiceProxy serviceProxy) {
        ISettingsProxy settings = serviceProxy.getSettings();
        for (String parameterName : settings.getParameterNames()) {
            LOGGER.info("service setting '{}': '{}'", parameterName, getParamValue(settings, parameterName));
        }
    }

    private static Object getParamValue(ISettingsProxy settings, String parameterName) {
        Object parameterValue = settings.getParameterValue(parameterName);
        if (containsIgnoreCase(parameterName, PASSWORD_PARAMETER)) {
            return repeat("*", defaultIfNull(parameterValue, DEFAULT_PASSWORD_PARAMETER).toString().length());
        }
        return parameterValue;
    }

    private static @NonNull Flowable<ConnectableFlowable<ConnectivityMessage>> createPipeline(Configuration configuration,
            Flowable<ConnectivityMessage> processor, EventStoreServiceBlockingStub eventStoreConnector) throws IOException, TimeoutException {
        LOGGER.info("AvailableProcessors '{}'", Runtime.getRuntime().availableProcessors());

        return processor.groupBy(ConnectivityMessage::getDirection)
                .map(group -> {
                    @Nullable Direction direction = group.getKey();
                    ConnectableFlowable<ConnectivityMessage> messageConnectable = group.publish();

                    if (direction == Direction.SECOND) {
                        subscribeToSendMessage(eventStoreConnector, direction, messageConnectable);
                    }
                    createPackAndPublishPipeline(configuration, direction, messageConnectable);

                    messageConnectable.connect();

                    return messageConnectable;
                });
    }

    private static void subscribeToSendMessage(EventStoreServiceBlockingStub eventStoreConnector, @Nullable Direction direction, Flowable<ConnectivityMessage> messageConnectable) {
        messageConnectable.filter(message -> contains(message.getiMessage().getMetaData(), PARENT_EVENT_ID))
                .subscribe(message -> {
                    storeEvent(eventStoreConnector, Event.start().endTimestamp()
                            .name("Send '" + message.getiMessage().getName() + "' message")
                            .type("Send message")
                            .messageID(message.getMessageID()), getParentEventID(message.getiMessage().getMetaData()).getId());
                });
    }

    private static void createPackAndPublishPipeline(Configuration configuration, @Nullable Direction direction, ConnectableFlowable<ConnectivityMessage> messageConnectable) throws IOException, TimeoutException {
        Map<ConnectionType, String> routingKeyMap = createRoutingKeyMap(configuration);
        IMQFactory mqFactory = new RabbitMQFactory(
                configuration.getRabbitMQHost(),
                configuration.getRabbitMQPort(),
                configuration.getRabbitMQVirtualHost(),
                configuration.getRabbitMQUserName(),
                configuration.getRabbitMQPassword());
        String exchangeName = configuration.getExchangeName();

        LOGGER.info("Map group {}", direction);
        ConnectableFlowable<ConnectivityBatch> batchConnectable = messageConnectable
                .window(1, TimeUnit.SECONDS, MAX_MESSAGES_COUNT)
                .flatMap(msgs -> msgs.toList().toFlowable())
                .filter(list -> !list.isEmpty())
                .map(ConnectivityBatch::new)
                .publish();

        subscribeToPackAndPublish(routingKeyMap, batchConnectable.map(ConnectivityBatch::convertToProtoRawBatch), mqFactory,
                direction, exchangeName, ContentType.RAW,
                batch -> batch.getMessagesList().get(0).getMetadata().getId(),
                RawMessageBatch::getMessagesCount);
        LOGGER.info("Subscribed to transfer raw batch group {}", direction);

        subscribeToPackAndPublish(routingKeyMap, batchConnectable.map(ConnectivityBatch::convertToProtoParsedBatch), mqFactory,
                direction, exchangeName, ContentType.PARSED,
                batch -> batch.getMessagesList().get(0).getMetadata().getId(),
                MessageBatch::getMessagesCount);
        LOGGER.info("Subscribed to transfer parsed batch group {}", direction);

        batchConnectable.connect();
        LOGGER.info("Connected to publish batches group {}", direction);
    }

    private static <T extends MessageLite> void subscribeToPackAndPublish(Map<ConnectionType, String> routingKeyMap, Flowable<T> batchConnectable,
            IMQFactory mqFactory, Direction direction, String exchangeName, ContentType contentType, Function<T, MessageID> extractMessageID, Function<T, Integer> extractSize) throws IOException {
        String routingKeyParsed = routingKeyMap.get(new ConnectionType(direction, contentType));
        batchConnectable.doOnNext(batch -> {
            MessageID messageID = extractMessageID.apply(batch);
            LOGGER.debug("The '{}' batch seq '{}:{}:{}' size '{}' is transfered to queue '{}:{}'",
                    contentType, messageID.getConnectionId().getSessionAlias(), messageID.getDirection(), messageID.getSequence(),
                    extractSize.apply(batch), exchangeName, routingKeyParsed);
        })
                .map(MessageLite::toByteArray)
                .subscribe(mqFactory.createBytesPublisher(exchangeName, ExchnageType.DIRECT,
                        routingKeyParsed));
        LOGGER.info("Subscribed to transfer '{}' batch group {}", contentType, direction);
    }

    private static void configureShutdownHook(Subscriber<ConnectivityMessage> subscriber, IServiceProxy serviceProxy, ConnectivityGrpsServer server, MessageSender messageSender,
            AutoCloseable serviceFactory) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                try {
                    server.stop();
                } finally {
                    try {
                        messageSender.stop();
                    } catch (IOException e) {
                        LOGGER.error("Message sender stopping is failure", e);
                    } finally {
                        try {
                            LOGGER.info("Stop internal service");
                            serviceProxy.stop();
                        } finally {
                            try {
                                LOGGER.info("Stop service factory");
                                serviceFactory.close();
                            } catch (Exception e) {
                                LOGGER.error("Service factory closing is failure", e);
                            } finally {
                                subscriber.onComplete();
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error("Shutdown hook interrupted", e);
            } catch (RuntimeException e) {
                LOGGER.error("Shutdown hook failure", e);
            }
        }, "Shutdown hook"));
    }

    @NotNull
    private static Map<ConnectionType, String> createRoutingKeyMap(Configuration configuration) {
        return Map.of(
                new ConnectionType(Direction.FIRST, ContentType.RAW), configuration.getInRawQueueName(),
                new ConnectionType(Direction.FIRST, ContentType.PARSED), configuration.getInQueueName(),
                new ConnectionType(Direction.SECOND, ContentType.RAW), configuration.getOutRawQueueName(),
                new ConnectionType(Direction.SECOND, ContentType.PARSED), configuration.getOutQueueName());
    }

    // FIXME: Request to a disctinct service to get actual values
    private static Map<Direction, AtomicLong> getActualSequences() {
        return Map.copyOf(Stream.of(Direction.values())
                .collect(Collectors.toMap(Function.identity(), direction -> new AtomicLong(getLastSequence()))));
    }

    //FIXME: This values should be got from memcache
    private static long getLastSequence() {
        Instant now = Instant.now();
        return now.getEpochSecond() * NANOSECONDS_IN_SECOND + now.getNano();
    }

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration(getEnvGRPCPort(),
                getEnvSessionAlias(), getEnvRabbitMQExchangeNameTH2Connectivity());
        configuration.setRabbitMQHost(getEnvRabbitMQHost());
        configuration.setRabbitMQVirtualHost(getEnvRabbitMQVhost());
        configuration.setRabbitMQPort(getEnvRabbitMQPort());
        configuration.setRabbitMQUserName(getEnvRabbitMQUser());
        configuration.setRabbitMQPassword(getEnvRabbitMQPass());
        return configuration;
    }

    private static IServiceProxy loadService(IServiceFactory serviceFactory, File servicePath, IServiceListener serviceListener) {
        try (InputStream serviceStream = new FileInputStream(servicePath)) {
            return serviceFactory.createService(serviceStream, serviceListener);
        } catch (IOException | ServiceFactoryException e) {
            throw new RuntimeException(String.format("Could not import service %s", servicePath), e);
        }
    }

    @SuppressWarnings("ParameterNameDiffersFromOverriddenParameter")
    private static class TermibnationSubscriber<T> extends DisposableSubscriber<T> {

        private final IServiceProxy serviceProxy;
        private final ConnectivityGrpsServer server;
        private final MessageSender messageSender;

        public TermibnationSubscriber(IServiceProxy serviceProxy, ConnectivityGrpsServer server, MessageSender messageSender) {
            this.serviceProxy = serviceProxy;
            this.server = server;
            this.messageSender = messageSender;
        }

        @Override
        protected void onStart() {
            super.onStart();
            try {
                LOGGER.info("Subscribed to pipeline");
                serviceProxy.start();
                server.start();
                messageSender.start();
            } catch (IOException | TimeoutException e) {
                LOGGER.error("Services starting failure", e);
                Exceptions.propagate(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            LOGGER.error("Upstream threw error", throwable);
        }

        @Override
        public void onComplete() {
            LOGGER.info("Upstream is completed");
        }

        @Override
        public void onNext(T object) {
            // Do nothing
        }
    }

    private static class ConnectionType {
        private final Direction direction;
        private final ContentType contentType;

        private ConnectionType(Direction direction, ContentType contentType) {
            this.direction = direction;
            this.contentType = contentType;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            ConnectionType another = (ConnectionType)obj;

            return new EqualsBuilder()
                    .append(direction, another.direction)
                    .append(contentType, another.contentType)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(direction)
                    .append(contentType)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("direction", direction)
                    .append("contentType", contentType)
                    .toString();
        }
    }

    private enum ContentType {
        RAW,
        PARSED
    }
}
