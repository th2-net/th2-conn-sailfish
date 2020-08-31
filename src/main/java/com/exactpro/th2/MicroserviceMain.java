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
import static io.reactivex.rxjava3.plugins.RxJavaPlugins.createSingleScheduler;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.structures.IDictionaryStructure;
import com.exactpro.sf.configuration.suri.SailfishURI;
import com.exactpro.sf.configuration.suri.SailfishURIException;
import com.exactpro.sf.configuration.workspace.WorkspaceSecurityException;
import com.exactpro.sf.externalapi.IMessageFactoryProxy;
import com.exactpro.sf.externalapi.IServiceFactory;
import com.exactpro.sf.externalapi.IServiceListener;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.sf.externalapi.ISettingsProxy;
import com.exactpro.sf.externalapi.ServiceFactory;
import com.exactpro.sf.externalapi.impl.ServiceFactoryException;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration;
import com.exactpro.th2.connectivity.configuration.Configuration;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceBlockingStub;
import com.exactpro.th2.infra.grpc.Direction;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.mq.ExchnageType;
import com.exactpro.th2.mq.IMQFactory;
import com.exactpro.th2.mq.rabbitmq.RabbitMQFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.MessageLite;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
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

    private static final Scheduler PIPELINE_SCHEDULER = createSingleScheduler(new ThreadFactoryBuilder()
            .setNameFormat("Pipeline-%d").build());

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
        Disposer disposer = new Disposer();
        Runtime.getRuntime().addShutdownHook(new Thread(disposer::dispose, "Shutdown hook"));
        int exitCode = 0;
        try {
            if (args.length < 2) {
                LOGGER.error("Args is empty. Must have at least three parameters " +
                        "with path to workspace folder, service configuration");
                exitCode = 1;
                return;
            }

            File workspaceFolder = new File(args[0]);
            File servicePath = new File(workspaceFolder, args[1]);

            Map<Direction, AtomicLong> directionToSequence = getActualSequences();

            disposer.register(() -> {
                LOGGER.info("Shutdown pipeline scheduler");
                PIPELINE_SCHEDULER.shutdown();
            });

            Configuration configuration = getConfiguration();
            FlowableProcessor<ConnectivityMessage> processor = UnicastProcessor.create();
            disposer.register(() -> {
                LOGGER.info("Complite pipeline publisher");
                processor.onComplete();
            });

            Th2Configuration th2Configuration = configuration.getTh2Configuration();
            EventStoreServiceBlockingStub eventStoreConnector = EventStoreServiceGrpc.newBlockingStub(forAddress(
                    th2Configuration.getTh2EventStorageGRPCHost(), th2Configuration.getTh2EventStorageGRPCPort())
                    .usePlaintext().build());

            Event rootEvent = Event.start().endTimestamp()
                    .name("Connectivity '" + configuration.getSessionAlias() + "' " + Instant.now())
                    .type("Microservice");
            String rootEventID = rootEvent.getId();

            storeEvent(eventStoreConnector, rootEvent);

            IServiceFactory serviceFactory = new ServiceFactory(workspaceFolder);
            disposer.register(() -> {
                LOGGER.info("Close service factory");
                serviceFactory.close();
            });
            IServiceListener serviceListener = new ServiceListener(directionToSequence, new IMessageToProtoConverter(), configuration.getSessionAlias(), processor, eventStoreConnector, rootEventID);
            IServiceProxy serviceProxy = loadService(serviceFactory, servicePath, serviceListener);
            disposer.register(() -> {
                LOGGER.info("Stop service proxy");
                serviceProxy.stop();
            });
            printServiceSetting(serviceProxy);
            IMessageFactoryProxy messageFactory = serviceFactory.getMessageFactory(serviceProxy.getType());
            SailfishURI dictionaryURI = serviceProxy.getSettings().getDictionary();
            IDictionaryStructure dictionary = serviceFactory.getDictionary(dictionaryURI);

            MessageSender messageSender = new MessageSender(serviceProxy, configuration, eventStoreConnector,
                    new ProtoToIMessageConverter(messageFactory, dictionary, dictionaryURI));
            disposer.register(() -> {
                LOGGER.info("Stop 'message send' listener");
                messageSender.stop();
            });
            ConnectivityGrpsServer server = new ConnectivityGrpsServer(configuration,
                    new ConnectivityHandler(configuration));
            disposer.register(() -> {
                LOGGER.info("Stop gRPC server");
                server.stop();
            });

            createPipeline(configuration, processor, eventStoreConnector)
                    .blockingSubscribe(new TermibnationSubscriber<>(serviceProxy, server, messageSender));
        } catch (SailfishURIException | WorkspaceSecurityException e) { LOGGER.error(e.getMessage(), e); exitCode = 2;
        } catch (IOException e) { LOGGER.error(e.getMessage(), e); exitCode = 3;
        } catch (IllegalArgumentException e) { LOGGER.error(e.getMessage(), e); exitCode = 4;
        } catch (RuntimeException e) { LOGGER.error(e.getMessage(), e); exitCode = 5;
        } finally {
            System.exit(exitCode); // Initiate close JVM with all resource leaks.
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

    private static @NonNull Flowable<Flowable<ConnectivityMessage>> createPipeline(Configuration configuration,
            FlowableProcessor<ConnectivityMessage> processor, EventStoreServiceBlockingStub eventStoreConnector) {
        LOGGER.info("AvailableProcessors '{}'", Runtime.getRuntime().availableProcessors());

        return processor.observeOn(PIPELINE_SCHEDULER)
                .doOnNext(msg -> LOGGER.debug("Start handling message with sequence {}", msg.getSequence()))
                .groupBy(ConnectivityMessage::getDirection)
                .map(group -> {
                    @NonNull Direction direction = requireNonNull(group.getKey(), "Direction can't be null");
                    Flowable<ConnectivityMessage> messageConnectable = group
                            .doOnCancel(processor::onComplete)
                            .publish()
                            .refCount(direction == Direction.SECOND ? 2 : 1);

                    if (direction == Direction.SECOND) {
                        subscribeToSendMessage(eventStoreConnector, messageConnectable);
                    }
                    createPackAndPublishPipeline(configuration, direction, messageConnectable);

                    return messageConnectable;
                });
    }

    private static void subscribeToSendMessage(EventStoreServiceBlockingStub eventStoreConnector, Flowable<ConnectivityMessage> messageConnectable) {
        //noinspection ResultOfMethodCallIgnored
        messageConnectable.filter(message -> contains(message.getSailfishMessage().getMetaData(), PARENT_EVENT_ID))
                .subscribe(message -> {
                    Event event = Event.start().endTimestamp()
                            .name("Send '" + message.getSailfishMessage().getName() + "' message")
                            .type("Send message")
                            .messageID(message.getMessageID());
                    LOGGER.debug("Sending event {} related to message with sequence {}", event.getId(), message.getSequence());
                    storeEvent(eventStoreConnector, event, getParentEventID(message.getSailfishMessage().getMetaData()).getId());
                });
    }

    private static void createPackAndPublishPipeline(Configuration configuration, @NotNull Direction direction, Flowable<ConnectivityMessage> messageConnectable) throws IOException, TimeoutException {
        Map<ConnectionType, String> routingKeyMap = createRoutingKeyMap(configuration);
        IMQFactory mqFactory = new RabbitMQFactory(
                configuration.getRabbitMQHost(),
                configuration.getRabbitMQPort(),
                configuration.getRabbitMQVirtualHost(),
                configuration.getRabbitMQUserName(),
                configuration.getRabbitMQPassword());
        String exchangeName = configuration.getExchangeName();

        LOGGER.info("Map group {}", direction);
        Flowable<ConnectivityBatch> batchConnectable = messageConnectable
                .window(1, TimeUnit.SECONDS, PIPELINE_SCHEDULER, MAX_MESSAGES_COUNT)
                .concatMapSingle(Flowable::toList)
                .filter(list -> !list.isEmpty())
                .map(ConnectivityBatch::new)
                .doOnNext(batch -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Batch {}:{} is created", batch.getSequence(), batch.getMessages().stream()
                                .map(ConnectivityMessage::getSequence)
                                .collect(Collectors.toList()));
                    }
                })
                .publish()
                .refCount(2);

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

    private interface Disposable {
        void dispose() throws Exception;
    }

    private static class Disposer {

        private final Deque<Disposable> disposableQueue = new ConcurrentLinkedDeque<>();

        public void register(Disposable disposable) {
            disposableQueue.push(disposable);
        }
        /**
         * Disposes registered resources in LIFO order.
         */
        public void dispose() {
            LOGGER.info("Disposing ...");

            for (Disposable disposable : disposableQueue) {
                try {
                    disposable.dispose();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }

            LOGGER.info("Disposed");
        }
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
