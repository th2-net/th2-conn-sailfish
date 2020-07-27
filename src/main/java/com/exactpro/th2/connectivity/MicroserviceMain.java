/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.connectivity;

import static com.exactpro.cradle.messages.StoredMessageBatch.MAX_MESSAGES_COUNT;
import static com.exactpro.th2.connectivity.utility.EventStoreExtensions.storeEvent;
import static com.exactpro.th2.connectivity.utility.MetadataProperty.PARENT_EVENT_ID;
import static com.exactpro.th2.connectivity.utility.SailfishMetadataExtensions.contains;
import static com.exactpro.th2.connectivity.utility.SailfishMetadataExtensions.getParentEventID;
import static org.apache.commons.lang.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.exactpro.sf.common.services.ServiceName;
import com.exactpro.sf.comparison.conversion.ConversionException;
import com.exactpro.sf.comparison.conversion.MultiConverter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ClassUtils;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.structures.IDictionaryStructure;
import com.exactpro.sf.configuration.suri.SailfishURI;
import com.exactpro.sf.externalapi.IMessageFactoryProxy;
import com.exactpro.sf.externalapi.IServiceFactory;
import com.exactpro.sf.externalapi.IServiceListener;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.sf.externalapi.ISettingsProxy;
import com.exactpro.sf.externalapi.ServiceFactory;
import com.exactpro.sf.externalapi.impl.ServiceFactoryException;
import com.exactpro.th2.IMessageToProtoConverter;
import com.exactpro.th2.ProtoToIMessageConverter;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration;
import com.exactpro.th2.connectivity.configuration.ConnectivityConfiguration;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceService;
import com.exactpro.th2.infra.grpc.Direction;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.schema.factory.CommonFactory;
import com.exactpro.th2.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.schema.message.MessageRouter;

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
        CommonFactory factory;
        try {
            factory = CommonFactory.createFromArguments(args);
        } catch (ParseException e) {
            factory = new CommonFactory();
            LOGGER.warn("Can not create common factory from arguments");
        }
        ConnectivityConfiguration configuration = factory.getCustomConfiguration(ConnectivityConfiguration.class);

        File workspaceFolder = new File(configuration.getWorkspaceFolder());

        Map<Direction, AtomicLong> directionToSequence = getActualSequences();

        try {
            GrpcRouter grpcRouter = factory.getGrpcRouter();
            MessageRouter<MessageBatch> parsedMessageBatch = (MessageRouter<MessageBatch>) factory.getMessageRouterParsedBatch();

            IServiceFactory serviceFactory = new ServiceFactory(workspaceFolder);
            FlowableProcessor<ConnectivityMessage> processor = UnicastProcessor.create();

            EventStoreServiceService eventStore = grpcRouter.getService(EventStoreServiceService.class);

            String rootEventID = storeEvent(eventStore, Event.start().endTimestamp()
                    .name("Connectivity '" + configuration.getSessionAlias() + "' " + Instant.now())
                    .type("Microservice")).getId();

            IServiceListener serviceListener = new ServiceListener(directionToSequence, new IMessageToProtoConverter(), configuration.getSessionAlias(), processor, eventStore, rootEventID);
            IServiceProxy serviceProxy = loadService(serviceFactory, configuration, serviceListener);
            printServiceSetting(serviceProxy);
            IMessageFactoryProxy messageFactory = serviceFactory.getMessageFactory(serviceProxy.getType());
            SailfishURI dictionaryURI = serviceProxy.getSettings().getDictionary();
            IDictionaryStructure dictionary = serviceFactory.getDictionary(dictionaryURI);

            MessageSender messageSender = new MessageSender(serviceProxy, new ProtoToIMessageConverter(messageFactory, dictionary, dictionaryURI), parsedMessageBatch);

            configureShutdownHook(processor, serviceProxy, messageSender, serviceFactory);

            createPipeline(configuration, processor, eventStore, parsedMessageBatch, (MessageRouter<RawMessageBatch>) factory.getMessageRouterRawBatch())
                    .blockingSubscribe(new TermibnationSubscriber<>(serviceProxy, messageSender));
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

    private static @NonNull Flowable<ConnectableFlowable<ConnectivityMessage>> createPipeline(ConnectivityConfiguration configuration,
            Flowable<ConnectivityMessage> processor, EventStoreServiceService eventStoreConnector,
            MessageRouter<MessageBatch> parsedMessageRouter, MessageRouter<RawMessageBatch> rawMessageRouter) throws IOException, TimeoutException {
        LOGGER.info("AvailableProcessors '{}'", Runtime.getRuntime().availableProcessors());

        return processor.groupBy(ConnectivityMessage::getDirection)
                .map(group -> {
                    @Nullable Direction direction = group.getKey();
                    ConnectableFlowable<ConnectivityMessage> messageConnectable = group.publish();

                    if (direction == Direction.SECOND) {
                        subscribeToSendMessage(eventStoreConnector, direction, messageConnectable);
                    }
                    createPackAndPublishPipeline(direction, messageConnectable, parsedMessageRouter, rawMessageRouter);

                    messageConnectable.connect();

                    return messageConnectable;
                });
    }

    private static void subscribeToSendMessage(EventStoreServiceService eventStoreConnector, @Nullable Direction direction, Flowable<ConnectivityMessage> messageConnectable) {
        messageConnectable.filter(message -> contains(message.getiMessage().getMetaData(), PARENT_EVENT_ID))
                .subscribe(message -> {
                    storeEvent(eventStoreConnector, Event.start().endTimestamp()
                            .name("Send '" + message.getiMessage().getName() + "' message")
                            .type("Send message")
                            .messageID(message.getMessageID()), getParentEventID(message.getiMessage().getMetaData()).getId());
                });
    }

    private static void createPackAndPublishPipeline(@Nullable Direction direction, ConnectableFlowable<ConnectivityMessage> messageConnectable,
            MessageRouter<MessageBatch> parsedMessageRouter, MessageRouter<RawMessageBatch> rawMessageRouter) throws IOException, TimeoutException {

        LOGGER.info("Map group {}", direction);
        ConnectableFlowable<ConnectivityBatch> batchConnectable = messageConnectable
                .window(1, TimeUnit.SECONDS, MAX_MESSAGES_COUNT)
                .flatMap(msgs -> msgs.toList().toFlowable())
                .filter(list -> !list.isEmpty())
                .map(ConnectivityBatch::new)
                .publish();

        batchConnectable.map(ConnectivityBatch::convertToProtoRawBatch).subscribe(it -> rawMessageRouter.send(it, direction == Direction.FIRST ? "in" : "out", "raw"));
        LOGGER.info("Subscribed to transfer raw batch group {}", direction);

        batchConnectable.map(ConnectivityBatch::convertToProtoParsedBatch).subscribe(it -> parsedMessageRouter.send(it, direction == Direction.FIRST ? "in" : "out", "parsed"));
        LOGGER.info("Subscribed to transfer parsed batch group {}", direction);

        batchConnectable.connect();
        LOGGER.info("Connected to publish batches group {}", direction);
    }

    private static void configureShutdownHook(Subscriber<ConnectivityMessage> subscriber, IServiceProxy serviceProxy, MessageSender messageSender,
            AutoCloseable serviceFactory) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
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
            } catch (RuntimeException e) {
                LOGGER.error("Shutdown hook failure", e);
            }
        }, "Shutdown hook"));
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

    private static IServiceProxy loadService(IServiceFactory serviceFactory,
                                             ConnectivityConfiguration configuration,
                                             IServiceListener serviceListener) {
        try  {
            IServiceProxy service = serviceFactory.createService(ServiceName.parse(configuration.getName()),
                    SailfishURI.unsafeParse(configuration.getType()),
                    serviceListener);
            ISettingsProxy settings = service.getSettings();
            for (Entry<String, Object> settingsEntry : configuration.getSettings().entrySet()) {
                String settingName = settingsEntry.getKey();
                Object castValue = castValue(settings, settingName, settingsEntry.getValue());
                settings.setParameterValue(settingName, castValue);
            }
            return service;
        } catch (ConversionException | ServiceFactoryException e) {
            throw new RuntimeException(String.format("Could not load service '%s'", configuration.getName()), e);
        }
    }

    private static Object castValue(ISettingsProxy settings, String settingName, Object value) {
        Class<?> settingType = primitiveToWrapper(settings.getParameterType(settingName));
        if (SailfishURI.class.isAssignableFrom(settingType)) {
            return SailfishURI.unsafeParse(value.toString());
        }
        if (MultiConverter.SUPPORTED_TYPES.contains(settingType)) {
            return MultiConverter.convert(value, settingType);
        }
        return value;
    }

    @SuppressWarnings("ParameterNameDiffersFromOverriddenParameter")
    private static class TermibnationSubscriber<T> extends DisposableSubscriber<T> {

        private final IServiceProxy serviceProxy;
        private final MessageSender messageSender;

        public TermibnationSubscriber(IServiceProxy serviceProxy, MessageSender messageSender) {
            this.serviceProxy = serviceProxy;
            this.messageSender = messageSender;
        }

        @Override
        protected void onStart() {
            super.onStart();
            try {
                LOGGER.info("Subscribed to pipeline");
                serviceProxy.start();
                messageSender.start();
            } catch (Exception e) {
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
}
