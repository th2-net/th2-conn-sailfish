/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.conn;

import static com.exactpro.th2.conn.utility.EventStoreExtensions.storeEvent;
import static com.exactpro.th2.conn.utility.MetadataProperty.PARENT_EVENT_ID;
import static com.exactpro.th2.conn.utility.SailfishMetadataExtensions.contains;
import static com.exactpro.th2.conn.utility.SailfishMetadataExtensions.getParentEventID;
import static io.reactivex.rxjava3.plugins.RxJavaPlugins.createSingleScheduler;
import static org.apache.commons.lang.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.services.ServiceName;
import com.exactpro.sf.comparison.conversion.ConversionException;
import com.exactpro.sf.comparison.conversion.MultiConverter;
import com.exactpro.sf.configuration.suri.SailfishURI;
import com.exactpro.sf.configuration.suri.SailfishURIException;
import com.exactpro.sf.configuration.suri.SailfishURIUtils;
import com.exactpro.sf.configuration.workspace.WorkspaceSecurityException;
import com.exactpro.sf.externalapi.DictionaryType;
import com.exactpro.sf.externalapi.IServiceFactory;
import com.exactpro.sf.externalapi.IServiceListener;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.sf.externalapi.ISettingsProxy;
import com.exactpro.sf.externalapi.ServiceFactory;
import com.exactpro.sf.externalapi.impl.ServiceFactoryException;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.conn.configuration.ConnectivityConfiguration;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventType;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.subscribers.DisposableSubscriber;

public class MicroserviceMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroserviceMain.class);

    public static final int MAX_MESSAGES_COUNT = 100;
    public static final long NANOSECONDS_IN_SECOND = 1_000_000_000L;
    public static final String PASSWORD_PARAMETER = "password";
    public static final String DEFAULT_PASSWORD_PARAMETER = "default";

    private static final Scheduler PIPELINE_SCHEDULER = createSingleScheduler(new ThreadFactoryBuilder()
            .setNameFormat("Pipeline-%d").build());

    public static void main(String[] args) {
        Disposer disposer = new Disposer();
        Runtime.getRuntime().addShutdownHook(new Thread(disposer::dispose, "Shutdown hook"));
        CommonFactory factory;
        try {
            factory = CommonFactory.createFromArguments(args);
        } catch (RuntimeException e) {
            factory = new CommonFactory();
            LOGGER.warn("Can not create common factory from arguments");
        }

        // just to use in lambda
        CommonFactory finalFactory = factory;
        disposer.register(() -> {
            LOGGER.info("Closing factory");
            finalFactory.close();
        });

        ConnectivityConfiguration configuration = factory.getCustomConfiguration(ConnectivityConfiguration.class);

        File workspaceFolder = new File(configuration.getWorkspaceFolder());

        Map<Direction, AtomicLong> directionToSequence = getActualSequences();
        int exitCode = 0;

        try {
            disposer.register(() -> {
                LOGGER.info("Shutdown pipeline scheduler");
                PIPELINE_SCHEDULER.shutdown();
            });

            FlowableProcessor<ConnectivityMessage> processor = UnicastProcessor.create();
            disposer.register(() -> {
                LOGGER.info("Complite pipeline publisher");
                processor.onComplete();
            });

            IServiceFactory serviceFactory = new ServiceFactory(workspaceFolder,
                    Files.createTempDirectory("sailfish-workspace").toFile());
            disposer.register(() -> {
                LOGGER.info("Close service factory");
                serviceFactory.close();
            });

            MessageRouter<EventBatch> eventBatchRouter = factory.getEventBatchRouter();

            var rootEvent = Event.start().endTimestamp()
                    .name("Connectivity '" + configuration.getSessionAlias() + "' " + Instant.now())
                    .type("Microservice");
            String rootEventID = storeEvent(eventBatchRouter, rootEvent).getId();

            var errorEventsRoot = Event.start().endTimestamp()
                    .name("Errors")
                    .type("ConnectivityErrors");
            storeEvent(eventBatchRouter, errorEventsRoot, rootEventID);

            var serviceEventsRoot = Event.start().endTimestamp()
                    .name("ServiceEvents")
                    .type("ConnectivityServiceEvents");
            storeEvent(eventBatchRouter, serviceEventsRoot, rootEventID);

            var untrackedSentMessages = Event.start().endTimestamp()
                    .name("UntrackedMessages")
                    .description("Contains messages that we send via this connectivity but does not have attacked parent event ID")
                    .type("ConnectivityUntrackedMessages");
            storeEvent(eventBatchRouter, serviceEventsRoot, rootEventID);

            var eventDispatcher = EventDispatcher.createDispatcher(eventBatchRouter, rootEventID, Map.of(
                    EventType.ERROR, errorEventsRoot.getId(),
                    EventType.SERVICE_EVENT, serviceEventsRoot.getId()
            ));

            IServiceListener serviceListener = new ServiceListener(directionToSequence, configuration.getSessionAlias(), processor, eventDispatcher, configuration.getSessionGroup());
            IServiceProxy serviceProxy = loadService(serviceFactory, factory, configuration, serviceListener);
            disposer.register(() -> {
                LOGGER.info("Stop service proxy");
                serviceProxy.stop();
            });
            printServiceSetting(serviceProxy);

            MessageRouter<RawMessageBatch> rawMessageRouter = factory.getMessageRouterRawBatch();

            MessageSender messageSender = new MessageSender(serviceProxy, rawMessageRouter, eventDispatcher,
                    EventID.newBuilder().setId(untrackedSentMessages.getId()).build()
            );
            disposer.register(() -> {
                LOGGER.info("Stop 'message send' listener");
                messageSender.stop();
            });

            createPipeline(processor, processor::onComplete, eventBatchRouter, rawMessageRouter,
                    configuration.getMaxMessageBatchSize(), configuration.isEnableMessageSendingEvent())
                    .blockingSubscribe(new TermibnationSubscriber<>(serviceProxy, messageSender));
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

    private static @NonNull Flowable<ConnectivityMessage> createPipeline(
            Flowable<ConnectivityMessage> flowable, Action terminateFlowable,
            MessageRouter<EventBatch> eventBatchRouter,
            MessageRouter<RawMessageBatch> rawMessageRouter,
            int maxMessageBatchSize, boolean enableMessageSendingEvent) {
        LOGGER.info("AvailableProcessors '{}'", Runtime.getRuntime().availableProcessors());

		ConnectableFlowable<ConnectivityMessage> messageConnectable = flowable
				.doOnNext(message -> {
					LOGGER.trace(
							"Message before observeOn with sequence {} and direction {}",
							message.getSequence(),
							message.getDirection()
					);
				})
				.observeOn(PIPELINE_SCHEDULER)
				.doOnNext(connectivityMessage -> {
					LOGGER.debug("Start handling connectivity message {}", connectivityMessage);
					LOGGER.trace(
							"Message inside map with sequence {} and direction {}",
							connectivityMessage.getSequence(),
							connectivityMessage.getDirection());
				})
				.doOnCancel(terminateFlowable) // This call is required for terminate the publisher and prevent creation another group
				.publish();

		subscribeToSendMessage(eventBatchRouter, messageConnectable);

		createPackAndPublishPipeline(messageConnectable, rawMessageRouter, maxMessageBatchSize);

        messageConnectable.connect();

		return messageConnectable;
    }

    private static void subscribeToSendMessage(MessageRouter<EventBatch> eventBatchRouter, Flowable<ConnectivityMessage> messageConnectable) {
        //noinspection ResultOfMethodCallIgnored
        messageConnectable
                .subscribe(connectivityMessage -> {
                    // There should be only a single message
                    // because we are subscribed on a SECOND direction.
                    // Sailfish does not support sending multiple messages at once.
                    // So we should send only a single event here.
                    // But just in case we are wrong, we add checking for sending multiple events
                    if (connectivityMessage.getDirection() != Direction.SECOND) {
                        return;
                    }
                    boolean sent = false;
                    for (IMessage message : connectivityMessage.getSailfishMessages()) {
                        if (!contains(message.getMetaData(), PARENT_EVENT_ID)) {
                            continue;
                        }
                        if (sent) {
                            LOGGER.warn("The connectivity message has more than one sailfish message with parent event ID: {}", connectivityMessage);
                        }
                        Event event = Event.start().endTimestamp()
                                .name("Send '" + message.getName() + "' message")
                                .type("Send message")
                                .messageID(connectivityMessage.getMessageID());
                        LOGGER.debug("Sending event {} related to message with sequence {}", event.getId(), connectivityMessage.getSequence());
                        storeEvent(eventBatchRouter, event, getParentEventID(message.getMetaData()).getId());
                        sent = true;
                    }
                });
    }

    private static void createPackAndPublishPipeline(Flowable<ConnectivityMessage> messageConnectable,
            MessageRouter<RawMessageBatch> rawMessageRouter, int maxMessageBatchSize) {

        messageConnectable
                .doOnNext(message -> LOGGER.trace(
                        "Message before window with sequence {} and direction {}",
                        message.getSequence(),
                        message.getDirection()
                ))
                .window(1, TimeUnit.SECONDS, PIPELINE_SCHEDULER, maxMessageBatchSize)
                .concatMapSingle(Flowable::toList)
                .filter(list -> !list.isEmpty())
                .map(ConnectivityBatch::new)
                .doOnNext(batch -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Batch {} is created", batch.getMessages().stream()
                                .map(ConnectivityMessage::getSequence)
                                .collect(Collectors.toList()));
                    }
                })
                .subscribe(batch -> {
                    try {
                        RawMessageBatch rawBatch = batch.convertToProtoRawBatch();
                        rawMessageRouter.send(rawBatch); //FIXME: Only one pin can be used
                    } catch (Exception e) {
                        if (LOGGER.isErrorEnabled()) {
                            LOGGER.error("Cannot send batch with sequences: {}",
                                    batch.getMessages().stream().map(ConnectivityMessage::getSequence).collect(Collectors.toList()),
                                    e);
                        }
                    }
                });
        LOGGER.info("Subscribed to transfer raw batch group");

        LOGGER.info("Connected to publish batches group");
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

    // this method is public for test purposes
    public static IServiceProxy loadService(IServiceFactory serviceFactory,
            CommonFactory commonFactory,
            ConnectivityConfiguration configuration,
            IServiceListener serviceListener) throws IOException {
        try {
            IServiceProxy service = serviceFactory.createService(ServiceName.parse(configuration.getName()),
                    SailfishURI.unsafeParse(SailfishURIUtils.sanitize(configuration.getType())),
                    serviceListener);

            ISettingsProxy settings = service.getSettings();

            for (Entry<String, Object> settingsEntry : configuration.getSettings().entrySet()) {
                String settingName = settingsEntry.getKey();
                Object castValue = castValue(settings, settingName, settingsEntry.getValue());
                settings.setParameterValue(settingName, castValue);
            }

            for (DictionaryType sfDictionaryType : settings.getDictionaryTypes()) {
                var dictionaryType = com.exactpro.th2.common.schema.dictionary.DictionaryType.valueOf(sfDictionaryType.name());
                try (InputStream stream = commonFactory.readDictionary(dictionaryType)) {
                    SailfishURI uri = serviceFactory.registerDictionary(sfDictionaryType.name(), stream, true);
                    settings.setDictionary(sfDictionaryType, uri);
                }
            }

            return service;
        } catch (ConversionException | ServiceFactoryException e) {
            throw new RuntimeException(String.format("Could not load service '%s'", configuration.getName()), e);
        }
    }

    private static Object castValue(ISettingsProxy settings, String settingName, Object value) {
        Class<?> settingType = primitiveToWrapper(settings.getParameterType(settingName));

        if (settingType == null) {
            throw new IllegalArgumentException("Can not find setting '" + settingName + "' in service");
        }

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
                LOGGER.info("Service started. Starting message sender");
                messageSender.start();
                LOGGER.info("Subscription finished");
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
