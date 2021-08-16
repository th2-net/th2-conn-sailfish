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
package com.exactpro.th2.conn;

import static com.exactpro.sf.externalapi.DictionaryType.MAIN;
import static com.exactpro.sf.externalapi.DictionaryType.OUTGOING;
import static com.exactpro.th2.conn.utility.EventStoreExtensions.addException;
import static com.exactpro.th2.conn.utility.EventStoreExtensions.storeEvent;
import static com.exactpro.th2.conn.utility.MetadataProperty.PARENT_EVENT_ID;
import static com.exactpro.th2.conn.utility.SailfishMetadataExtensions.contains;
import static com.exactpro.th2.conn.utility.SailfishMetadataExtensions.getParentEventID;
import static com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter.createParameters;
import static io.reactivex.rxjava3.plugins.RxJavaPlugins.createSingleScheduler;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
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

import com.exactpro.sf.common.messages.structures.IDictionaryStructure;
import com.exactpro.sf.common.services.ServiceName;
import com.exactpro.sf.comparison.conversion.ConversionException;
import com.exactpro.sf.comparison.conversion.MultiConverter;
import com.exactpro.sf.configuration.suri.SailfishURI;
import com.exactpro.sf.configuration.suri.SailfishURIException;
import com.exactpro.sf.configuration.suri.SailfishURIUtils;
import com.exactpro.sf.configuration.workspace.WorkspaceSecurityException;
import com.exactpro.sf.externalapi.DictionaryType;
import com.exactpro.sf.externalapi.IMessageFactoryProxy;
import com.exactpro.sf.externalapi.IServiceFactory;
import com.exactpro.sf.externalapi.IServiceListener;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.sf.externalapi.ISettingsProxy;
import com.exactpro.sf.externalapi.ServiceFactory;
import com.exactpro.sf.externalapi.impl.ServiceFactoryException;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.conn.configuration.ConnectivityConfiguration;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.exactpro.th2.conn.events.EventType;
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter;
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.TextFormat;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.exceptions.Exceptions;
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

            MessageRouter<MessageBatch> parsedMessageBatch = factory.getMessageRouterParsedBatch();

            FlowableProcessor<RelatedMessagesBatch> processor = UnicastProcessor.create();
            disposer.register(() -> {
                LOGGER.info("Complite pipeline publisher");
                processor.onComplete();
            });

            IServiceFactory serviceFactory = new ServiceFactory(workspaceFolder);
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

            var eventDispatcher = EventDispatcher.createDispatcher(eventBatchRouter, rootEventID, Map.of(
                    EventType.ERROR, errorEventsRoot.getId(),
                    EventType.SERVICE_EVENT, serviceEventsRoot.getId()
            ));

            IServiceListener serviceListener = new ServiceListener(directionToSequence, new IMessageToProtoConverter(), configuration.getSessionAlias(), processor, eventDispatcher);
            IServiceProxy serviceProxy = loadService(serviceFactory, factory, configuration, serviceListener);
            disposer.register(() -> {
                LOGGER.info("Stop service proxy");
                serviceProxy.stop();
            });
            printServiceSetting(serviceProxy);
            IMessageFactoryProxy messageFactory = serviceFactory.getMessageFactory(serviceProxy.getType());
            DictionaryType dictionaryType = serviceProxy.getSettings().getDictionaryTypes().contains(OUTGOING) ? OUTGOING : MAIN;
            SailfishURI senderDictionaryURI = serviceProxy.getSettings().getDictionary(dictionaryType);
            IDictionaryStructure dictionary = serviceFactory.getDictionary(senderDictionaryURI);

            MessageSender messageSender = new MessageSender(serviceProxy,
                    new ProtoToIMessageConverter(
                            messageFactory, dictionary, senderDictionaryURI,
                            createParameters()
                                    .setAllowUnknownEnumValues(configuration.isAllowUnknownEnumValues())
                    ),
                    parsedMessageBatch, eventDispatcher);
            disposer.register(() -> {
                LOGGER.info("Stop 'message send' listener");
                messageSender.stop();
            });

            createPipeline(processor, processor::onComplete, eventDispatcher, parsedMessageBatch, factory.getMessageRouterRawBatch())
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

    private static @NonNull Flowable<Flowable<RelatedMessagesBatch>> createPipeline(
            Flowable<RelatedMessagesBatch> flowable, Action terminateFlowable,
            EventDispatcher eventDispatcher,
            MessageRouter<MessageBatch> parsedMessageRouter,
            MessageRouter<RawMessageBatch> rawMessageRouter
    ) {
        LOGGER.info("AvailableProcessors '{}'", Runtime.getRuntime().availableProcessors());

        return flowable.observeOn(PIPELINE_SCHEDULER)
                .doOnNext(relatedMessages -> LOGGER.debug("Start handling message batch with messages {}", relatedMessages.getMessages()))
                .groupBy(RelatedMessagesBatch::getDirection)
                .map(group -> {
                    @NonNull Direction direction = requireNonNull(group.getKey(), "Direction can't be null");
                    Flowable<RelatedMessagesBatch> messageConnectable = group
                            .doOnCancel(terminateFlowable) // This call is required for terminate the publisher and prevent creation another group
                            .publish()
                            .refCount(direction == Direction.SECOND ? 2 : 1);

                    if (direction == Direction.SECOND) {
                        subscribeToSendMessage(eventDispatcher, messageConnectable);
                    }
                    createPackAndPublishPipeline(direction, messageConnectable, parsedMessageRouter, rawMessageRouter, eventDispatcher);

                    return messageConnectable;
                });
    }

    private static void subscribeToSendMessage(EventDispatcher eventBatchRouter, Flowable<RelatedMessagesBatch> messageConnectable) {
        //noinspection ResultOfMethodCallIgnored
        messageConnectable
                .subscribe(relatedMessages -> {
                    for (ConnectivityMessage message : relatedMessages.getMessages()) {
                        if (!contains(message.getSailfishMessage().getMetaData(), PARENT_EVENT_ID)) {
                            continue;
                        }
                        Event event = Event.start().endTimestamp()
                                .name("Send '" + message.getSailfishMessage().getName() + "' message")
                                .type("Send message")
                                .messageID(message.getMessageID());
                        LOGGER.debug("Sending event {} related to message with sequence {}", event.getId(), message.getSequence());
                        safeStore(eventBatchRouter, event, getParentEventID(message.getSailfishMessage().getMetaData()).getId());
                    }
                });
    }

    private static void createPackAndPublishPipeline(Direction direction, Flowable<RelatedMessagesBatch> messageConnectable,
                                                     MessageRouter<MessageBatch> parsedMessageRouter, MessageRouter<RawMessageBatch> rawMessageRouter,
                                                     EventDispatcher eventDispatcher) {

        LOGGER.info("Map group {}", direction);
        Flowable<ConnectivityBatch> batchConnectable = messageConnectable
                .window(1, TimeUnit.SECONDS, PIPELINE_SCHEDULER, MAX_MESSAGES_COUNT)
                .concatMapSingle(flowable -> flowable.flatMapIterable(RelatedMessagesBatch::getMessages).toList())
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

        batchConnectable.subscribeWith(new PublishSubscriber<>(
                rawMessageRouter,
                ConnectivityBatch::convertToProtoRawBatch,
                eventDispatcher,
                direction == Direction.FIRST ? "first" : "second", "publish", "raw"
        ));
        LOGGER.info("Subscribed to transfer raw batch group {}", direction);

        batchConnectable.subscribeWith(new PublishSubscriber<>(
                parsedMessageRouter,
                ConnectivityBatch::convertToProtoParsedBatch,
                eventDispatcher,
                direction == Direction.FIRST ? "first" : "second", "publish", "parsed"
        ));
        LOGGER.info("Subscribed to transfer parsed batch group {}", direction);

        LOGGER.info("Connected to publish batches group {}", direction);
    }

    private static class PublishSubscriber<T> extends DisposableSubscriber<ConnectivityBatch> {
        private final Function<ConnectivityBatch, T> transformer;
        private final MessageRouter<T> router;
        private final EventDispatcher eventDispatcher;
        private final String[] attributes;

        private PublishSubscriber(
                MessageRouter<T> router,
                Function<ConnectivityBatch, T> transformer,
                EventDispatcher eventDispatcher,
                String... attributes
        ) {
            this.router = requireNonNull(router, "'Router' parameter");
            this.transformer = requireNonNull(transformer, "'Transformer' parameter");
            this.eventDispatcher = requireNonNull(eventDispatcher, "'Event Dispatcher' parameter");
            this.attributes = requireNonNull(attributes, "'Attributes' parameter");
        }

        @Override
        public void onNext(ConnectivityBatch connectivityBatch) {
            try {
                T toPublish = transformer.apply(connectivityBatch);
                router.send(toPublish, attributes);
            } catch (Exception ex) {
                List<MessageID> messageIDs = connectivityBatch.getMessages().stream().map(ConnectivityMessage::getMessageID).collect(Collectors.toList());
                String messageIDsAsString = messageIDs.stream().map(TextFormat::shortDebugString).collect(Collectors.joining(", "));
                LOGGER.error("Cannot send batch with seq {}", messageIDsAsString, ex);
                publishErrorToConnEvent(ex, messageIDs, messageIDsAsString);

                for (ConnectivityMessage message : connectivityBatch.getMessages()) {
                    EventID parentId = message.getParentId();
                    if (parentId != null) {
                        publishErrorToParentEvent(ex, parentId, message);
                    }
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Unexpected error for publisher with attributes: {}", Arrays.toString(attributes), t);
            }
            Event error = Event.start().endTimestamp()
                    .name("Unexpected error for " + Arrays.toString(attributes))
                    .type("UnexpectedPublishingError")
                    .status(Status.FAILED)
                    .bodyData(EventUtils.createMessageBean("Caught unexpected publishing error"));
            addException(error, t);
            safeStore(eventDispatcher, EventHolder.createError(error));
        }

        @Override
        public void onComplete() {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Publishing with attributes {} completed", Arrays.toString(attributes));
            }
        }

        private void publishErrorToParentEvent(Throwable ex, EventID parentId, ConnectivityMessage message) {
            Event error = Event.start().endTimestamp()
                    .name("Message publishing error")
                    .type("PublishingError")
                    .status(Status.FAILED)
                    .bodyData(EventUtils.createMessageBean("Cannot publish message " + message.getSailfishMessage().getName()));
            addException(error, ex);
            safeStore(eventDispatcher, error, parentId.getId());
        }

        private void publishErrorToConnEvent(Throwable ex, List<MessageID> messageIDs, String messageIDsAsString) {
            Event error = Event.start().endTimestamp()
                    .name("Publishing to " + Arrays.toString(attributes) + " error")
                    .type("PublishingError")
                    .status(Status.FAILED)
                    .bodyData(EventUtils.createMessageBean("Cannot publish messages batch with IDs: " + messageIDsAsString));
            messageIDs.forEach(error::messageID);/* probably will cause error in the rpt provider */
            addException(error, ex);
            safeStore(eventDispatcher, EventHolder.createError(error));
        }
    }

    private static void safeStore(EventDispatcher dispatcher, EventHolder holder) {
        try {
            dispatcher.store(holder);
        } catch (Exception ex) {
            LOGGER.error("Cannot store event {} with type {}", holder.getEvent().getId(), holder.getType(), ex);
        }
    }

    private static void safeStore(EventDispatcher dispatcher, Event event, String parentId) {
        try {
            dispatcher.store(event, parentId);
        } catch (Exception ex) {
            LOGGER.error("Cannot store event {} with parent ID {}", event.getId(), parentId, ex);
        }
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
