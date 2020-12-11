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

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.util.EvolutionBatch;
import com.exactpro.sf.externalapi.IServiceListener;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.sf.services.IdleStatus;
import com.exactpro.sf.services.ServiceEvent;
import com.exactpro.sf.services.ServiceEvent.Level;
import com.exactpro.sf.services.ServiceHandlerRoute;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.conn.utility.EventStoreExtensions;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.fasterxml.jackson.core.JsonProcessingException;

import static com.exactpro.th2.common.grpc.Direction.FIRST;
import static com.exactpro.th2.common.grpc.Direction.SECOND;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.annotations.NonNull;

public class ServiceListener implements IServiceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceListener.class);

    private final Map<Direction, AtomicLong> directionToSequence;
    private final IMessageToProtoConverter converter;
    private final String sessionAlias;
    private final Subscriber<RelatedMessagesBatch> subscriber;
    private final MessageRouter<EventBatch> eventBatchRouter;
    private final String rootEventID;

    public ServiceListener(Map<Direction, AtomicLong> directionToSequence, IMessageToProtoConverter converter, String sessionAlias, Subscriber<RelatedMessagesBatch> subscriber,
            MessageRouter<EventBatch> eventBatchRouter, String rootEventID
    ) {
        this.directionToSequence = requireNonNull(directionToSequence, "Map direction to sequence counter can't be null");
        this.converter = requireNonNull(converter, "Converter can't be null");
        this.sessionAlias = requireNonNull(sessionAlias, "Session alias can't be null");
        this.subscriber = requireNonNull(subscriber, "Subscriber can't be null");
        this.eventBatchRouter = requireNonNull(eventBatchRouter, "Event batch router can't be null");
        this.rootEventID = requireNonNull(rootEventID, "Root event ID can't be null");
    }

    @Override
    public void sessionOpened(IServiceProxy service) {
        LOGGER.info("Session '{}' opened", sessionAlias);
    }

    @Override
    public void sessionClosed(IServiceProxy service) {
        LOGGER.info("Session '{}' closed", sessionAlias);
    }

    @Override
    public void sessionIdle(IServiceProxy service, IdleStatus status) {
        LOGGER.debug("Session '{}' idle", sessionAlias);
    }

    @Override
    public void exceptionCaught(IServiceProxy service, Throwable cause) {
        LOGGER.error("Session '{}' threw exception", sessionAlias, cause);
        try {
            Event event = Event.start().endTimestamp()
                    .name("Connection error")
                    .status(Status.FAILED)
                    .type("Error");

            EventStoreExtensions.addException(event, cause);

            EventStoreExtensions.storeEvent(eventBatchRouter, event,
                    rootEventID);
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Store event related to internal error failure", e);
        }
    }

    @Override
    public void onMessage(IServiceProxy service, IMessage message, boolean rejected, ServiceHandlerRoute route) {
        LOGGER.debug("Handle message - route: {}; message: {}", route, message);
        Direction direction = route.isFrom() ? FIRST : SECOND;
        AtomicLong directionSeq = directionToSequence.get(direction);
        RelatedMessagesBatch relatedMessagesBatch;

        if (EvolutionBatch.MESSAGE_NAME.equals(message.getName())) {
            EvolutionBatch batch = new EvolutionBatch(message);
            List<ConnectivityMessage> relatedMessages = new ArrayList<>();
            for (IMessage msg : batch.getBatch()) {
                ConnectivityMessage connectivityMessage = createConnectivityMessage(msg, direction, directionSeq);
                relatedMessages.add(connectivityMessage);
            }
            relatedMessagesBatch = new RelatedMessagesBatch(direction, relatedMessages);
        } else {
            ConnectivityMessage connectivityMessage = createConnectivityMessage(message, direction, directionSeq);
            relatedMessagesBatch = new RelatedMessagesBatch(direction, List.of(connectivityMessage));
        }

        subscriber.onNext(relatedMessagesBatch);
    }

    @Override
    public void onEvent(IServiceProxy service, ServiceEvent serviceEvent) {
        LOGGER.info("Session '{}' emitted service event '{}'", sessionAlias, serviceEvent);
        try {
            Event event = Event.start().endTimestamp()
                    .name(serviceEvent.getMessage())
                    .status(serviceEvent.getLevel() == Level.ERROR ? Status.FAILED : Status.PASSED)
                    .type("Service event")
                    .description(serviceEvent.getDetails());

            EventStoreExtensions.storeEvent(eventBatchRouter, event,
                    rootEventID);
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Store event related to internal event failure", e);
        }
    }

    @NonNull
    private ConnectivityMessage createConnectivityMessage(IMessage message, Direction direction, AtomicLong directionSeq) {
        long sequence = directionSeq.incrementAndGet();
        LOGGER.debug("On message: direction '{}'; sequence '{}'; message '{}'", direction, sequence, message);
        return new ConnectivityMessage(converter, message, sessionAlias, direction, sequence);
    }
}
