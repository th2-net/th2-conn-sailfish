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
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.exactpro.th2.conn.utility.EventStoreExtensions;
import com.exactpro.th2.common.grpc.Direction;

import static com.exactpro.th2.common.grpc.Direction.FIRST;
import static com.exactpro.th2.common.grpc.Direction.SECOND;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Counter;
import io.reactivex.rxjava3.annotations.NonNull;

public class ServiceListener implements IServiceListener {

    private static final Map<Direction, Counter> DIRECTION_TO_COUNTER;

    static {
        Map<Direction, Counter> map = new EnumMap<>(Direction.class);
        map.put(FIRST, Counter.build()
                .name("th2_conn_incoming_msg_quantity")
                // FIXME: use DEFAULT_SESSION_ALIAS_LABEL_NAME variable
                .labelNames("session_alias")
                .help("Quantity of incoming messages to conn")
                .register());
        map.put(SECOND, Counter.build()
                .name("th2_conn_outgoing_msg_quantity")
                // FIXME: use DEFAULT_SESSION_ALIAS_LABEL_NAME variable
                .labelNames("session_alias")
                .help("Quantity of outgoing messages from conn")
                .register());

        DIRECTION_TO_COUNTER = Collections.unmodifiableMap(map);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceListener.class);

    private final Map<Direction, AtomicLong> directionToSequence;
    private final String sessionAlias;
    private final Subscriber<ConnectivityMessage> subscriber;
    private final EventDispatcher eventDispatcher;

    public ServiceListener(
            Map<Direction, AtomicLong> directionToSequence,
            String sessionAlias,
            Subscriber<ConnectivityMessage> subscriber,
            EventDispatcher eventDispatcher
    ) {
        this.directionToSequence = requireNonNull(directionToSequence, "Map direction to sequence counter can't be null");
        this.sessionAlias = requireNonNull(sessionAlias, "Session alias can't be null");
        this.subscriber = requireNonNull(subscriber, "Subscriber can't be null");
        this.eventDispatcher = requireNonNull(eventDispatcher, "Event dispatcher can't be null");
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

            eventDispatcher.store(EventHolder.createError(event));
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Store event related to internal error failure", e);
        }
    }

    @Override
    public void onMessage(IServiceProxy service, IMessage message, boolean rejected, ServiceHandlerRoute route) {
        synchronized (subscriber) {
            LOGGER.debug("Handle message - route: {}; message: {}", route, message);
            Direction direction = route.isFrom() ? FIRST : SECOND;
            DIRECTION_TO_COUNTER.get(direction).labels(sessionAlias).inc();
            AtomicLong directionSeq = directionToSequence.get(direction);
            ConnectivityMessage connectivityMessage;

            if (EvolutionBatch.MESSAGE_NAME.equals(message.getName())) {
                EvolutionBatch batch = new EvolutionBatch(message);
                connectivityMessage = createConnectivityMessage(batch.getBatch(), direction, directionSeq);
            } else {
                connectivityMessage = createConnectivityMessage(List.of(message), direction, directionSeq);
            }

            subscriber.onNext(connectivityMessage);
        }
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

            eventDispatcher.store(EventHolder.createServiceEvent(event));
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Store event related to internal event failure", e);
        }
    }

    @NonNull
    private ConnectivityMessage createConnectivityMessage(List<IMessage> messages, Direction direction, AtomicLong directionSeq) {
        long sequence = directionSeq.incrementAndGet();
        LOGGER.debug("On message: direction '{}'; sequence '{}'; messages '{}'", direction, sequence, messages);
        return new ConnectivityMessage(messages, sessionAlias, direction, sequence);
    }
}
