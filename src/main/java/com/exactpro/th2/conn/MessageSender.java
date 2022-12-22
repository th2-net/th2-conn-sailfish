/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.IMetadata;
import com.exactpro.sf.common.messages.MetadataExtensions;
import com.exactpro.sf.common.messages.impl.Metadata;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.exactpro.th2.conn.utility.EventStoreExtensions;
import com.exactpro.th2.conn.utility.SailfishMetadataExtensions;

import io.reactivex.rxjava3.annotations.Nullable;

public class MessageSender {
    private static final String SEND_ATTRIBUTE = "send";
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());
    private final IServiceProxy serviceProxy;
    private final MessageRouter<RawMessageBatch> router;
    private final EventDispatcher eventDispatcher;
    private final EventID untrackedMessagesRoot;
    private volatile SubscriberMonitor subscriberMonitor;

    public MessageSender(IServiceProxy serviceProxy,
                         MessageRouter<RawMessageBatch> router,
                         EventDispatcher eventDispatcher,
                         EventID untrackedMessagesRoot) {
        this.serviceProxy = requireNonNull(serviceProxy, "Service proxy can't be null");
        this.router = requireNonNull(router, "Message router can't be null");
        this.eventDispatcher = requireNonNull(eventDispatcher, "'Event dispatcher' can't be null");
        this.untrackedMessagesRoot = requireNonNull(untrackedMessagesRoot, "'untrackedMessagesRoot' can't be null");
    }

    public void start() {
        if (subscriberMonitor != null) {
            throw new IllegalStateException("Already subscribe");
        }

        logger.info("Subscribing to queue with messages to send");

        subscriberMonitor = router.subscribeAll(this::handle, SEND_ATTRIBUTE);
    }

    public void stop() throws IOException {
        if (subscriberMonitor == null) {
            throw new IllegalStateException("Not yet start subscribe");
        }

        logger.info("Stop listener the 'sender' queue");
        try {
            subscriberMonitor.unsubscribe();
        } catch (Exception e) {
            logger.error("Can not unsubscribe", e);
        }
    }

    private void handle(DeliveryMetadata deliveryMetadata, RawMessageBatch messageBatch) {
        for (RawMessage protoMessage : messageBatch.getMessagesList()) {
            try {
                sendMessage(protoMessage);
            } catch (InterruptedException e) {
                logger.error("Send message operation interrupted. Delivery metadata {}", deliveryMetadata, e);
            } catch (RuntimeException e) {
                logger.error("Could not send IMessage. Delivery metadata {}", deliveryMetadata, e);
            }
        }
    }

    private void sendMessage(RawMessage protoMsg) throws InterruptedException {
        EventID parentEventId = protoMsg.getParentEventId();
        String parentEventBookName = parentEventId.getBookName();
        if (!parentEventBookName.isEmpty() && !parentEventBookName.equals(untrackedMessagesRoot.getBookName())) {
            storeErrorEvent(
                    createErrorEvent(String.format(
                            "Parent event book name is '%s' but should be '%s' for message with group '%s', session alias '%s' and direction '%s'",
                            parentEventBookName,
                            untrackedMessagesRoot.getBookName(),
                            protoMsg.getMetadata().getId().getConnectionId().getSessionGroup(),
                            protoMsg.getMetadata().getId().getConnectionId().getSessionAlias(),
                            protoMsg.getMetadata().getId().getDirection()
                    )),
                    parentEventId
            );
            return;
        }
        byte[] data = protoMsg.getBody().toByteArray();
        try {
            serviceProxy.sendRaw(data, toSailfishMetadata(protoMsg));
            if (logger.isDebugEnabled()) {
                logger.debug("Message sent. Base64 view: {}", Base64.getEncoder().encodeToString(data));
            }
        } catch (Exception ex) {
            Event errorEvent = createErrorEvent("SendError", ex)
                    .bodyData(EventUtils.createMessageBean("Cannot send message. Message body in base64:"))
                    .bodyData(EventUtils.createMessageBean(Base64.getEncoder().encodeToString(data)));
            EventStoreExtensions.addException(errorEvent, ex);
            storeErrorEvent(errorEvent, protoMsg.hasParentEventId() ? parentEventId : null);
            throw ex;
        }
    }

    private void storeErrorEvent(Event errorEvent, @Nullable EventID parentId) {
        try {
            if (parentId == null) {
                eventDispatcher.store(EventHolder.createError(errorEvent));
            } else {
                eventDispatcher.store(errorEvent, parentId);
            }
        } catch (IOException e) {
            logger.error("Cannot store event {} (parentId: {})", errorEvent.getId(), parentId, e);
        }
    }

    private Event createErrorEvent(String eventType, Exception cause) {
        Event event = Event.start().endTimestamp()
                .status(Status.FAILED)
                .type(eventType)
                .name("Failed to send raw message");

        if (cause != null) {
            event.exception(cause, true);
        }
        return event;
    }

    private Event createErrorEvent(String eventType) {
        return createErrorEvent(eventType, null);
    }

    private IMetadata toSailfishMetadata(RawMessage protoMsg) {
        IMetadata metadata = new Metadata();

        SailfishMetadataExtensions.setParentEventID(metadata, protoMsg.hasParentEventId()
                ? protoMsg.getParentEventId()
                : untrackedMessagesRoot
        );

        Map<String, String> propertiesMap = protoMsg.getMetadata().getPropertiesMap();
        if (!propertiesMap.isEmpty()) {
            MetadataExtensions.setMessageProperties(metadata, propertiesMap);
        }
        return metadata;
    }
}
