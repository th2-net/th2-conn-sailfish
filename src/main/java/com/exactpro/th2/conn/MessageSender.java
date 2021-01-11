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

import static com.exactpro.th2.conn.utility.SailfishMetadataExtensions.contains;
import static com.exactpro.th2.conn.utility.SailfishMetadataExtensions.getParentEventID;
import static com.exactpro.th2.conn.utility.SailfishMetadataExtensions.setParentEventID;
import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.exactpro.th2.conn.utility.EventStoreExtensions;
import com.exactpro.th2.conn.utility.MetadataProperty;
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter;

import io.reactivex.rxjava3.annotations.NonNull;

public class MessageSender {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());
    private final IServiceProxy serviceProxy;
    private final MessageRouter<MessageBatch> router;
    private final ProtoToIMessageConverter protoToIMessageConverter;
    private final EventDispatcher eventDispatcher;
    private volatile SubscriberMonitor subscriberMonitor;

    public MessageSender(IServiceProxy serviceProxy,
                         ProtoToIMessageConverter protoToIMessageConverter,
                         MessageRouter<MessageBatch> router,
                         EventDispatcher eventDispatcher) {
        this.serviceProxy = requireNonNull(serviceProxy, "Service proxy can't be null");
        this.protoToIMessageConverter = requireNonNull(protoToIMessageConverter, "Protobuf to IMessage converter can't be null") ;
        this.router = requireNonNull(router, "Message router can't be null");
        this.eventDispatcher = requireNonNull(eventDispatcher, "'Event dispatcher' parameter");
    }

    public void start() {
        if (subscriberMonitor != null) {
            throw new IllegalStateException("Already subscribe");
        }

        subscriberMonitor = router.subscribeAll(this::handle, "send", "parsed");
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

    private void handle(String consumerTag, MessageBatch messageBatch) {
        for (Message protoMessage : messageBatch.getMessagesList()) {
            try {
                IMessage message = convertToIMessage(protoMessage);
                IMessage sentMessage = sendMessage(message);
                logger.debug("message sent {}.{}: {}", sentMessage.getNamespace(), sentMessage.getName(), sentMessage);
            } catch (InterruptedException e) {
                logger.error("Send message operation interrupted. Consumer tag {}", consumerTag, e);
            } catch (RuntimeException e) {
                logger.error("Could not send IMessage. Consumer tag {}", consumerTag, e);
            }
        }
    }

    @NonNull
    private IMessage convertToIMessage(Message protoMessage) {
        try {
            IMessage message = protoToIMessageConverter.fromProtoMessage(protoMessage, true);
            if (protoMessage.hasParentEventId()) {
                setParentEventID(message.getMetaData(), protoMessage.getParentEventId());
            }
            logger.debug("Converted {}.{} message {}", message.getNamespace(), message.getName(), message);
            return message;
        } catch (Exception ex) {
            Event errorEvent = createErrorEvent("ConversionError")
                    .bodyData(EventUtils.createMessageBean("Cannot convert proto Message to IMessage."))
                    .bodyData(EventUtils.createMessageBean("Protobuf message:"))
                    .bodyData(EventStoreExtensions.createProtoMessageBean(protoMessage));
            EventStoreExtensions.addException(errorEvent, ex);
            String parentId = protoMessage.hasParentEventId()
                    ? protoMessage.getParentEventId().getId()
                    : null;
            storeErrorEvent(errorEvent, parentId);
            throw ex;
        }
    }

    private IMessage sendMessage(IMessage message) throws InterruptedException {
        try {
            return serviceProxy.send(message);
        } catch (Exception ex) {
            Event errorEvent = createErrorEvent("SendError")
                    .bodyData(EventUtils.createMessageBean("Cannot send message."))
                    .bodyData(EventUtils.createMessageBean(message.toString()));
            EventStoreExtensions.addException(errorEvent, ex);
            String parentId = contains(message.getMetaData(), MetadataProperty.PARENT_EVENT_ID)
                    ? getParentEventID(message.getMetaData()).toString()
                    : null;
            storeErrorEvent(errorEvent, parentId);
            throw ex;
        }
    }

    private void storeErrorEvent(Event errorEvent, String parentId) {
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

    private Event createErrorEvent(String eventType) {
        return Event.start().endTimestamp()
                .status(Status.FAILED)
                .type(eventType);
    }

}
