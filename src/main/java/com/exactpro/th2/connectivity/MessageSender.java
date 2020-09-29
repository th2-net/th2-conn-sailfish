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
package com.exactpro.th2.connectivity;

import static com.exactpro.th2.connectivity.utility.SailfishMetadataExtensions.setParentEventID;
import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.ProtoToIMessageConverter;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.schema.message.SubscriberMonitor;

public class MessageSender {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());
    private final IServiceProxy serviceProxy;
    private final MessageRouter<MessageBatch> router;
    private final ProtoToIMessageConverter protoToIMessageConverter;
    private volatile SubscriberMonitor subscriberMonitor;

    public MessageSender(IServiceProxy serviceProxy,
            ProtoToIMessageConverter protoToIMessageConverter,
            MessageRouter<MessageBatch> router) {
        this.serviceProxy = requireNonNull(serviceProxy, "Service proxy can't be null");
        this.protoToIMessageConverter = requireNonNull(protoToIMessageConverter, "Protobuf to IMessage converter can't be null") ;
        this.router = requireNonNull(router, "Message router can't be null");
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
                IMessage message = protoToIMessageConverter.fromProtoMessage(protoMessage, true);
                if (protoMessage.hasParentEventId()) {
                    setParentEventID(message.getMetaData(), protoMessage.getParentEventId());
                }
                logger.debug("Converted {}.{} message {}", message.getNamespace(), message.getName(), message);
                IMessage sendMessage = serviceProxy.send(message);
                logger.debug("message sent {}.{}: {}", sendMessage.getNamespace(), sendMessage.getName(), sendMessage);
            } catch (InterruptedException e) {
                logger.error("Send message operation interrupted. Consumer tag {}", consumerTag, e);
            } catch (RuntimeException e) {
                logger.error("Could not send IMessage. Consumer tag {}", consumerTag, e);
            }
        }
    }
}
