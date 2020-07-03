/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.connectivity.configuration.Configuration;
import com.exactpro.th2.connectivity.utility.SailfishMetadataExtensions;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceBlockingStub;
import com.exactpro.th2.infra.grpc.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.exactpro.th2.connectivity.utility.SailfishMetadataExtensions.setParentEventID;
import static java.util.Objects.requireNonNull;

public class MessageSender {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());
    private final IServiceProxy serviceProxy;
    private final RabbitMqSubscriber rabbitMqSubscriber;
    private final EventStoreServiceBlockingStub eventStoreConnector;
    private final ProtoToIMessageConverter protoToIMessageConverter;

    public MessageSender(IServiceProxy serviceProxy,
            Configuration configuration,
            EventStoreServiceBlockingStub eventStoreConnector, ProtoToIMessageConverter protoToIMessageConverter) {
        this.serviceProxy = requireNonNull(serviceProxy, "Service proxy can't be null");
        this.eventStoreConnector = requireNonNull(eventStoreConnector, "Event store connector can't be null");
        this.protoToIMessageConverter = requireNonNull(protoToIMessageConverter, "Protobuf to IMessage converter can't be null") ;
        this.rabbitMqSubscriber = new RabbitMqSubscriber(configuration.getExchangeName(), this::processMessageFromQueue, null, configuration.getToSendQueueName());
    }

    public void start() throws IOException, TimeoutException {
        rabbitMqSubscriber.startListening(); //FIXME: We should subscribe to targrt queue
    }

    public void stop() throws IOException {
        logger.info("Stop listener the 'sender' queue");
        rabbitMqSubscriber.close();
    }

    private void processMessageFromQueue(String consumerTag, Delivery delivery) {
        try {
            Message protoMessage = Message.parseFrom(delivery.getBody());
            IMessage message = protoToIMessageConverter.fromProtoMessage(protoMessage, true);
            if (protoMessage.hasParentEventId()) {
                setParentEventID(message.getMetaData(), protoMessage.getParentEventId());
            }
            logger.debug("Converted {}.{} message {}", message.getNamespace(), message.getName(), message);
            IMessage sendMessage = serviceProxy.send(message);
            logger.debug("message sent {}.{}: {}", sendMessage.getNamespace(), sendMessage.getName(), sendMessage);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Could not convert proto message to IMessage. Consumer tag {}", consumerTag, e);
        } catch (InterruptedException e) {
            logger.error("Send message operation interrupted. Consumer tag {}", consumerTag, e);
        } catch (RuntimeException e) {
            logger.error("Could not send IMessage. Consumer tag {}", consumerTag, e);
        }
    }
}
