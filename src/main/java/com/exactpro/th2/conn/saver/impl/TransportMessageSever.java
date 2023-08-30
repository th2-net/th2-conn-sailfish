package com.exactpro.th2.conn.saver.impl;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.IMetadata;
import com.exactpro.sf.common.messages.MetadataExtensions;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage;
import com.exactpro.th2.common.utils.event.EventUtilsKt;
import com.exactpro.th2.common.utils.message.MessageUtilsKt;
import com.exactpro.th2.conn.ConnectivityBatch;
import com.exactpro.th2.conn.ConnectivityMessage;
import com.exactpro.th2.conn.saver.MessageSaver;
import com.exactpro.th2.conn.utility.MetadataProperty;
import com.exactpro.th2.conn.utility.SailfishMetadataExtensions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.exactpro.sf.common.messages.MetadataExtensions.getMessageProperties;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class TransportMessageSever implements MessageSaver {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportMessageSever.class);
    private final MessageRouter<GroupBatch> router;

    public TransportMessageSever(MessageRouter<GroupBatch> router) {
        this.router = Objects.requireNonNull(router, "router cannot be null");
    }

    @Override
    public void save(ConnectivityBatch batch) throws IOException {
        router.send(convertToGroupBatch(batch));
    }

    private static GroupBatch convertToGroupBatch(ConnectivityBatch batch) {
        return GroupBatch.builder()
                .setBook(batch.getBookName())
                .setSessionGroup(batch.getGroup())
                .setGroups(convertToGroups(batch.getMessages()))
                .build();
    }

    private static List<MessageGroup> convertToGroups(List<ConnectivityMessage> messages) {
        return messages.stream()
                .map(TransportMessageSever::convertToGroup)
                .collect(Collectors.toUnmodifiableList());
    }

    public static MessageGroup convertToGroup(ConnectivityMessage connectivityMessage) {
        MessageID messageId = connectivityMessage.getMessageId();
        RawMessage.Builder rawMessage = RawMessage.builder()
                .setId(MessageUtilsKt.toTransport(messageId));
        ByteBuf body = Unpooled.buffer(connectivityMessage.calculateTotalBodySize());
        for (IMessage message : connectivityMessage.getSailfishMessages()) {
            IMetadata sfMetadata = message.getMetaData();
            if (SailfishMetadataExtensions.contains(sfMetadata, MetadataProperty.PARENT_EVENT_ID)) {
                EventID parentEventID = SailfishMetadataExtensions.getParentEventID(sfMetadata);
                // Should never happen because the Sailfish does not support sending multiple messages at once
                if (rawMessage.getEventId() != null) {
                    LOGGER.warn("The parent ID is already set for message {}. Current ID: {}, New ID: {}",
                            messageId, rawMessage.getEventId(), parentEventID);
                }
                rawMessage.setEventId(EventUtilsKt.toTransport(parentEventID));
            }
            Map<String, String> props = defaultIfNull(getMessageProperties(sfMetadata), Collections.emptyMap());
            if (!props.isEmpty()) {
                rawMessage.metadataBuilder().putAll(props);
            }

            byte[] rawMessageData = MetadataExtensions.getRawMessage(sfMetadata);
            if (rawMessageData == null) {
                LOGGER.warn("The message has empty raw data {}: {}", message.getName(), message);
                continue;
            }
            body.writeBytes(rawMessageData);
        }
        return MessageGroup.builder()
                .addMessage(rawMessage.setBody(body).build())
                .build();
    }
}
