package com.exactpro.th2.conn;

import com.exactpro.sf.common.messages.IMetadata;
import com.exactpro.sf.common.messages.MetadataExtensions;
import com.exactpro.sf.common.messages.impl.Metadata;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.exactpro.th2.conn.utility.EventStoreExtensions;
import com.exactpro.th2.conn.utility.SailfishMetadataExtensions;
import io.reactivex.rxjava3.annotations.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractMessageSender {
    protected static final String SEND_ATTRIBUTE = "send";
    protected final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());
    protected final IServiceProxy serviceProxy;
    protected final EventDispatcher eventDispatcher;
    protected final EventID untrackedMessagesRoot;

    protected AbstractMessageSender(
            IServiceProxy serviceProxy,
            EventDispatcher eventDispatcher,
            EventID untrackedMessagesRoot
    ) {
        this.serviceProxy = Objects.requireNonNull(serviceProxy, "service proxy");
        this.eventDispatcher = Objects.requireNonNull(eventDispatcher, "event dispatcher");
        this.untrackedMessagesRoot =
                Objects.requireNonNull(untrackedMessagesRoot, "untracked messages root event");
    }

    public abstract void start();

    protected void sendMessage(MessageHolder holder) throws InterruptedException {
        EventID parentEventId = holder.getParentEventId();
        String parentEventBookName = parentEventId == null
                ? null
                : parentEventId.getBookName();
        if (StringUtils.isNotEmpty(parentEventBookName)
                && !parentEventBookName.equals(untrackedMessagesRoot.getBookName())) {
            MessageID messageID = holder.getId();
            storeErrorEvent(
                    createErrorEvent(String.format(
                            "Parent event book name is '%s' but should be '%s' for message with group '%s', session alias '%s' and direction '%s'",
                            parentEventBookName,
                            untrackedMessagesRoot.getBookName(),
                            messageID.getConnectionId().getSessionGroup(),
                            messageID.getConnectionId().getSessionAlias(),
                            messageID.getDirection()
                    )),
                    parentEventId
            );
            return;
        }
        byte[] data = holder.getBody();
        try {
            serviceProxy.sendRaw(data, toSailfishMetadata(holder));
            if (logger.isDebugEnabled()) {
                logger.debug("Message sent. Base64 view: {}", Base64.getEncoder().encodeToString(data));
            }
        } catch (Exception ex) {
            Event errorEvent = createErrorEvent("SendError", ex)
                    .bodyData(EventUtils.createMessageBean("Cannot send message. Message body in base64:"))
                    .bodyData(EventUtils.createMessageBean(Base64.getEncoder().encodeToString(data)));
            EventStoreExtensions.addException(errorEvent, ex);
            storeErrorEvent(errorEvent, holder.getParentEventId());
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
                .status(Event.Status.FAILED)
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

    private IMetadata toSailfishMetadata(MessageHolder holder) {
        IMetadata metadata = new Metadata();

        SailfishMetadataExtensions.setParentEventID(metadata, holder.getParentEventId() == null
                ? untrackedMessagesRoot
                : holder.getParentEventId()
        );

        Map<String, String> propertiesMap = holder.getProperties();
        if (!propertiesMap.isEmpty()) {
            MetadataExtensions.setMessageProperties(metadata, propertiesMap);
        }
        return metadata;
    }

    protected interface MessageHolder {
        @Nullable
        EventID getParentEventId();

        MessageID getId();

        Map<String, String> getProperties();

        byte[] getBody();
    }
}
