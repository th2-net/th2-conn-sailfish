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
package com.exactpro.th2;

import static java.util.Objects.requireNonNull;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.th2.infra.grpc.ConnectionID;
import com.exactpro.th2.infra.grpc.Direction;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.th2.infra.grpc.MessageMetadata;
import com.exactpro.th2.infra.grpc.RawMessage;
import com.exactpro.th2.infra.grpc.RawMessageMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

public class ConnectivityMessage {
    public static final long MILLISECONDS_IN_SECOND = 1_000L;
    public static final long NANOSECONDS_IN_MILLISECOND = 1_000_000L;

    private final IMessage iMessage;

    private final IMessageToProtoConverter converter;

    // This variables can be calcilated in methods
    private final MessageID messageID;
    private final Timestamp timestamp;

    public ConnectivityMessage(IMessageToProtoConverter converter, IMessage iMessage, String sessionAlias, Direction direction, long sequence) {
        this.converter = requireNonNull(converter, "Converter can't be null");
        this.iMessage = requireNonNull(iMessage, "Message can't be null");
        messageID = createMessageID(createConnectionID(requireNonNull(sessionAlias, "Session alias can't be null")),
                requireNonNull(direction, "Direction can't be null"), sequence);
        timestamp = createTimestamp(iMessage.getMetaData().getMsgTimestamp().getTime());
    }

    public String getSessionAlias() {
        return messageID.getConnectionId().getSessionAlias();
    }

    public RawMessage convertToProtoRawMessage() {
        return RawMessage.newBuilder()
                        .setMetadata(createRawMessageMetadata(messageID, timestamp))
                        .setBody(ByteString.copyFrom(iMessage.getMetaData().getRawMessage()))
                        .build();
    }

    public Message convertToProtoParsedMessage() {
        return converter.toProtoMessage(iMessage)
                        .setMetadata(createMessageMetadata(messageID, iMessage.getName(), timestamp))
                        .build();
    }

    public MessageID getMessageID() {
        return messageID;
    }

    public long getSequence() {
        return messageID.getSequence();
    }

    public Direction getDirection() {
        return messageID.getDirection();
    }

    public IMessage getiMessage() {
        return iMessage;
    }

    private static ConnectionID createConnectionID(String sessionAlias) {
        return ConnectionID.newBuilder()
                .setSessionAlias(sessionAlias)
                .build();
    }

    private static MessageID createMessageID(ConnectionID connectionId, Direction direction, long sequence) {
        return MessageID.newBuilder()
                .setConnectionId(connectionId)
                .setDirection(direction)
                .setSequence(sequence)
                .build();
    }

    private static MessageMetadata createMessageMetadata(MessageID messageID, String messageName, Timestamp timestamp) {
        return MessageMetadata.newBuilder()
                .setId(messageID)
                .setTimestamp(timestamp)
                .setMessageType(messageName)
                .build();
    }

    private static RawMessageMetadata createRawMessageMetadata(MessageID messageID, Timestamp timestamp) {
        return RawMessageMetadata.newBuilder()
                .setId(messageID)
                .setTimestamp(timestamp)
                .build();
    }

    // TODO: Required nanosecond accuracy
    private static Timestamp createTimestamp(long milliseconds) {
        return Timestamp.newBuilder()
                .setSeconds(milliseconds / MILLISECONDS_IN_SECOND)
                .setNanos((int) (milliseconds % MILLISECONDS_IN_SECOND * NANOSECONDS_IN_MILLISECOND))
                .build();
    }
}
