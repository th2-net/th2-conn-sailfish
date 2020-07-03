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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.exactpro.th2.infra.grpc.Direction;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.RawMessageBatch;

public class ConnectivityBatch {
    private final String sessionAlias;
    private final long sequence;
    private final Direction direction;
    private final List<ConnectivityMessage> iMessages;

    public ConnectivityBatch(List<ConnectivityMessage> iMessages) {
        if (Objects.requireNonNull(iMessages, "Message list can't be null").isEmpty()) {
            throw new IllegalArgumentException("Message list can't be empty");
        }

        ConnectivityMessage firstMessage = iMessages.get(0);
        this.sessionAlias = firstMessage.getSessionAlias();
        this.direction = firstMessage.getDirection();
        if (!checkMessages(iMessages, sessionAlias, direction)) {
            throw new IllegalArgumentException("Message list can't be empty");
        }

        this.iMessages = List.copyOf(iMessages);
        this.sequence = firstMessage.getSequence();
    }

    public RawMessageBatch convertToProtoRawBatch() {
        return RawMessageBatch.newBuilder()
                .addAllMessages(iMessages.stream()
                        .map(iMsg -> iMsg.convertToProtoRawMessage())
                        .collect(Collectors.toList()))
                .build();
    }

    public MessageBatch convertToProtoParsedBatch() {
        return MessageBatch.newBuilder()
                .addAllMessages(iMessages.stream()
                        .map(iMsg -> iMsg.convertToProtoParsedMessage())
                        .collect(Collectors.toList()))
                .build();
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public long getSequence() {
        return sequence;
    }

    public Direction getDirection() {
        return direction;
    }

    public List<ConnectivityMessage> getiMessages() {
        return iMessages;
    }

    private static boolean checkMessages(Collection<ConnectivityMessage> iMessages, String sessionAlias, Direction direction) {
        return iMessages.stream()
                .allMatch(iMessage -> Objects.equals(sessionAlias, iMessage.getSessionAlias())
                        && direction == iMessage.getDirection());
    }
}
