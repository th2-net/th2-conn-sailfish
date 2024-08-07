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

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@SuppressWarnings("ClassNamePrefixedWithPackageName")
public class ConnectivityBatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityBatch.class);

    private final String bookName;
    private final String sessionAlias;
    private final String group;
    private final long sequence;
    private final List<ConnectivityMessage> connectivityMessages;

    public ConnectivityBatch(List<ConnectivityMessage> connectivityMessages) {
        if (Objects.requireNonNull(connectivityMessages, "Message list can't be null").isEmpty()) {
            throw new IllegalArgumentException("Message list can't be empty");
        }

        ConnectivityMessage firstMessage = connectivityMessages.get(0);
        this.bookName = firstMessage.getBookName();
        this.sessionAlias = firstMessage.getSessionAlias();
        this.group = firstMessage.getGroup();
        checkMessages(connectivityMessages, bookName, sessionAlias);

        this.sequence = firstMessage.getSequence();
        this.connectivityMessages = List.copyOf(connectivityMessages);
    }

    public String getBookName() {
        return bookName;
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public String getGroup() {
        return group;
    }

    public long getSequence() {
        return sequence;
    }

    public List<ConnectivityMessage> getMessages() {
        return connectivityMessages;
    }

    private static void checkMessages(List<ConnectivityMessage> iMessages, String bookName, String sessionAlias) {
        if (iMessages.isEmpty()) {
            throw new IllegalArgumentException("List can't be empty");
        }

        if (!iMessages.stream()
                .allMatch(iMessage -> Objects.equals(bookName, iMessage.getBookName())
                        && Objects.equals(sessionAlias, iMessage.getSessionAlias()))
        ) {
            throw new IllegalArgumentException(String.format(
                    "List %s has elements with incorrect metadata, expected book name '%s', session alias '%s'",
                    iMessages,
                    bookName,
                    sessionAlias
            ));
        }
        if (LOGGER.isErrorEnabled()) {
            Map<Direction, List<ConnectivityMessage>> directionToMessages = iMessages.stream().collect(Collectors.groupingBy(ConnectivityMessage::getDirection));
            directionToMessages.forEach((direction, messages) -> checkDirectionMessages(messages, direction, sessionAlias));
        }
        // FIXME: Replace logging to thowing exception after solving message reordering problem
//            throw new IllegalArgumentException("List " + iMessages.stream()
//                    .map(ConnectivityMessage::getSequence)
//                    .collect(Collectors.toList())+ " hasn't elements with incremental sequence with one step");
    }

    private static void checkDirectionMessages(List<ConnectivityMessage> iMessages, Direction direction, String sessionAlias) {
        boolean sequencesUnordered = false;
        List<Long> missedSequences = new ArrayList<>();
        for (int index = 0; index < iMessages.size() - 1; index++) {
            long nextExpectedSequence = iMessages.get(index).getSequence() + 1;
            long nextSequence = iMessages.get(index + 1).getSequence();
            if (nextExpectedSequence != nextSequence) {
                sequencesUnordered = true;
            }
            LongStream.range(nextExpectedSequence, nextSequence).forEach(missedSequences::add);
        }
        if (sequencesUnordered) {
            LOGGER.error(
                    "List {} hasn't elements with incremental sequence with one step for session alias '{}' and direction '{}'{}",
                    iMessages.stream()
                            .map(ConnectivityMessage::getSequence)
                            .collect(Collectors.toList()),
                    sessionAlias,
                    direction,
                    missedSequences.isEmpty()
                            ? ""
                            : String.format(". Missed sequences %s", missedSequences)
            );
        }
    }
}
