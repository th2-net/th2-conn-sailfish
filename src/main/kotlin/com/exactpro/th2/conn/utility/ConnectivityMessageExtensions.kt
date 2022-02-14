package com.exactpro.th2.conn.utility

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.conn.ConnectivityMessage
import java.util.stream.LongStream

@Throws(IllegalStateException::class)
fun List<ConnectivityMessage>.checkForUnorderedSequences() {
    var sequencesUnordered = false
    val missedSequences: MutableList<Long> = ArrayList()

    for (index in 0 until size - 1) {
        val nextExpectedSequence = get(index).sequence + 1
        val nextSequence = get(index + 1).sequence
        if (nextExpectedSequence != nextSequence) {
            sequencesUnordered = true
            LongStream.range(nextExpectedSequence, nextSequence).forEach { e: Long -> missedSequences.add(e) }
        }
    }

    check(sequencesUnordered) { "List ${map { obj: ConnectivityMessage -> obj.sequence }} hasn't elements with incremental sequence with one step ${if (missedSequences.isEmpty()) "" else String.format(". Missed sequences %s", missedSequences)}" }
}

@Throws(IllegalStateException::class)
fun List<ConnectivityMessage>.checkAliasAndDirection(validAlias: String, validDirection: Direction) = check(this.all { validAlias == it.sessionAlias && validDirection == it.direction }) {
    "List $this has elements with incorrect metadata, expected session alias '$validAlias' direction '$validDirection'"
}

