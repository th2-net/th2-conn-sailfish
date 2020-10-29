/******************************************************************************
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
 ******************************************************************************/
@file:JvmName("ConnectivityMetrics")

package com.exactpro.th2.conn.metrics

import com.exactpro.sf.services.ServiceStatus
import io.prometheus.client.Gauge

private val SESSION_STATUS_METRIC = Gauge.build("th2_connectivity_status", "Session status")
    .labelNames("session", "status")
    .register()

/**
 * Publishes session status as a Prometheus metric
 */
fun publishSessionStatus(sessionAlias: String, currentStatus: ServiceStatus) {
    ServiceStatus.values().forEach { status ->
        SESSION_STATUS_METRIC.labels(sessionAlias, status.toString()).set(if (status == currentStatus) 1.0 else 0.0)
    }
}