/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.connectivity.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConnectivityConfiguration {
    @JsonProperty(value = "session-alias", required = true)
    private String sessionAlias;

    @JsonProperty(value = "workspace",required = true)
    private String workspaceFolder;

    @JsonProperty(value = "service-config", required = true)
    private String serviceConfigurationFile;

    public String getSessionAlias() {
        return sessionAlias;
    }

    public String getWorkspaceFolder() {
        return workspaceFolder;
    }

    public String getServiceConfigurationFile() {
        return serviceConfigurationFile;
    }
}
