# Connect (4.1.1)

The "Connect" component is responsible for the communication with a target system.
This component implements the logic of the interaction protocol, receiving and sending messages from and to the system, respectively.

This project includes only an adapter for using the Sailfish service in the th2 packed into a Docker Image.
This image should be used as a base to implement extensions with the real logic for specific protocols using services in the Sailfish format.

As an example, the [th2-conn-generic](https://github.com/th2-net/th2-conn-generic) project implements the extension for connecting via FIX protocol using standard Sailfish's FIX service.

## Configuration

This configuration should be specified in the custom configuration block in schema editor.

```yaml
session-alias: "connectivity-alias"
workspace: "/home/sailfish/workspace"
type: "th2_service:Your_Service_Type"
name: "your_service"
sessionGroup: "group"
dictionaries:
  MAIN: aliasA
  LEVEL1: aliasB
settings:
  param1: "value1"
```

Parameters:
+ session-alias - that session alias will be set for all messages received or sent by this component. **It should be unique for each "Connect" component**;
+ workspace - the folder inside the container that will contain a plugin adapted to use in the TH2;
+ type - the service type from **services.xml** file. If service name from services.xml file contains `-` symbols they must be replaced with `_` symbol;
+ name - the service name that will be displayed in the events inside the report;
+ settings - the parameters that will be transformed to the actual service's settings specified in the **services.xml** file.
+ maxMessageBatchSize - the limitation for message batch size which connect sends to the first and to the second publish pins with. The default value is set to 100.
+ maxMessageFlushTime - defines maximum time between outgoing message batches in milliseconds. The default value is set to 1000.
+ enableMessageSendingEvent - if this option is set to `true`, connect sends a separate event for every message sent which incomes from the pin with the send attribute. The default value is set to true
+ sessionGroup - parameter will be set for all messages received or sent by this component
+ dictionaries - (optional) dictionaries will be picked up based on its aliases. Exception will be thrown in case of wrong configuration.
+ useTransport - (optional) defines whether the provider should use th2 transport or protobuf protocol for storing messages.
  By default, it has the value `true`.

## Metrics

Connect component produces several metrics related to its activity.
+ th2_conn_incoming_msg_quantity / th2_conn_outgoing_msg_quantity are counter type metrics which are incremented when a message is sent or received via the implemented protocol.  They contain the `session_alias` attribute.

## Extension

You can add the ability to connect to a target system by implementing your own service in the Sailfish format and putting it together with its configuration to the correct places into the base image.

You need to perform the following steps:

1. Create the implementation of the [com.exactpro.sf.services.IService](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/java/com/exactpro/sf/services/IService.java).
   The examples of implementing this interface can be found [here](https://github.com/exactpro/sailfish-core/tree/master/BackEnd/Service).
   If the protocol is already implemented in the Sailfish services you can simply use the dependencies on the service's artifact for that protocol.

2. Create the **services.xml** configuration file that contains the description for services you can use from that "Connect" component.
   You can find the example [here](https://github.com/th2-net/th2-conn-generic/blob/master/conn-fix/src/main/plugin/cfg/services.xml).
   This file must contain:
    + service name - the alias to use it from the "Connect" component;
    + the full class name of the _com.exactpro.sf.services.IService_ interface implementation for the protocol;
    + the full settings' class name for that protocol;
    + the full validator's class name - the optional parameter. _Can be omitted_.

3. Create a file with **VERSION** in the following format (parameters _plugin_alias_ and _name_ can be customized, but you will need to use a different folder on step 4):
    ```yaml
    lightweight: true
    plugin_alias: th2_service
    name: th2_service
    build_number: 0
    revision: 0
    git_hash: 0
    branch: fake
    version: 3.3.0.0
    core_version: 3.3.0
    ```
4. Create you own image based on the current one and put all the files in the correct places in the base image:
    + Create the following directory - **${workspace}/plugins/th2_service**.
      _**${workspace}**_ - it is a folder from the "Connect" configuration.
      If you use the _plugin_alias_ and _name_ different from _th2_service_ in the VERSION file correct the _th2_service_ folder name according to the value that you are using.
      Let's name that directory as **PLUGIN_DIRECTORY** for simplicity. This name will be used in future steps.
    + Artifact with the service(s) implementation(s) and all its dependencies should be put into the following directory - **${PLUGIN_DIRECTORY}/libs**.
    + The configuration file created on the step 2 should be put into the following directory - **${PLUGIN_DIRECTORY}/cfg**.
    + The _VERSION_ file created on step 3 should be put into the following directory - **${PLUGIN_DIRECTORY}/**.

## Pins

Connect has only 1 type of pins for interacting with th2 components.
Messages that were received from / sent to the target system will be sent to the following queue:

- sended raw messages


The "Connect" component uses a queue to send messages. The component subscribes to that pin at the start and waits for the messages.
The messages received from that pin will be sent to the target system.
Also, this component is responsible for maintaining connections and sessions in the cases where this is provided by the communication protocol.
Here you can automatically send heartbeat messages, send a logon/logout, requests to retransmit messages in the event of a gap, etc.

## Custom resources for infra-mgr

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
spec:
  image-name: your.image.repo:42/your_image_name
  image-version: 1.0.0
  type: th2-conn
  custom-config:
    session-alias: "connectivity-alias"
    workspace: "/home/sailfish/workspace"
    type: "th2_service:Your_Service_Type"
    name: "your_service"
    sessionGroup: "group"
    maxMessageBatchSize: 100
    maxMessageFlushTime: 1000
    enableMessageSendingEvent: true
    settings:
      param1: "value1"
  pins:
    - name: send_raw
      connection-type: mq
      attributes: ["raw", "publish", "store"]
    - name: to_send
      connection-type: mq
      attributes: ["send", "raw", "subscribe"]
```

## Release notes

### 4.1.1
+ Fixed conn write sailfish service logs into last sailfish workspace layer problem

### 4.1.0

+ Migrate to th2 gradle plugin `0.1.1`

#### Updated
+ sailfish-core: `3.3.241`
+ bom: `4.6.1`
+ common: `5.14.0-dev`
+ common-utils: `2.2.3-dev`
+ sailfish: `4.1.1-dev`
+ rxjava: `3.1.8`

### 4.0.1

#### Fixed:

+ Exception when processing sent message with event ID

### 4.0.0

+ Migration to books/pages cradle 5.0.0
+ Added ability to define dictionaries via custom config in based on sailfish adapters
+ Add session group support
+ Replace 2 queues with in/out pins to one queue.
+ Messages are not grouped by direction, both direction publish together.

### 3.12.0
+ Added `maxMessageFlushTime` option

### 3.11.2

+ Sailfish updated from `3.3.132` to `3.3.144`
+ Added `maxMessageFlushTime` option
+ Use temporal directory for last layer in sailfish's workspace

### 3.11.1
+ Updated `sailfish-core` version from `3.3.54` to `3.3.132`

### 3.11.0

+ Updated `sailfish-core` version from `3.2.1741` to `3.3.54`
+ Updated `common` from `3.33.0` to `3.44.0`
+ Updated `kotlin` form `1.5.30` to `1.6.21`
+ Renamed project to `conn-sailfish`

### 3.10.2

+ Events are made more convenient. Added event names and error logs. Error message moved from the name to the body of the event.

### 3.10.1

+ Updated `sailfish-core` version from `3.2.1674` to `3.2.1741`
  + Add exception for checking the property in `IMetadata`
+ Added synchronization by processor to `ServiceListener.onMessage()` otherwise processor sometimes misses some sequences
+ Added log about missed sequences
+ Added trace log to flowable processor

### 3.10.0

+ Updated `th2-common` version from `3.25.1` to `3.33.0`
+ Updated `org.jetbrains.kotlin.jvm` version from `1.3.72` to `1.5.30`

### 3.9.0

+ Updated `sailfish-core` version from `3.2.1650` to `3.2.1674`
  + Embedded Sailfish service based on MINA decodes the message as sender during sending. This approach is important for protocols in which a pair of messages have the same protocol message type and different structures depending on the direction. 
+ Updated `th2-common` version from `3.16.5` to `3.25.1`
+ Updated `th2-sailfish-utils` version from `3.4.0` to `3.8.0`
    
### 3.8.1

+ Netty services do not copy metadata to the `IMessage` when sending one. This problem was fixed and now they copy metadata.

### 3.8.0

+ Disable waiting for connection recovery when closing the `SubscribeMonitor`    

### 3.7.2

+ Updated Sailfish version to 3.2.1603

### 3.7.1

+ Updated Sailfish version to 3.2.1572 (unwraps the EvolutionBatch when sending raw message)

### 3.7.0

+ Added maxMessageBatchSize option to configure limitation of message batch size 
+ Added enableMessageSendingEvent option to manage the event emitted related to sent messages
+ Produce th2_conn_incoming_msg_quantity / th2_conn_outgoing_msg_quantity metrics

### 3.6.1

+ Use release version for sailfish-core
+ An alert is sent if it gets an ErrorMessage when sending raw message
+ Copies message properties from the th2 proto Message to Sailfish IMessage when converting

### 3.6.0

+ resets embedded log4j configuration before configuring it from a file

### 3.5.1

+ removed gRPC event loop handling
+ fixed dictionary reading

### 3.5.0

+ reads dictionaries from the /var/th2/config/dictionary folder.
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration
+ updated Cradle version. Introduce async API for storing events

### 3.4.1

+ Netty:
    + Fix incorrect timeout information in `SendMessageFailedException`

### 3.4.0

+ Validates configured dictionaries during initialization

### 3.3.1

+ Support for sending raw messages via Netty services

### 3.3.0

+ Copies the parent event ID from the original raw message to the actual one;
+ Joins all related `IMessage`s to a single raw message;
+ Messages that were sent using this connectivity but did not have any parent event ID
  are attached to the dedicated event for this connectivity.
