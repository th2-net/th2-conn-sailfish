# Connect (2.9.0)

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
allowUnknownEnumValues: false
settings:
  param1: "value1"
```

Parameters:
+ session-alias - that session alias will be set for all messages received or sent by this component. **It should be unique for each "Connect" component**;
+ workspace - the folder inside the container that will contain a plugin adapted to use in the TH2;
+ type - the service type from **services.xml** file. If service name from services.xml file contains `-` symbols they must be replaced with `_` symbol;
+ name - the service name that will be displayed in the events in the report;
+ allowUnknownEnumValues - allows unknown enum values for messages received via mq to send to the system. By default, it has value of `false`.
+ settings - the parameters that will be transformed to the actual service's settings specified in the **services.xml** file.

## Extension

You can add the ability to connect to a target system by implementing your own service in the Sailfish format and putting it and its configuration to the correct places in the base image.

You need to take the following steps:

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
    version: 3.2.0.0
    core_version: 3.2.0
    ```
4. Create you own image based on the current one and put all files to the correct places in the base image:
    + Create the following directory - **${workspace}/plugins/th2_service**.
    _**${workspace}**_ - it is a folder from the "Connect" configuration.
    If you use the _plugin_alias_ and _name_ different from _th2_service_ in the VERSION file correct the _th2_service_ folder name according to value you use.
    Let's name that directory as **PLUGIN_DIRECTORY** for simplicity. This name will be used in future steps.
    + Artifact with the service(s) implementation(s) and all its dependencies should be put to the following directory - **${PLUGIN_DIRECTORY}/libs**.
    + The configuration file created on the step 2 should be put to the following directory - **${PLUGIN_DIRECTORY}/cfg**.
    + The _VERSION_ file created on step 3 should be put to the following directory - **${PLUGIN_DIRECTORY}/**.

## Pins

Connect has 4 types of pins for interacting with th2 components. Messages that were received from / sent to the target system will be sent to the following queues:

- incoming parsed messages // it will be removed in future
- outgoing parsed messages // it will be removed in future
- incoming raw messages
- outgoing raw messages

The "Connect" component uses a separate queue to send messages. The component subscribes to that pin at the start and waits for the messages.
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
    allowUnknownEnumValues: false
    settings:
      param1: "value1"
  pins:
    - name: in_parsed
      connection-type: mq
      attributes: ["first", "parsed", "publish"]
    - name: in_raw
      connection-type: mq
      attributes: ["first", "raw", "publish", "store"]
    - name: out_parsed
      connection-type: mq
      attributes: ["second", "parsed", "publish"]
    - name: out_raw
      connection-type: mq
      attributes: ["second", "raw", "publish", "store"]
    - name: fix_to_send
      connection-type: mq
      attributes: ["send", "parsed", "subscribe"]
```

## Changes

+ 2.9.0
    + Update common version to 2.19.3
    + Update sailfish utils version to 2.5.0
    + Add `allowUnknownEnumValues` parameter

+ 2.8.1
    + Update Sailfish version to 3.2.1603

+ 2.8.0
    + ability to get service from gRPC router via attributes
    + reading dictionary from new directory (`var/th2/config/directory`)

+ 2.7.0
    + Migration to Sonatype

+ 2.6.2
    + Fix problem with incorrect parent event ID when failed to send a message

+ 2.6.1
    + Use `sailfish-utils` with corrected logging output
    + Netty:
        + Fix incorrect timeout information in `SendMessageFailedException`

+ 2.6.0
    + Validates configured dictionaries during initialization