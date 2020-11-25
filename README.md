# Connect

The "Connect" component is responsible for the communication with the target system.
This component implements the logic of the interaction protocol, by receiving and sending messages from and to the system respectively.

This project includes only one adapter for using the Sailfish service in the th2 package into the Docker Image.
This image should be used as a base to implement extensions with the real logic for the specific protocols, by using services in the Sailfish format.

As an example, the [th2-conn-generic](https://github.com/th2-net/th2-conn-generic) project implements the extension for connecting via FIX protocol using standard Sailfish's FIX service.

## Configuration

This configuration should be specified in the custom configuration block at the schema editor.

```yaml
session-alias: "connectivity-alias"
workspace: "/home/sailfish/workspace"
type: "th2_service:Your_Service_Type"
name: "your_service"
settings:
  param1: "value1"
```

Parameters:
+ session-alias - the session alias specified in this field, will be set for all messages received or sent by this component. **It should be unique for each "Connect" component**;
+ workspace - the folder inside the container which will contain a plugin adapted for using with TH2;
+ type - the service type specified from **services.xml** file. If the service name from services.xml file contains the symbols `-`, they must be replaced with the symbol `_`;
+ name - the service name which will be displayed into the events inside the report;
+ settings - the parameters that will be transformed to the actual service's settings specified in the **services.xml** file.

## Extension

You can add the ability to connect to a target system by implementing your own service in the Sailfish format and by putting it together with its configuration to the correct places into the base image.

You need to perform the following steps:

1. Create the implementation of [com.exactpro.sf.services.IService](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/java/com/exactpro/sf/services/IService.java).
The examples of implementing this interface can be found [here](https://github.com/exactpro/sailfish-core/tree/master/BackEnd/Service).
If the protocol is already implemented in the Sailfish services you can simply use the dependencies on the service's artifact for that protocol.

2. Create the **services.xml** configuration file which contains the description for the services that you can use from the "Connect" component.
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
4. Create you own image based on the current one and put all the files to the correct places into the base image:
    + Create the following directory - **${workspace}/plugins/th2_service**.
    _**${workspace}**_ - is a folder from the "Connect" configuration.
    If you use the _plugin_alias_ and _name_ different from _th2_service_ in the VERSION file correct the _th2_service_ folder name according to the value that you use.
    Let's name that directory as **PLUGIN_DIRECTORY** for simplicity. This name will be used in future steps.
    + Artifact with the service(s) implementation(s) and all its dependencies should be placed into the following directory - **${PLUGIN_DIRECTORY}/libs**.
    + The configuration file created on the step 2 should be placed into the following directory - **${PLUGIN_DIRECTORY}/cfg**.
    + The _VERSION_ file created on step 3 should be placed into the following directory - **${PLUGIN_DIRECTORY}/**.

## Pins

Connect has 4 types of pins for interacting with the th2 components. Messages that were received from / sent to the target system will be directed to the following queues:

- incoming parsed messages // it will be removed in future
- outgoing parsed messages // it will be removed in future
- incoming raw messages
- outgoing raw messages

The "Connect" component uses a separate queue to send messages. The component subscribes to that pin at the start and wait for messages to be received.
The received messages from that pin will be sent to the target system.
Besides, this component is responsible for maintaining connections and sessions , in the cases provided by the communication protocol.
Here you can automatically send heartbeat messages, logon/logout, requests to retransmit messages in the event of a gap, etc.

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
