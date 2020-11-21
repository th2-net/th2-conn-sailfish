## How it works

The Connect component is responsible for the communication with the target system. This component implements the logic of the interaction protocol, receives and sends messages from and to the system, respectively.

## Appointment

This project include only adapter logic between Sailfish and th2 packed into Docker Image. This image used in [th2-conn-generic](https://github.com/th2-net/th2-conn-generic) as base.

## Pins

Connect has 4 type of pins for interacting with th2 components. Messages that were received from / sent to the target system will be sent to the following queues:

- incoming parsed messages // it will be removed in future
- outgoing parsed messages // it will be removed in future
- incoming raw messages
- outgoing raw messages

To send messages, a separate queue is used. The Connect component subscribes to that pin at the start and waits for the messages - to be sent to the target system - to come from the pin.
Also, this component is responsible for maintaining connections and sessions in the cases where this is provided by the communication protocol. Here you can automatically send heartbeat messages, send a logon/logout, requests to retransmit messages in the event of a gap, etc.

## Custom resources for infra-mgr

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
spec:
  type: th2-conn
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
