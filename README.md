## How it works

The Connectivity component is responsible for the communication with the target system. This component implements the logic of the interaction protocol, receives and sends messages from and to the system, respectively.

## Queues

Connectivity has 4 queues for interacting with th2 components. Messages that were received from / sent to the target system will be sent to the following queues:

- incoming parsed messages
- outgoing parsed messages
- incoming raw messages
- outgoing raw messages

To send messages, a separate queue is used. The Connectivity component subscribes to that queue at the start and waits for the messages - to be sent to the target system - to come from the queue.
Also, this component is responsible for maintaining connections and sessions in the cases where this is provided by the communication protocol. Here you can automatically send heartbeat messages, send a logon/logout, requests to retransmit messages in the event of a gap, etc.


## Environment variables

- RABBITMQ_PASS=some_pass
- RABBITMQ_HOST=some-host-name-or-ip
- RABBITMQ_PORT=7777
- RABBITMQ_VHOST=someVhost
- RABBITMQ_USER=some_user
- GRPC_PORT=7878
- SESSION_ALIAS=fix_client
- TH2_EVENT_STORAGE_GRPC_HOST=some-host-name-or-ip
- TH2_EVENT_STORAGE_GRPC_PORT=some-port
- IN_QUEUE_NAME=in_queue
- IN_RAW_QUEUE_NAME=in_raw_queue
- OUT_QUEUE_NAME=out_queue
- OUT_RAW_QUEUE_NAME=out_raw_queue
- TO_SEND_QUEUE_NAME=to_send_queue
- TO_SEND_RAW_QUEUE_NAME=to_send_queue