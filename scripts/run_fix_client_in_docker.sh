#!/bin/bash

docker run --env-file env.list --name fix-client --link fix-server:fix-server -d th2-connectivity-generic-fix-client:latest