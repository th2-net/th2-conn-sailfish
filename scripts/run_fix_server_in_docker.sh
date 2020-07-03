#!/bin/bash

docker run --env-file env.list --name fix-server -d th2-connectivity-generic-fix-server:latest