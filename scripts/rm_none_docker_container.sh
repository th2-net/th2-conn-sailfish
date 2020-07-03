#!/bin/bash

docker images -a |  grep "$1" | awk '{print $3}' | xargs docker rmi -f
