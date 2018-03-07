#!/bin/bash

docker rmi -f pipe-to-elasticsearch-worker

docker build -t pipe-to-elasticsearch-worker .
