# pipe-to-elasticsearch-worker
 Takes the content of a named pipe (here, fed from Kafka) and put it in an Elasticsearch index. 
 
## Build
Build the Docker image (if you face an error you may need to add yourself to the Docker group: `sudo usermod -aG docker $USER`, then logout and log back to continue):

```
$ ./build.sh
```

## Run
Now run the worker container, here is an example of running configuration:

```
$ docker run -t -i --rm --name feeder --net host pipe-to-elasticsearch-worker -en $ES_IP -i kafka3 -p /tmp/fifo -n 300000 -nt 4 -s 99 -f 180000 -ki $KAFKA_IP -kp $KAFKA_PORT -t toprocess -g elasticsearch -c
```

Another useful option is `-l` (lower-case L) followed by the number of documents you want to limit the feeder to feed.

Further informations on the parameters can be found using the `-h` parameter.
