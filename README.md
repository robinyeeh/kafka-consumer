Kafka Consumer with Golang
===========

## Kafka Consumer with Golang

This is used to test if kafka works well and you can build it on CentOS and run it on other Machines.

### Start Kafka

```
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties &
$ ./bin/kafka-server-start.sh config/server.properties &
```

### Install Go

```
$ wget https://studygolang.com/dl/golang/go1.12.7.linux-amd64.tar.gz
$ tar zxvf go1.12.7.linux-amd64.tar.gz -C /usr/lib
$ vi ~/.bash_profile

and add the following lines:
#GOROOT
export GOROOT=/usr/lib/go

#GOPATH
export GOPATH=/root/gocode

#GOPATH bin
export PATH=$PATH:$GOPATH/bin

#GOPATH root bin
export PATH=$PATH:$GOROOT/bin
```

### Go Environment Configuration

```
As there's network issue when using go get to install 3rd party libs in China, you will need to do some configurations
for go proxy
$ cd 
$ vi .bash_profile

and add the following lines:
# Enable the go modules feature
export GO111MODULE=on
# Set the GOPROXY environment variable
export GOPROXY=https://mirrors.aliyun.com/goproxy/

$ source .bash_profile
```

### Build Program

```
$ git clone https://github.com/robinyeeh/kafka-consumer.git
$ cd kafka-consumer
$ go build

$ ./kafka_consumer 127.0.0.1:9092 test_topic
```

### Example Results

```
msg offset: 0, partition: 0, timestamp: 0001-01-01 00:00:00 +0000 UTC, value: asdad
msg offset: 1, partition: 0, timestamp: 0001-01-01 00:00:00 +0000 UTC, value: asdasad 
```