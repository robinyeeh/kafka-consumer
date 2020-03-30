package main

import (
	"fmt"
	_ "fmt"
	"github.com/Shopify/sarama"
	"os"
	_ "sync"
	_ "time"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Println("There should be 2 arguments, bootstrap and topic")
		return
	}


	address := []string{args[1]}
	topic := args[2]

	fmt.Printf("Start consuming message from bootstrap : %s, topic : %s", address, topic)

	consumeMessages(address, topic)
}

func consumeMessages(addr []string, topic string) {

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy  = sarama.BalanceStrategyRange
	//config.Version = sarama.V1_0_0_0

	consumer, err := sarama.NewConsumer(addr, config)
	if err != nil {
		fmt.Println("Connect to consumer error : ", err)
		return
	}

	defer consumer.Close()

	//List all partitions
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Println("Get partition failed, err : ", err)
		return
	}

	//Consume each partition of topic
	for _, p := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(topic, p, sarama.OffsetNewest)
		if err != nil {
			fmt.Println("Partition consumer err : ", err)
			continue
		}

		defer partitionConsumer.Close()

		//Consume messages
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				{
					fmt.Printf("msg offset: %d, partition: %d, timestamp: %s, value: %s\n", msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))
				}
			case err := <-partitionConsumer.Errors():
				{
					fmt.Printf("err: %s\n", err.Error())
				}
			}
		}
	}
}
