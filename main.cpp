#include <iostream>
#include <string>
#include <thread>

#include "KafkaProducer.h"
#include "KafkaConsumer.h"
#include "ConsumeCb.h"

#include <librdkafka/rdkafkacpp.h>

void receiveMessage(KafkaConsumer *kafkaConsumer)
{
    ExCosumeCb cb;
    kafkaConsumer->consumeMessages(cb);
}

int main()
{
    KafkaConsumer *kafkaConsumer = new KafkaConsumer("localhost:9092", "topic");

    std::thread receive(receiveMessage, kafkaConsumer);

    KafkaProducer *kafkaProducer = new KafkaProducer("localhost:9092");

    while (true)
    {
        std::string input;
        std::cin >> input;

        kafkaProducer->produceMessages("topic", input);
    }

    receive.join();

    delete kafkaConsumer;

    return 0;
}