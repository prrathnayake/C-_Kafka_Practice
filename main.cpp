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

void sendMessage(KafkaProducer *kafkaProducer)
{
    while (true)
    {
        std::string input;
        std::cin >> input;

        kafkaProducer->produceMessages("topic", input);
    }
}

int main()
{
    KafkaConsumer *kafkaConsumer = new KafkaConsumer("localhost:9092", "topic");
    std::thread messageReceivingThread(receiveMessage, kafkaConsumer);

    KafkaProducer *kafkaProducer = new KafkaProducer("localhost:9092");
    std::thread messageSendingThread(sendMessage, kafkaProducer);

    messageReceivingThread.join();
    messageSendingThread.join();

    delete kafkaProducer;
    delete kafkaConsumer;

    return 0;
}