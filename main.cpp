#include <iostream>
#include <string>
#include <thread>

#include "KafkaProducer.h"
#include "KafkaConsumer.h"
#include "ConsumeCb.h"
#include "Helper.h"

#include <librdkafka/rdkafkacpp.h>

void receiveMessage(KafkaConsumer *kafkaConsumer)
{
    ExCosumeCb cb;
    kafkaConsumer->consumeMessages(cb);
}

void sendMessage(KafkaProducer *kafkaProducer)
{
    int count = 0;
    while (count < 10)
    {
        std::string message = std::to_string(Helper::getTimeInMicroseconds());
        kafkaProducer->produceMessages("topic", message);
        count++;
        Helper::holdSeconds(1);
    }
}

int main()
{
    KafkaConsumer *kafkaConsumer = new KafkaConsumer("localhost:9092", "topic");
    std::thread messageReceivingThread(receiveMessage, kafkaConsumer);

    KafkaProducer *kafkaProducer = new KafkaProducer("localhost:9092");
    std::thread messageSendingThread(sendMessage, kafkaProducer);

    messageSendingThread.join();
    delete kafkaProducer;

    kafkaConsumer->stopConsumeMessages();
    messageReceivingThread.join();
    delete kafkaConsumer;

    return 0;
}