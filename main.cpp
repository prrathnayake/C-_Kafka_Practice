#include <iostream>
#include <string>
#include <thread>

#include "KafkaProducer.h"
#include "KafkaConsumer.h"
#include "ConsumeCb.h"
#include "Helper.h"
#include "ThreadPool.h"

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
    ThreadPool pool;
    pool.addThread("consumer");
    pool.addThread("producer");

    KafkaConsumer *kafkaConsumer = new KafkaConsumer("localhost:9092", "topic");
    pool.addTask("consumer", std::bind(receiveMessage, kafkaConsumer));

    KafkaProducer *kafkaProducer = new KafkaProducer("localhost:9092");
    pool.addTask("producer", std::bind(sendMessage, kafkaProducer));

    pool.joinAll();

    delete kafkaProducer;

    kafkaConsumer->stopConsumeMessages();
    delete kafkaConsumer;

    return 0;
}