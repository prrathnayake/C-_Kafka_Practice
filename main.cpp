#include <iostream>
#include <string>
#include <thread>

#include <kafka/KafkaProducer.h>
#include <kafka/KafkaConsumer.h>
#include <kafka/ConsumeCb.h>
#include <kafka/Helper.h>
#include <utils/ThreadPool.h>

void receiveMessage(kafka::KafkaConsumer *kafkaConsumer)
{
    kafka::ExCosumeCb cb;
    kafkaConsumer->consumeMessages(cb);
}

void sendMessage(kafka::KafkaProducer *kafkaProducer)
{
    int count = 0;
    while (count < 10)
    {
        std::string message = std::to_string(kafka::Helper::getTimeInMicroseconds());
        kafkaProducer->produceMessages("topic", message);
        count++;
        kafka::Helper::holdSeconds(1);
    }
}

int main()
{
    utils::ThreadPool pool;
    pool.addThread("consumer");
    pool.addThread("producer");

    kafka::KafkaConsumer *kafkaConsumer = new kafka::KafkaConsumer("localhost:9092", "topic");
    pool.addTask("consumer", std::bind(receiveMessage, kafkaConsumer));

    kafka::KafkaProducer *kafkaProducer = new kafka::KafkaProducer("localhost:9092");
    pool.addTask("producer", std::bind(sendMessage, kafkaProducer));

    int input;
    std::cin >> input;
    kafkaConsumer->stopConsumeMessages();
    delete kafkaProducer;
    delete kafkaConsumer;

    std::cout << "End" << std::endl;

    pool.joinAll();

    return 0;
}