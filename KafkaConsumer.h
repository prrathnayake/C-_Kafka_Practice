#pragma once
#include <iostream>
#include <librdkafka/rdkafkacpp.h>

#include "ConsumeCb.h"

class KafkaConsumer
{
public:
    RdKafka::Consumer *consumer;
    RdKafka::Topic *C_topic;
    bool consume = true;

    KafkaConsumer(std::string brokers, std::string topics);

    void consumeMessages(ExCosumeCb ex_consume_cb);

    void stopConsumeMessages();

    ~KafkaConsumer();
};
