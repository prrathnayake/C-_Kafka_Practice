#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>

#include "DeliveryReportCb.cpp"
#include "KafkaProducer.h"

KafkaProducer::KafkaProducer(std::string brokers)
{
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    std::string errstr;

    if (conf->set("bootstrap.servers", brokers, errstr) !=
        RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    ExampleDeliveryReportCb ex_dr_cb;

    if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    producer = RdKafka::Producer::create(conf, errstr);
    if (!producer)
    {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;
}

void KafkaProducer::produceMessages(std::string topic, std::string message)
{

retry:
    RdKafka::ErrorCode err = producer->produce(
        topic,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char *>(message.c_str()), message.size(),
        NULL, 0,
        0,
        NULL,
        NULL);

    if (err != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "% Failed to produce to topic " << topic << ": "
                  << RdKafka::err2str(err) << std::endl;

        if (err == RdKafka::ERR__QUEUE_FULL)
        {
            producer->poll(1000);
            goto retry;
        }
    }
}

KafkaProducer::~KafkaProducer()
{
    std::cerr << "% Flushing final messages..." << std::endl;
    std::cerr << "1" << std::endl;
    producer->flush(10 * 1000);
    std::cerr << "2" << std::endl;
    if (producer->outq_len() > 0)
        std::cerr << producer->outq_len()
                  << " message(s) were not delivered" << std::endl;
    std::cerr << "3" << std::endl;
    delete producer;
}