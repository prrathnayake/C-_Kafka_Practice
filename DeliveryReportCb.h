#pragma once
#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>

class DeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
    void dr_cb(RdKafka::Message &message);
};