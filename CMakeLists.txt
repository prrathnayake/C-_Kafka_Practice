cmake_minimum_required(VERSION 3.25.2)

set(CMAKE_CXX_STANDARD 11)

project(kafka)

add_executable(exec main.cpp KafkaProducer.cpp KafkaConsumer.cpp ConsumeCb.cpp DeliveryReportCb.cpp)

target_link_libraries(exec PUBLIC rdkafka++)
