#Steps to build application

git clone https://github.com/prrathnayake/CPP_Kafka_Practice.git
cd CPP_Kafka_Practice
conan install .
conan build .

#To run application
./build/Release/bin/KafkaProject
