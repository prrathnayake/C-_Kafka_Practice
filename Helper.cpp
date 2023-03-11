#include <iostream>
#include <chrono>

#include "Helper.h"

long long int Helper::getTimeInNanoseconds()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

long long int Helper::getTimeInMiliseconds()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

long long int Helper::getTimeInMicroseconds()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

long long int Helper::getTimeInSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void Helper::holdSeconds(int secs)
{
    long long int pre = Helper::getTimeInMiliseconds();
    bool hold = true;
    while (hold)
    {
        long long int now = Helper::getTimeInMiliseconds();
        if (now == (pre + (secs * 1000)))
        {
            hold = false;
        }
    }
}

void Helper::holdMiliseconds(int miliseconds)
{
    long long int pre = Helper::getTimeInMicroseconds();
    bool hold = true;
    while (hold)
    {
        long long int now = Helper::getTimeInMicroseconds();
        if (now == (pre + (miliseconds * 1000)))
        {
            hold = false;
        }
    }
}
