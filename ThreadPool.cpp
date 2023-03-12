#include <iostream>
#include "ThreadPool.h"

void ThreadPool::addThread(const std::string &threadName)
{
    if (threads.count(threadName) > 0)
    {
        std::cerr << "Thread with name " << threadName << " already exists.\n";
        return;
    }

    threads[threadName] = std::thread(&ThreadPool::threadFunc, this, threadName);
}

void ThreadPool::addTask(const std::string &threadName, std::function<void()> task)
{
    if (threads.count(threadName) == 0)
    {
        std::cerr << "Thread " << threadName << " does not exist.\n";
        return;
    }

    std::unique_lock<std::mutex> lock(taskMutexes[threadName]);
    if (tasks[threadName].size() > 0)
    {
        std::cerr << "Thread " << threadName << " is already executing a task.\n";
        return;
    }

    tasks[threadName].push(task);
    taskCVs[threadName].notify_one();
}

void ThreadPool::joinAll()
{
    stop = true;
    for (auto &pair : threads)
    {
        std::cout << "Join Thread" << pair.second.get_id() << std::endl;
        pair.second.join();
    }
}

void ThreadPool::threadFunc(const std::string &threadName)
{
    while (!stop)
    {
        std::unique_lock<std::mutex> lock(taskMutexes[threadName]);
        while (tasks[threadName].empty() && !stop)
        {
            taskCVs[threadName].wait(lock);
        }

        if (tasks[threadName].empty())
        {
            continue;
        }

        auto task = tasks[threadName].front();
        tasks[threadName].pop();
        lock.unlock();

        try
        {
            task();
        }
        catch (const std::exception &e)
        {
            std::cerr << "Exception caught in thread " << threadName << ": " << e.what() << std::endl;
        }
    }
}