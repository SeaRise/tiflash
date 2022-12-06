// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Flash/Pipeline/IORunner.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <common/likely.h>
#include <common/logger_useful.h>

namespace DB
{
IORunner::IORunner(TaskScheduler & scheduler_, size_t thread_num)
    : scheduler(scheduler_)
{
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
    {
        threads.emplace_back([&] {
            TaskPtr job;
            while (likely(popJob(job)))
            {
                job->doBlockingJob();
                scheduler.submit(std::move(job));
            }
        });
    }
}

IORunner::~IORunner()
{
    {
        std::lock_guard<std::mutex> lock(job_mutex);
        is_closed = true;
    }
    cv.notify_all();
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "stop io runner");
}

bool IORunner::popJob(TaskPtr & task)
{
    {
        std::unique_lock<std::mutex> lock(job_mutex);
        while (true)
        {
            if (unlikely(is_closed))
                return false;
            if (!job_queue.empty())
                break;
            cv.wait(lock);
        }

        task = std::move(job_queue.front());
        job_queue.pop_front();
    }
    return true;
}

void IORunner::submit(TaskPtr && task)
{
    {
        std::lock_guard<std::mutex> lock(job_mutex);
        job_queue.push_back(std::move(task));
    }
    cv.notify_one();
}
} // namespace DB
