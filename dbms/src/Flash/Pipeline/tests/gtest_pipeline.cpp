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

#include <Flash/Pipeline/TaskBuilder.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class PipelineRunner : public ::testing::Test
{
    void SetUp() override
    {
        OpRunner::getInstance().reset(0, 0, 0);
    }

    void TearDown() override
    {
        OpRunner::getInstance().reset();
    }
};

namespace
{
const Int64 cpu_core_num = std::thread::hardware_concurrency();
}

TEST_F(PipelineRunner, empty)
{
    std::vector<TaskPtr> tasks;
    TaskScheduler task_scheduler(cpu_core_num, cpu_core_num);
    task_scheduler.submit(tasks);
    task_scheduler.waitForFinish();
}

TEST_F(PipelineRunner, all_cpu)
{
    std::vector<TaskPtr> tasks;
    tasks.emplace_back(TaskBuilder().setCPUSource().appendCPUTransform().setCPUSink().build());
    TaskScheduler task_scheduler(cpu_core_num, cpu_core_num);
    task_scheduler.submit(tasks);
    task_scheduler.waitForFinish();
}

TEST_F(PipelineRunner, all_io)
{
    auto tester = [](bool is_async) {
        std::vector<TaskPtr> tasks;
        tasks.emplace_back(TaskBuilder().setIOSource(is_async).appendIOTransform(is_async).setIOSink(is_async).build());
        TaskScheduler task_scheduler(cpu_core_num, cpu_core_num);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    };

    tester(true);
    tester(false);
}

TEST_F(PipelineRunner, io_cpu)
{
    auto tester = [](bool is_async) {
        std::vector<TaskPtr> tasks;
        tasks.emplace_back(TaskBuilder().setCPUSource().appendCPUTransform().setIOSink(is_async).build());
        tasks.emplace_back(TaskBuilder().setCPUSource().appendIOTransform(is_async).setIOSink(is_async).build());
        tasks.emplace_back(TaskBuilder().setCPUSource().appendIOTransform(is_async).setCPUSink().build());
        tasks.emplace_back(TaskBuilder().setIOSource(is_async).appendIOTransform(is_async).setCPUSink().build());
        tasks.emplace_back(TaskBuilder().setIOSource(is_async).appendCPUTransform().setCPUSink().build());
        tasks.emplace_back(TaskBuilder().setIOSource(is_async).appendCPUTransform().setIOSink(is_async).build());
        TaskScheduler task_scheduler(cpu_core_num, cpu_core_num);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    };

    tester(true);
    tester(false);
}
} // namespace DB::tests
