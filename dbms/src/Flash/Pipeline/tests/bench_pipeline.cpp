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
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <benchmark/benchmark.h>

namespace DB::tests
{
namespace
{
const Int64 cpu_core_num = std::thread::hardware_concurrency();

TaskScheduler createTaskScheduler(bool is_async)
{
    if (is_async)
        return TaskScheduler{static_cast<size_t>(cpu_core_num), static_cast<size_t>(cpu_core_num)};
    else
        return TaskScheduler{static_cast<size_t>(cpu_core_num * 2), 0};
}
}

class PipelineBench : public benchmark::Fixture
{
    void SetUp(const benchmark::State &) override
    {
        OpRunner::getInstance().reset(1, 1, cpu_core_num);
    }
    void TearDown(const benchmark::State &) override
    {
        OpRunner::getInstance().reset();
    }
};

BENCHMARK_DEFINE_F(PipelineBench, cpu_task)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t task_num = state.range(1);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setCPUSource()
                .appendCPUTransform()
                .setCPUSink()
                .build());
        }

        assert(tasks.size() == task_num);
        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, cpu_task)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, cpu_core_num})
    ->Args({true, cpu_core_num})
    ->Args({false, cpu_core_num * 5})
    ->Args({true, cpu_core_num * 5})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, io_task)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t task_num = state.range(1);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setIOSource(is_async)
                .appendIOTransform(is_async)
                .setIOSink(is_async)
                .build());
        }

        assert(tasks.size() == task_num);
        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, io_task)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, cpu_core_num})
    ->Args({true, cpu_core_num})
    ->Args({false, cpu_core_num * 5})
    ->Args({true, cpu_core_num * 5})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, cpu_io_task)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t task_num = state.range(1);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setIOSource(is_async)
                .appendCPUTransform()
                .appendIOTransform(is_async)
                .appendCPUTransform()
                .setIOSink(is_async)
                .build());
        }

        assert(tasks.size() == task_num);
        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, cpu_io_task)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, cpu_core_num})
    ->Args({true, cpu_core_num})
    ->Args({false, cpu_core_num * 5})
    ->Args({true, cpu_core_num * 5})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, cpu_task_and_io_task)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t cpu_task_num = state.range(1);
    const size_t io_task_num = state.range(2);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < cpu_task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setCPUSource()
                .appendCPUTransform()
                .setCPUSink()
                .build());
        }
        for (size_t i = 0; i < io_task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setIOSource(is_async)
                .appendIOTransform(is_async)
                .setIOSink(is_async)
                .build());
        }

        assert(tasks.size() == (cpu_task_num + io_task_num));
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(tasks.begin(), tasks.end(), g);

        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, cpu_task_and_io_task)
    ->Args({false, 1, 1})
    ->Args({true, 1, 1})
    ->Args({false, cpu_core_num / 2, cpu_core_num / 2})
    ->Args({true, cpu_core_num / 2, cpu_core_num / 2})
    ->Args({false, cpu_core_num * 2, cpu_core_num * 2})
    ->Args({true, cpu_core_num * 2, cpu_core_num * 2})
    ->Args({false, cpu_core_num * 4, cpu_core_num / 2})
    ->Args({true, cpu_core_num * 4, cpu_core_num / 2})
    ->Args({false, cpu_core_num * 4, cpu_core_num / 4})
    ->Args({true, cpu_core_num * 4, cpu_core_num / 4})
    ->Args({false, cpu_core_num / 2, cpu_core_num * 4})
    ->Args({true, cpu_core_num / 2, cpu_core_num * 4})
    ->Args({false, cpu_core_num / 4, cpu_core_num * 4})
    ->Args({true, cpu_core_num / 4, cpu_core_num * 4})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, cpu_task_and_cpu_io_task)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t cpu_task_num = state.range(1);
    const size_t cpu_io_task_num = state.range(2);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;
        for (size_t i = 0; i < cpu_task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setCPUSource()
                .appendCPUTransform()
                .setCPUSink()
                .build());
        }
        for (size_t i = 0; i < cpu_io_task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setIOSource(is_async)
                .appendCPUTransform()
                .appendIOTransform(is_async)
                .appendCPUTransform()
                .setIOSink(is_async)
                .build());
        }

        assert(tasks.size() == (cpu_task_num + cpu_io_task_num));
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(tasks.begin(), tasks.end(), g);

        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, cpu_task_and_cpu_io_task)
    ->Args({false, 1, 1})
    ->Args({true, 1, 1})
    ->Args({false, cpu_core_num / 2, cpu_core_num / 2})
    ->Args({true, cpu_core_num / 2, cpu_core_num / 2})
    ->Args({false, cpu_core_num * 2, cpu_core_num * 2})
    ->Args({true, cpu_core_num * 2, cpu_core_num * 2})
    ->Args({false, cpu_core_num * 4, cpu_core_num / 2})
    ->Args({true, cpu_core_num * 4, cpu_core_num / 2})
    ->Args({false, cpu_core_num * 4, cpu_core_num / 4})
    ->Args({true, cpu_core_num * 4, cpu_core_num / 4})
    ->Args({false, cpu_core_num / 2, cpu_core_num * 4})
    ->Args({true, cpu_core_num / 2, cpu_core_num * 4})
    ->Args({false, cpu_core_num / 4, cpu_core_num * 4})
    ->Args({true, cpu_core_num / 4, cpu_core_num * 4})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, cpu_task_and_big_io_task)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t io_factor = state.range(1);

    OpRunner::getInstance().reset(1, io_factor, cpu_core_num);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;
        size_t task_num = cpu_core_num * 5;
        size_t cpu_task_num = cpu_core_num / 4;
        size_t io_task_num = task_num - cpu_task_num;
        for (size_t i = 0; i < cpu_task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setCPUSource()
                .appendCPUTransform()
                .setCPUSink()
                .build());
        }
        for (size_t i = 0; i < io_task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setIOSource(is_async)
                .appendIOTransform(is_async)
                .setIOSink(is_async)
                .build());
        }

        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(tasks.begin(), tasks.end(), g);

        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }

    OpRunner::getInstance().reset(1, 1, cpu_core_num);
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, cpu_task_and_big_io_task)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, 5})
    ->Args({true, 5})
    ->Args({false, 10})
    ->Args({true, 10})
    ->Iterations(3)
;
} // namespace DB::tests
