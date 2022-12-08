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
class PipelineBench : public benchmark::Fixture
{
};

namespace
{
template<typename FF>
void fillBoolVec(std::vector<bool> & bool_vec, size_t trigger_line, FF && ff)
{
    if (bool_vec.size() == trigger_line)
    {
        ff(bool_vec);
        return;
    }

    bool_vec.push_back(true);
    fillBoolVec(bool_vec, trigger_line, std::forward<FF>(ff));
    bool_vec.pop_back();

    bool_vec.push_back(false);
    fillBoolVec(bool_vec, trigger_line, std::forward<FF>(ff));
    bool_vec.pop_back();
}

const Int64 cpu_core_num = std::thread::hardware_concurrency();

TaskScheduler createTaskScheduler(bool is_async)
{
    if (is_async)
        return TaskScheduler{static_cast<size_t>(cpu_core_num), static_cast<size_t>(cpu_core_num)};
    else
        return TaskScheduler{static_cast<size_t>(cpu_core_num * 2), 0};
}
}

BENCHMARK_DEFINE_F(PipelineBench, all_case)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t transform_num = state.range(1);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;

        auto add_task = [is_async, &tasks](const std::vector<bool> & bool_vec) {
            assert(bool_vec.size() >= 2);
            size_t index = 0;
            TaskBuilder task_builder;
            if (bool_vec[index++])
                task_builder.setCPUSource();
            else
                task_builder.setIOSource(is_async);
            for (; index < bool_vec.size() - 1; ++index)
            {
                if (bool_vec[index])
                    task_builder.appendCPUTransform();
                else
                    task_builder.appendIOTransform(is_async);
            }
            if (bool_vec[index])
                task_builder.setCPUSink();
            else
                task_builder.setIOSink(is_async);
            tasks.emplace_back(task_builder.build());
        };

        std::vector<bool> bool_vec;
        size_t num = (transform_num + 2);
        fillBoolVec(bool_vec, num, add_task);

        assert(tasks.size() == pow(2, num));

        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, all_case)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, 5})
    ->Args({true, 5})
    ->Iterations(3);

BENCHMARK_DEFINE_F(PipelineBench, all_cpu)
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
                .appendCPUTransform()
                .appendCPUTransform()
                .appendCPUTransform()
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
BENCHMARK_REGISTER_F(PipelineBench, all_cpu)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, cpu_core_num})
    ->Args({true, cpu_core_num})
    ->Args({false, cpu_core_num * 5})
    ->Args({true, cpu_core_num * 5})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, all_io)
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
                .appendIOTransform(is_async)
                .appendIOTransform(is_async)
                .appendIOTransform(is_async)
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
BENCHMARK_REGISTER_F(PipelineBench, all_io)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, cpu_core_num})
    ->Args({true, cpu_core_num})
    ->Args({false, cpu_core_num * 5})
    ->Args({true, cpu_core_num * 5})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, cpu_io_1)
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
                .appendCPUTransform()
                .appendIOTransform(is_async)
                .appendCPUTransform()
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
BENCHMARK_REGISTER_F(PipelineBench, cpu_io_1)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, cpu_core_num})
    ->Args({true, cpu_core_num})
    ->Args({false, cpu_core_num * 5})
    ->Args({true, cpu_core_num * 5})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, cpu_io_2)
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
                .appendIOTransform(is_async)
                .appendCPUTransform()
                .appendIOTransform(is_async)
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
BENCHMARK_REGISTER_F(PipelineBench, cpu_io_2)
    ->Args({false, 1})
    ->Args({true, 1})
    ->Args({false, cpu_core_num})
    ->Args({true, cpu_core_num})
    ->Args({false, cpu_core_num * 5})
    ->Args({true, cpu_core_num * 5})
    ->Iterations(3)
;

BENCHMARK_DEFINE_F(PipelineBench, cpu_io_3)
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
                .appendCPUTransform()
                .appendCPUTransform()
                .appendCPUTransform()
                .appendCPUTransform()
                .setCPUSink()
                .build());
        }
        for (size_t i = 0; i < io_task_num; ++i)
        {
            tasks.emplace_back(TaskBuilder()
                .setIOSource(is_async)
                .appendIOTransform(is_async)
                .appendIOTransform(is_async)
                .appendIOTransform(is_async)
                .appendIOTransform(is_async)
                .appendIOTransform(is_async)
                .setIOSink(is_async)
                .build());
        }

        assert(tasks.size() == (cpu_task_num + io_task_num));
        auto task_scheduler = createTaskScheduler(is_async);
        task_scheduler.submit(tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, cpu_io_3)
    ->Args({false, 1, 1})
    ->Args({true, 1, 1})
    ->Args({false, cpu_core_num / 2, cpu_core_num / 2})
    ->Args({true, cpu_core_num / 2, cpu_core_num / 2})
    ->Args({false, cpu_core_num * 2, cpu_core_num * 2})
    ->Args({true, cpu_core_num * 2, cpu_core_num * 2})
    ->Args({false, cpu_core_num * 4, cpu_core_num * 1})
    ->Args({true, cpu_core_num * 4, cpu_core_num * 1})
    ->Args({false, cpu_core_num * 1, cpu_core_num * 4})
    ->Args({true, cpu_core_num * 1, cpu_core_num * 4})
    ->Iterations(3)
;
} // namespace DB::tests
