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
public:
    void SetUp(const benchmark::State &) override
    {
        DynamicThreadPool::global_instance = std::make_unique<DynamicThreadPool>(
            /*fixed_thread_num=*/300,
            std::chrono::milliseconds(100000));
    }

    void TearDown(const benchmark::State &) override
    {
        DynamicThreadPool::global_instance.reset();
    }
};

BENCHMARK_DEFINE_F(PipelineBench, bench)
(benchmark::State & state)
try
{
    const bool is_async = state.range(0);
    const size_t transform_num = state.range(1);

    for (auto _ : state)
    {
        std::vector<TaskPtr> tasks;
        std::vector<bool> bool_vec;
        auto add_task = []() {
            assert(bool_vec.size() >= 2);
            size_t index = 0;
            TaskBuilder task_builder;
            if (bool_vec[index++])
                task_builder.setCPUSource();
            else
                task_builder.setIOSource(is_async);
            for (index < bool_vec.size() - 1; ++index)
            {
                if (bool_vec[index++])
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
        size_t num = (transform_num + 2);
        size_t i = 0;
        auto recursion_func = []() {
            bool_vec.push_back(true);
            ++i;
            recursion_func();

            bool_vec.pop_back();
            --i;

            bool_vec.push_back(false);
            ++i;
            recursion_func();
        };

        for (size_t i = 0; i < num; ++i)
        {
            bool_vec.
        }
        TaskScheduler task_scheduler(std::thread::hardware_concurrency(), tasks);
        task_scheduler.waitForFinish();
    }
}
CATCH
BENCHMARK_REGISTER_F(PipelineBench, bench)
    ->Args({true, 1})
    ->Args({false, 1})
    ->Args({true, 5})
    ->Args({false, 5});
} // namespace DB::tests
