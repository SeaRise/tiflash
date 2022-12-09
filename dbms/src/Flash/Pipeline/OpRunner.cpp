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

#include <Flash/Pipeline/OpRunner.h>

#include <thread>

namespace DB
{
void IOToken::reset(size_t io_token_num_)
{
    // no lock here.
    assert(0 == used_io_token_num);
    io_token_num = io_token_num_;
}

void OpRunner::reset(size_t cpu_factor_, size_t io_factor_, size_t io_token_num_)
{
    cpu_factor = cpu_factor_;
    io_factor = io_factor_;
    io_token.reset(io_token_num_);
}

size_t OpRunner::doCpuOp()
{
    size_t count = 0;
    size_t seed = 1 + (random() % 100);
    size_t loop_size = 19999999 * cpu_factor;
    for (size_t i = 0; i < loop_size; ++i)
        count += (i % seed);
    return count;
}

void IOToken::pop()
{
    if (io_token_num > 0)
    {
        std::unique_lock<std::mutex> lock(token_mu);
        while (true)
        {
            if (used_io_token_num < io_token_num)
            {
                ++used_io_token_num;
                return;
            }
            cv.wait(lock);
        }
    }
}

void IOToken::push()
{
    if (io_token_num > 0)
    {
        std::lock_guard<std::mutex> lock(token_mu);
        assert(used_io_token_num > 0);
        --used_io_token_num;
        cv.notify_one();
    }
}

void OpRunner::doIOOp()
{
    io_token.pop();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000 * io_factor));
    io_token.push();
}

OpRunner & OpRunner::getInstance()
{
    static OpRunner op_runner;
    return op_runner;
}
} // namespace DB
