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

#pragma once

#include <memory>
#include <mutex>

namespace DB
{
struct IOToken
{
    mutable std::mutex token_mu;
    std::condition_variable cv;
    size_t io_token_num = 0;
    size_t used_io_token_num = 0;

    void reset(size_t io_token_num_ = 0);

    void pop();

    void push();
};

struct OpRunner
{
    void reset(size_t cpu_factor_ = 1, size_t io_factor_ = 1, size_t io_token_num_ = 0);

    size_t doCpuOp();
    
    void doIOOp();

    static OpRunner & getInstance();

    size_t cpu_factor = 1;
    size_t io_factor = 1;

    IOToken io_token;
};
} // namespace DB
