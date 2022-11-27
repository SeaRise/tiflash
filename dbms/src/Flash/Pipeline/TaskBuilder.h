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

#include <Flash/Pipeline/Sink.h>
#include <Flash/Pipeline/Source.h>
#include <Flash/Pipeline/Task.h>
#include <Flash/Pipeline/Transform.h>

namespace DB
{
class TaskBuilder
{
public:
    TaskBuilder & setCPUSource()
    {
        assert(!source);
        source = std::make_unique<CPUSource>();
        return *this;
    }
    TaskBuilder & setIOSource(bool is_async)
    {
        assert(!source);
        if (is_async)
            source = std::make_unique<AsyncIOSource>();
        else
            source = std::make_unique<SyncIOSource>();
        return *this;
    }
    TaskBuilder & appendCPUTransform()
    {
        transforms.emplace_back(std::make_unique<CPUTransform>());
        return *this;
    }
    TaskBuilder & appendIOTransform(bool is_async)
    {
        if (is_async)
            transforms.emplace_back(std::make_unique<AsyncIOTransform>());
        else
            transforms.emplace_back(std::make_unique<SyncIOTransform>());
        return *this;
    }
    TaskBuilder & setCPUSink()
    {
        assert(!sink);
        sink = std::make_unique<CPUSink>();
        return *this;
    }
    TaskBuilder & setIOSink(bool is_async)
    {
        assert(!sink);
        if (is_async)
            sink = std::make_unique<AsyncIOSink>();
        else
            sink = std::make_unique<SyncIOSink>();
        return *this;
    }

    TaskPtr build()
    {
        assert(source);
        assert(sink);
        return std::make_unique<Task>(std::move(source), std::move(transforms), std::move(sink));
    }

private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;
};
} // namespace DB
