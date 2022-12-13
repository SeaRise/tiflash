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

#include <Common/FmtUtils.h>
#include <Flash/Pipeline/Event.h>
#include <Flash/Pipeline/FinalizePipelineEvent.h>
#include <Flash/Pipeline/GetResultSink.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/PipelineEvent.h>
#include <Flash/Pipeline/PipelineExecStatus.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Operators/OperatorBuilder.h>

namespace DB
{
namespace
{
void addPrefix(FmtBuffer & buffer, size_t level)
{
    buffer.append(String(level, ' '));
}
} // namespace

void Pipeline::addPlan(const PhysicalPlanNodePtr & plan)
{
    plans.push_back(plan);
}

void Pipeline::addDependency(const PipelinePtr & dependency)
{
    dependencies.emplace_back(dependency);
}

String Pipeline::toString() const
{
    if (plans.empty())
        return "";
    FmtBuffer buffer;
    buffer.append("[");
    buffer.joinStr(
        plans.cbegin(),
        plans.cend(),
        [](const auto & item, FmtBuffer & buf) { buf.append(item->toString()); },
        "] <== [");
    buffer.append("]");
    return buffer.toString();
}

void Pipeline::toTreeString(FmtBuffer & buffer, size_t level) const
{
    addPrefix(buffer, level);
    buffer.fmtAppend("{}\n", toString());
    ++level;
    for (const auto & dependency : dependencies)
    {
        auto dependency_ptr = dependency.lock();
        assert(dependency_ptr);
        dependency_ptr->toTreeString(buffer, level);
    }
}

void Pipeline::addGetResultSink(ResultHandler result_handler)
{
    assert(!plans.empty());
    auto get_result_sink = PhysicalGetResultSink::build(result_handler, plans.front());
    plans.push_front(get_result_sink);
}

OperatorExecutorGroups Pipeline::transform(Context & context, size_t concurrency)
{
    OperatorsBuilder builder;
    for (auto it = plans.rbegin(); it != plans.rend(); ++it)
        (*it)->transform(builder, context, concurrency);
    return builder.build();
}

Events Pipeline::toEvents(PipelineExecStatus & status, Context & context, size_t concurrency)
{
    Events events;
    auto pipeline_event = std::make_shared<PipelineEvent>(status, context, concurrency, shared_from_this());
    for (const auto & dependency : dependencies)
    {
        auto dependency_ptr = dependency.lock();
        assert(dependency_ptr);
        auto dependency_events = dependency_ptr->toEvents(status, context, concurrency);
        if (!dependency_events.empty())
        {
            pipeline_event->addDependency(dependency_events.back());
            events.insert(events.end(), dependency_events.begin(), dependency_events.end());
        }
    }
    events.push_back(pipeline_event);
    auto finalize_event = std::make_shared<FinalizePipelineEvent>(status);
    finalize_event->addDependency(pipeline_event);
    events.push_back(finalize_event);
    return events;
}

bool Pipeline::isSupported(const tipb::DAGRequest & dag_request)
{
    bool is_supported = true;
    traverseExecutors(
        &dag_request,
        [&](const tipb::Executor & executor) {
            switch (executor.tp())
            {
            case tipb::ExecType::TypeTableScan:
            case tipb::ExecType::TypeSelection:
            case tipb::ExecType::TypeExchangeSender:
            case tipb::ExecType::TypeExchangeReceiver:
            case tipb::ExecType::TypeProjection:
                return true;
            default:
                is_supported = false;
                return false;
            }
        });
    return is_supported;
}
} // namespace DB
