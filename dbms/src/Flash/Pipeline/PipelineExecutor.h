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

#include <Flash/Executor/QueryExecutor.h>
#include <Flash/Pipeline/PipelineExecStatus.h>

namespace DB
{
class Context;

class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;

/**
 * PipelineExecutor is the implementation of the pipeline-base execution model.
 * 
 *                                            ┌──────────────────┐
 *                                            │  task scheduler  │
 *           generate          submit tasks   │                  │
 *  pipeline ────────►  event1 ─────────────► │ ┌──────────────┐ │
 *                                            │ │spill executor│ │
 *                        │ trigger           │ └────▲──┬──────┘ │
 *                        ▼                   │      │  │        │
 *                             submit tasks   │ ┌────┴──▼─────┐  │
 *                      event2 ─────────────► │ │task executor│  │
 *                                            │ └────▲──┬─────┘  │
 *                        │ trigger           │      │  │        │
 *                        ▼                   │  ┌───┴──▼─────┐  │
 *                             submit tasks   │  │wait reactor│  │
 *                      event3 ─────────────► │  └────────────┘  │
 *                                            │                  │
 *                                            └──────────────────┘
 * 
 * As shown above, the pipeline generates a number of events, which are executed in dependency order, 
 * and the events generate a number of tasks that are submitted to the task scheduler for execution.
 */
class PipelineExecutor : public QueryExecutor
{
public:
    PipelineExecutor(
        const ProcessListEntryPtr & process_list_entry_,
        Context & context_,
        const Pipelines & pipelines_);

    String toString() const override;

    void cancel() override;

    int estimateNewThreadCount() override;

protected:
    ExecutionResult execute(ResultHandler result_handler) override;

private:
    Context & context;

    Pipelines pipelines;
    PipelinePtr root_pipeline;

    PipelineExecStatus status;
};
} // namespace DB