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

#define _GNU_SOURCE
#include <sched.h>

int __sched_cpucount(size_t size, const cpu_set_t *set)
{
	size_t i, j, cnt=0;
	const unsigned char *p = (const void *)set;
	for (i=0; i<size; i++) for (j=0; j<8; j++)
		if (p[i] & (1<<j)) cnt++;
	return cnt;
}
