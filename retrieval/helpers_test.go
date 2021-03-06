// Copyright 2013 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retrieval

import (
	"time"

	"github.com/prometheus/client_golang/extraction"
)

type literalScheduler time.Time

func (s literalScheduler) ScheduledFor() time.Time {
	return time.Time(s)
}

func (s literalScheduler) Reschedule(earliest time.Time, future TargetState) {
}

type nopIngester struct{}

func (i nopIngester) Ingest(*extraction.Result) error {
	return nil
}
