/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework.metronome;

import io.pravega.test.system.framework.metronome.model.v1.Job;
import feign.Headers;
import feign.Param;
import feign.RequestLine;
import feign.Response;

import java.util.List;

/**
 * REST client for https://github.com/dcos/metronome , this project is the replacement for Chronos (No active
 * development is happening on Chronos).
 * Note: Not all REST endpoints have been enabled. This can be done as a future task..
 */
public interface Metronome {

    @RequestLine("GET /v1/jobs")
    @Headers("Content-Type: application/json")
    List<Job> getJobs() throws MetronomeException;

    @RequestLine("POST /v1/jobs")
    @Headers("Content-Type: application/json")
    Job createJob(Job job) throws MetronomeException;

    @RequestLine("GET /v1/jobs/{id}?embed=history&embed=activeRuns")
    @Headers("Content-Type: application/json")
    Job getJob(@Param("id") String id) throws MetronomeException;

    @RequestLine("DELETE /v1/jobs/{id}?stopCurrentJobRuns=true")
    @Headers("Content-Type: application/json")
    Response deleteJob(@Param("id") String id) throws MetronomeException;

    @RequestLine("POST /v1/jobs/{id}/runs")
    Response triggerJobRun(@Param("id") String id) throws MetronomeException;

    @RequestLine("GET /ping")
    String ping() throws MetronomeException;

}
