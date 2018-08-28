/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.Dataflow
import com.google.api.services.dataflow.model.Job
import com.google.common.reflect.ClassPath
import com.spotify.scio._
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.StreamingOptions
import org.joda.time._
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration


object ScioStreamingBenchmark {
  import ScioBenchmarkSettings._

  private val dataflow: Dataflow = {
    val transport = GoogleNetHttpTransport.newTrustedTransport()
    val jackson = JacksonFactory.getDefaultInstance
    val credential = GoogleCredential.getApplicationDefault
    new Dataflow.Builder(transport, jackson, credential).build()
  }

  private val benchmarks = ClassPath.from(Thread.currentThread().getContextClassLoader)
    .getAllClasses
    .asScala
    .filter(_.getName.matches("com\\.spotify\\.ScioStreamingBenchmark\\$[\\w]+\\$"))
    .flatMap { ci =>
      val cls = ci.load()
      if (classOf[StreamingBenchmark] isAssignableFrom cls) {
        Some(cls.newInstance().asInstanceOf[StreamingBenchmark])
      } else {
        None
      }
    }

  def main(args: Array[String]): Unit = {
    val argz = Args(args)
    val name = argz("name")
    val regex = argz.getOrElse("regex", ".*")
    val projectId = argz.getOrElse("project", defaultProjectId)
    val timestamp = DateTimeFormat.forPattern("yyyyMMddHHmmss")
      .withZone(DateTimeZone.UTC)
      .print(System.currentTimeMillis())
    val prefix = s"ScioStreamingBenchmark-$name-$timestamp"

    cancelCurrentJobs(projectId)

    val scioResults = benchmarks
      .filter(_.name.matches(regex))
      .map(_.run(projectId, prefix, commonArgs))

    new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
      () => {
        val benchmarks = scioResults.map { case (benchmark, result) =>
          BenchmarkResult.streaming(benchmark, result) }

        DatastoreLogger(circleCIEnv).log(benchmarks)
      },
      1,
      1,
      TimeUnit.MINUTES)

    scioResults.foreach(result => result._2.waitUntilFinish(Duration(1, TimeUnit.DAYS)))
  }

  private val cancelledState = "JOB_STATE_CANCELLED"

  /** Cancel any currently running streaming benchmarks before spawning new ones */
  private def cancelCurrentJobs(projectId: String): Unit = {
    val jobs = dataflow.projects().jobs()

    Option(jobs.list(projectId).setFilter("ACTIVE").execute().getJobs).foreach { activeJobs =>
      activeJobs.asScala.foreach { job =>
        if (job.getName.toLowerCase.startsWith("sciostreamingbenchmark")) {
          PrettyPrint.print("CancelCurrentJobs", s"Stopping job.... ${job.getName}")

          jobs.update(
            projectId,
            job.getId,
            new Job().setProjectId(projectId).setId(job.getId).setRequestedState(cancelledState)
          ).execute()
        }
      }
    }
  }

  // =======================================================================
  // Benchmarks
  // =======================================================================

  // @Todo write actual benchmark job
  object StreamingBenchmarkExample extends StreamingBenchmark {
    override def run(sc: ScioContext): Unit = {
      sc.optionsAs[StreamingOptions].setStreaming(true)

      sc
        .customInput(
          "testJob",
          GenerateSequence
            .from(0)
            .withRate(1, org.joda.time.Duration.standardSeconds(10))
        )
        .withFixedWindows(
          duration = org.joda.time.Duration.standardSeconds(10),
          offset = org.joda.time.Duration.ZERO)
        .map(_ * 10)
    }
  }
}

abstract class StreamingBenchmark {
  val name: String = this.getClass.getSimpleName.replaceAll("\\$$", "")

  def run(projectId: String,
          prefix: String,
          args: Array[String]): (String, ScioResult) = {
    val username = sys.props("user.name")

    val (sc, _) = ContextAndArgs(Array(s"--project=$projectId") ++ args)
    sc.setAppName(name)
    sc.setJobName(s"$prefix-$name-$username".toLowerCase())

    run(sc)

    (name, sc.close())
  }

  def run(sc: ScioContext): Unit
}
