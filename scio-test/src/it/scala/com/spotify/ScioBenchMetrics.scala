/*
 * Copyright 2017 Spotify AB.
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

import com.google.api.services.dataflow.model.Job
import com.google.datastore.v1._
import com.google.datastore.v1.client.{Datastore, DatastoreHelper}
import com.spotify.scio._
import com.spotify.scio.runners.dataflow.DataflowResult
import org.apache.beam.sdk.PipelineResult.State
import org.joda.time.format.{ISODateTimeFormat, PeriodFormat}
import org.joda.time.{Instant, LocalDateTime, Seconds}
import shapeless.datatype.datastore._

import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}


final case class CircleCIEnv(buildNum: Long, gitHash: String)

object ScioBenchmarkSettings {
  val defaultProjectId: String = "data-integration-test"
  val numOfWorkers = 4
  val commonArgs = Array(
    "--runner=DataflowRunner",
    s"--numWorkers=$numOfWorkers",
    "--workerMachineType=n1-standard-4",
    "--autoscalingAlgorithm=NONE")

  val shuffleConf = Map("ShuffleService" -> Array("--experiments=shuffle_mode=service"))

  val circleCIEnv: Option[CircleCIEnv] = {
    val isCircleCIRun = sys.env.get("CIRCLECI").contains("true")

    if (isCircleCIRun) {
      (sys.env.get("CIRCLE_BUILD_NUM"), sys.env.get("CIRCLE_SHA1")) match {
        case (Some(buildNumber), Some(gitHash)) =>
          Some(CircleCIEnv(buildNumber.toLong, gitHash))
        case _ => throw new IllegalStateException("CIRCLECI env variable is set but not " +
          "CIRCLE_BUILD_NUM and CIRCLE_SHA1")
      }
    } else {
      PrettyPrint.print("CircleCI", "CIRCLECI env variable not found. Will not publish " +
        "benchmark results to Datastore.")
      None
    }
  }
}


trait BenchmarkLogger[F[_]] {
  def log(benchmarks: Iterable[BenchmarkResult]): F[Unit]
}

final case class ScioBenchmarkLogger[F[_]](loggers: BenchmarkLogger[F]*) {
  def log(benchmarks: BenchmarkResult*): Seq[F[Unit]] =
    loggers.map(_.log(benchmarks))
}

case class BenchmarkResult(name: String,
                           elapsed: Option[Seconds],
                           startTime: LocalDateTime,
                           finishTime: Option[LocalDateTime],
                           state: State,
                           extraArgs: Array[String],
                           metrics: Map[String, String])

object BenchmarkResult {
  val BatchMetrics = Set("Elapsed", "TotalMemoryUsage", "TotalPdUsage",
    "TotalShuffleDataProcessed", "TotalSsdUsage", "TotalStreamingDataProcessed", "TotalVcpuTime")

  private val dateTimeParser = ISODateTimeFormat.dateTimeParser()

  def batch(name: String,
            extraArgs: Array[String],
            scioResult: ScioResult): BenchmarkResult = {
    require(scioResult.isCompleted)

    val job: Job = scioResult.as[DataflowResult].getJob
    val startTime: LocalDateTime = dateTimeParser.parseLocalDateTime(job.getCreateTime)
    val finishTime: LocalDateTime = dateTimeParser.parseLocalDateTime(job.getCurrentStateTime)
    val elapsedTime: Seconds = Seconds.secondsBetween(startTime, finishTime)

    val metrics: Map[String, String] = scioResult.as[DataflowResult]
      .getJobMetrics
      .getMetrics
      .asScala
      .filter { m =>
        m.getName.getName.startsWith("Total") &&
          !m.getName.getContext.containsKey("tentative") &&
          BatchMetrics.contains(m.getName.getName)
      }
      .map(m => (m.getName.getName, m.getScalar.toString))
      .sortBy(_._1)
      .toMap

    BenchmarkResult(
      name, Some(elapsedTime), startTime, Some(finishTime), scioResult.state, extraArgs, metrics)
  }

  def streaming(name: String,
                scioResult: ScioResult): BenchmarkResult = {
    val job: Job = scioResult.as[DataflowResult].getJob
    val startTime: LocalDateTime = dateTimeParser.parseLocalDateTime(job.getCreateTime)

    val metrics = scioResult.as[DataflowResult]
      .getJobMetrics.getMetrics.asScala
      .filter { metric =>
        val name = metric.getName.getName
        name.startsWith("Total") || name.startsWith("Current")
      }.map { metric =>
      PrettyPrint.print(name, s"${metric.getName.getName} -> ${metric.getScalar}")
      metric.getName.getName -> metric.getScalar.toString
    }.toMap

    // @todo better way to store hourly stats...
    val timeKey = new LocalDateTime().toString("yyyy-MM-dd[HH]")
    BenchmarkResult(s"$name-$timeKey", None, startTime, None, scioResult.state, Array(), metrics)
  }
}

object DatastoreLogger {
  final case class ScioBenchmarkRun(timestamp: Instant,
                                    gitHash: String,
                                    buildNum: Long,
                                    operation: String)

  private lazy val Storage: Datastore = DatastoreHelper.getDatastoreFromEnv

  private val Kind = "Benchmarks"

  private val OrderByBuildNumQuery =
    s"SELECT * from ${Kind}_%s ORDER BY buildNum DESC LIMIT 2"

}

final case class DatastoreLogger(circleCIEnv: Option[CircleCIEnv]) extends BenchmarkLogger[Try] {
  import DatastoreLogger._

  // Save metrics to integration testing Datastore instance. Can't make this into a
  // transaction because DS limit is 25 entities per transaction.
  override def log(benchmarks: Iterable[BenchmarkResult]): Try[Unit] = {
    circleCIEnv.map { env =>
      val now = new Instant()
      val dt = DatastoreType[ScioBenchmarkRun]

      val commits = benchmarks.map { benchmark =>
        val entity = dt
          .toEntityBuilder(
            ScioBenchmarkRun(now, env.gitHash, env.buildNum, benchmark.name))
          .setKey(DatastoreHelper.makeKey(
            s"${Kind}_${benchmark.name}", env.buildNum.toString))

        val metrics = benchmark.elapsed match {
          case Some(period) => Map("Elapsed" -> period.getSeconds.toString) ++ benchmark.metrics
          case _ => benchmark.metrics
        }

        metrics.foreach {
          case (key, value) =>
            val entityValue = DatastoreHelper.makeValue(value).build()
            entity.putProperties(key, entityValue)
        }

        Try {
          val commit = Storage.commit(CommitRequest.newBuilder()
            .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
            // Upsert means we can re-run a job for same build if necessary; insert would trigger
            // a Datastore exception
            .addMutations(Mutation.newBuilder().setUpsert(entity.build()).build())
            .build())
          PrettyPrint.print("DS entity", entity.build().toString)

          (benchmark, null)
        }
      }

      commits
        .foldLeft(Try(List[(BenchmarkResult, CommitResponse)]())) {
          case (Success(list), Success(value)) => Success(value :: list)
          case (Success(_), Failure(ex)) => Failure(ex)
          case (f @ Failure(_), _) => f
        }
        .map(_.map(_._1.name))
        .map(printMetricsComparison)
    }.getOrElse {
      Success(Unit)
    }
  }

  // TODO: move this to email generator
  private[this] def printMetricsComparison(benchmarkNames: Iterable[String]): Unit = {
    benchmarkNames.foreach { benchmarkName =>
      try {
        val comparisonMetrics = Storage.runQuery(
          RunQueryRequest.newBuilder().setGqlQuery(
            GqlQuery.newBuilder()
              .setAllowLiterals(true)
              .setQueryString(OrderByBuildNumQuery.format(benchmarkName))
              .build()
          ).build())

        val metrics = comparisonMetrics.getBatch.getEntityResultsList.asScala
          .sortBy(_.getEntity.getKey.getPath(0).getName.toInt)
          .map(_.getEntity)
        if (metrics.size == 2) {
          val opName = metrics.head.getKey.getPath(0).getKind.substring(Kind.length + 1)
          val props = metrics.map(_.getPropertiesMap.asScala)
          PrettyPrint.printSeparator()
          PrettyPrint.print("Benchmark", opName)

          val List(b1, b2) = props.map(_("buildNum").getIntegerValue).toList
          PrettyPrint.print("BuildNum", "%15d%15d%15s".format(b1, b2, "Delta"))

          BenchmarkResult.BatchMetrics.foreach { k =>
            val List(prev, curr) = props.map(_(k).getStringValue.toDouble).toList
            val delta = (curr - prev) / curr * 100.0
            val signed = if (delta.isNaN) {
              "0.00%"
            } else {
              (if (delta > 0) "+" else "") + "%.2f%%".format(delta)
            }
            PrettyPrint.print(k, "%15.2f%15.2f%15s".format(prev, curr, signed))
          }
        }
      } catch {
        case e: Exception =>
          PrettyPrint
            .print(benchmarkName, s"Caught error fetching benchmark metrics from Datastore: $e")
      }
    }
  }
}

final case class ConsoleLogger() extends BenchmarkLogger[Try] {
  override def log(benchmarks: Iterable[BenchmarkResult]): Try[Unit] = Try {
    benchmarks.foreach { benchmark =>
      PrettyPrint.printSeparator()
      PrettyPrint.print("Benchmark", benchmark.name)
      PrettyPrint.print("Extra arguments", benchmark.extraArgs.mkString(" "))
      PrettyPrint.print("State", benchmark.state.toString)
      PrettyPrint.print("Create time", benchmark.startTime.toString())
      PrettyPrint.print("Finish time", benchmark.finishTime.map(_.toString()).getOrElse("N/A"))
      PrettyPrint.print("Elapsed",
        benchmark.elapsed.map(period => PeriodFormat.getDefault.print(period)).getOrElse("N/A"))
      benchmark.metrics.foreach { kv =>
        PrettyPrint.print(kv._1, kv._2)
      }
    }
  }
}
