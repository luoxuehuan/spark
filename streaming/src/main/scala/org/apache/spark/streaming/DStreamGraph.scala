/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.util.Utils


/**
  *  DStreamGraph就是RDD的静态模板，来表示空间的处理逻辑具体该怎么做，随着时间的流逝，会将模板实例化。
  *
  *  在时间间隔中用数据来填充模板，然后就变成了RDD的Graph.
  *
  *  动态的Job控制器：它会根据我们设定的时间间隔收集到数据让我们的DStreamGraph变成RDD DAG.
  *
  *  a)  DAG生成模板 ：DStreamGraph
b)  Timeline的Job生成：
c)  输入和输出流
d)  具体的容错
e)  事务处理：绝大多数情况下，数据流进来一定被处理，而且仅被处理一次。


  Spark Streaming系统的容错是基于DStreamGraph这个模板，
不断的根据时间间隔产生Graph也就是DAG依赖的实例，产生具体的作业，
作业具体运行时基于RDD，也就是对于单个Job的容错是基于RDD的。


  Spark Streaming是一个框架，本身也有自己的容错方式，例如数据接收的太多处理不完，这个时候Spark Streaming就会限流。

   Spark Streaming会根据数据的大小，动态的调整CPU和内存等计算资源。比如数据多的话，用更多的资源。
  */
/*
Spark Streaming中不断的有数据流进来，
他会把数据积攒起来，积攒的依据是以Batch Interval的方式进行积攒的，
例如1秒钟，但是这1秒钟里面会有很多的数据例如event，event就构成了一个数据的集合，
而RDD处理的时候，是基于固定不变的集合产生RDD。实际上10秒钟产生一个作业的话，就基于这10个event进行处理，
对于连续不断的流进来的数据，就会根据这个连续不断event构成batch，
因为时间间隔是固定的，所以每个时间间隔产生的数据也是固定的，基于这些batch就会生成RDD的依赖关系。

这里的RDD依赖关系是基于时间间隔中的一个batch中的数据。随着时间的流逝，产生了不同RDD的Graph依赖关系的实例，但是其实RDD的Graph的依赖关系都是一样的。
DStream Graph是RDD的Graph的模板，因为RDD的Graph只是DStreamGraph上空间维度上的而已。


所以从整个Spark Streaming运行角度来看，
由于运行在Spark Core上需要一种机制表示RDD DAG的处理逻辑，也就是空间维度，所以就产生了DStreamGraph.


 */
final private[streaming] class DStreamGraph extends Serializable with Logging {

  private val inputStreams = new ArrayBuffer[InputDStream[_]]()
  private val outputStreams = new ArrayBuffer[DStream[_]]()

  var rememberDuration: Duration = null
  var checkpointInProgress = false

  var zeroTime: Time = null
  var startTime: Time = null
  var batchDuration: Duration = null

  def start(time: Time) {
    this.synchronized {
      require(zeroTime == null, "DStream graph computation already started")
      zeroTime = time
      startTime = time
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validateAtStart)
      inputStreams.par.foreach(_.start())
    }
  }

  def restart(time: Time) {
    this.synchronized { startTime = time }
  }

  def stop() {
    this.synchronized {
      inputStreams.par.foreach(_.stop())
    }
  }

  def setContext(ssc: StreamingContext) {
    this.synchronized {
      outputStreams.foreach(_.setContext(ssc))
    }
  }

  def setBatchDuration(duration: Duration) {
    this.synchronized {
      require(batchDuration == null,
        s"Batch duration already set as $batchDuration. Cannot set it again.")
      batchDuration = duration
    }
  }

  def remember(duration: Duration) {
    this.synchronized {
      require(rememberDuration == null,
        s"Remember duration already set as $rememberDuration. Cannot set it again.")
      rememberDuration = duration
    }
  }

  def addInputStream(inputStream: InputDStream[_]) {
    this.synchronized {
      inputStream.setGraph(this)
      inputStreams += inputStream
    }
  }

  def addOutputStream(outputStream: DStream[_]) {
    this.synchronized {
      outputStream.setGraph(this)
      outputStreams += outputStream
    }
  }

  def getInputStreams(): Array[InputDStream[_]] = this.synchronized { inputStreams.toArray }

  def getOutputStreams(): Array[DStream[_]] = this.synchronized { outputStreams.toArray }

  def getReceiverInputStreams(): Array[ReceiverInputDStream[_]] = this.synchronized {
    inputStreams.filter(_.isInstanceOf[ReceiverInputDStream[_]])
      .map(_.asInstanceOf[ReceiverInputDStream[_]])
      .toArray
  }

  def getInputStreamName(streamId: Int): Option[String] = synchronized {
    inputStreams.find(_.id == streamId).map(_.name)
  }

  def generateJobs(time: Time): Seq[Job] = {
    logDebug("Generating jobs for time " + time)
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }
    }
    logDebug("Generated " + jobs.length + " jobs for time " + time)
    jobs
  }

  def clearMetadata(time: Time) {
    logDebug("Clearing metadata for time " + time)
    this.synchronized {
      outputStreams.foreach(_.clearMetadata(time))
    }
    logDebug("Cleared old metadata for time " + time)
  }

  def updateCheckpointData(time: Time) {
    logInfo("Updating checkpoint data for time " + time)
    this.synchronized {
      outputStreams.foreach(_.updateCheckpointData(time))
    }
    logInfo("Updated checkpoint data for time " + time)
  }

  def clearCheckpointData(time: Time) {
    logInfo("Clearing checkpoint data for time " + time)
    this.synchronized {
      outputStreams.foreach(_.clearCheckpointData(time))
    }
    logInfo("Cleared checkpoint data for time " + time)
  }

  def restoreCheckpointData() {
    logInfo("Restoring checkpoint data")
    this.synchronized {
      outputStreams.foreach(_.restoreCheckpointData())
    }
    logInfo("Restored checkpoint data")
  }

  def validate() {
    this.synchronized {
      require(batchDuration != null, "Batch duration has not been set")
      // assert(batchDuration >= Milliseconds(100), "Batch duration of " + batchDuration +
      // " is very low")
      require(getOutputStreams().nonEmpty, "No output operations registered, so nothing to execute")
    }
  }

  /**
   * Get the maximum remember duration across all the input streams. This is a conservative but
   * safe remember duration which can be used to perform cleanup operations.
   */
  def getMaxInputStreamRememberDuration(): Duration = {
    // If an InputDStream is not used, its `rememberDuration` will be null and we can ignore them
    inputStreams.map(_.rememberDuration).filter(_ != null).maxBy(_.milliseconds)
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    logDebug("DStreamGraph.writeObject used")
    this.synchronized {
      checkpointInProgress = true
      logDebug("Enabled checkpoint mode")
      oos.defaultWriteObject()
      checkpointInProgress = false
      logDebug("Disabled checkpoint mode")
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug("DStreamGraph.readObject used")
    this.synchronized {
      checkpointInProgress = true
      ois.defaultReadObject()
      checkpointInProgress = false
    }
  }
}

