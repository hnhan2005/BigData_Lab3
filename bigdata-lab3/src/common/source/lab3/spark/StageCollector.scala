package lab3.spark

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}

import scala.collection.mutable

final class StageCollector(targetJobGroup: String) extends SparkListener {
  private val completed = mutable.HashSet.empty[Int]
  private val targetStages = mutable.HashSet.empty[Int]

  override def onJobStart(event: SparkListenerJobStart): Unit = synchronized {
    val properties = event.properties
    if (properties != null && targetJobGroup == properties.getProperty("spark.jobGroup.id")) {
      targetStages ++= event.stageIds
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = synchronized {
    if (targetStages.contains(event.stageInfo.stageId)) completed += event.stageInfo.stageId
  }

  def stageIds: Set[Int] = synchronized(completed.toSet)
}
