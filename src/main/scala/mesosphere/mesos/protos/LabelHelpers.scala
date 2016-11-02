package mesosphere.mesos.protos

import mesosphere.marathon.stream.Implicits._
import org.apache.mesos.Protos.{ Label, Labels }

import scala.collection.immutable.Seq

trait LabelHelpers {

  implicit final class MesosLabels(labels: Map[String, String]) {
    def toMesosLabels: Labels = {
      val builder = Labels.newBuilder
      labels.foreach(e => builder.addLabels(Label.newBuilder.setKey(e._1).setValue(e._2).build))
      builder.build
    }

    def toProto: Seq[Label] =
      labels.map { e => Label.newBuilder.setKey(e._1).setValue(e._2).build }(collection.breakOut)
  }

  implicit final class LabelsToMap(labels: Labels) {
    def fromProto: Map[String, String] =
      labels.getLabelsList.collect {
        case label if label.hasKey && label.hasValue => label.getKey -> label.getValue
      }(collection.breakOut)
  }

  implicit final class LabelSeqToMap(labels: Seq[Label]) {
    def fromProto: Map[String, String] =
      labels.collect {
        case label if label.hasKey && label.hasValue => label.getKey -> label.getValue
      }(collection.breakOut)
  }
}

object LabelHelpers extends LabelHelpers