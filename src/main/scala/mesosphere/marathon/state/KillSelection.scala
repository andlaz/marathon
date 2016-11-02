package mesosphere.marathon
package state

/**
  * Defines a kill selection for tasks. See [[mesosphere.marathon.core.deployment.ScalingProposition]].
  */
sealed trait KillSelection {
  def apply(a: Timestamp, b: Timestamp): Boolean = this match {
    case KillSelection.YoungestFirst => a.youngerThan(b)
    case KillSelection.OldestFirst => a.olderThan(b)
  }
  val value: String
}

object KillSelection {

  case object YoungestFirst extends KillSelection {
    override val value = raml.KillSelection.YoungestFirst.value
  }
  case object OldestFirst extends KillSelection {
    override val value = raml.KillSelection.OldestFirst.value
  }

  def DefaultKillSelection: KillSelection = raml.KillSelection.DefaultValue.fromRaml
}
