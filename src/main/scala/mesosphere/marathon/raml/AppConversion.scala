package mesosphere.marathon
package raml

import mesosphere.marathon.Protos.ResidencyDefinition
import mesosphere.marathon.state._

import scala.concurrent.duration._

trait AppConversion extends ConstraintConversion with EnvVarConversion with HealthCheckConversion
    with NetworkConversion with ReadinessConversions with SecretConversion with VolumeConversion with UnreachableStrategyConversion with KillSelectionConversion {

  import AppConversion._

  implicit val pathIdWrites: Writes[PathId, String] = Writes { _.toString }

  implicit val artifactWrites: Writes[FetchUri, Artifact] = Writes { fetch =>
    Artifact(fetch.uri, fetch.extract, fetch.executable, fetch.cache)
  }

  implicit val upgradeStrategyWrites: Writes[state.UpgradeStrategy, UpgradeStrategy] = Writes { strategy =>
    UpgradeStrategy(strategy.maximumOverCapacity, strategy.minimumHealthCapacity)
  }

  implicit val appResidencyWrites: Writes[Residency, AppResidency] = Writes { residency =>
    AppResidency(residency.relaunchEscalationTimeoutSeconds, residency.taskLostBehavior.toRaml)
  }

  implicit val versionInfoWrites: Writes[state.VersionInfo, Option[VersionInfo]] = Writes {
    case state.VersionInfo.FullVersionInfo(_, scale, config) => Some(VersionInfo(scale.toOffsetDateTime, config.toOffsetDateTime))
    case state.VersionInfo.OnlyVersion(_) => None
    case state.VersionInfo.NoVersion => None
  }

  implicit val appWriter: Writes[AppDefinition, App] = Writes { app =>
    // we explicitly do not write ports, uris, ipAddress because they are deprecated fields
    App(
      id = app.id.toString,
      acceptedResourceRoles = if (app.acceptedResourceRoles.nonEmpty) Some(app.acceptedResourceRoles) else None,
      args = app.args,
      backoffFactor = app.backoffStrategy.factor,
      backoffSeconds = app.backoffStrategy.backoff.toSeconds.toInt,
      cmd = app.cmd,
      constraints = app.constraints.toRaml[Set[Seq[String]]],
      container = app.container.toRaml,
      cpus = app.resources.cpus,
      dependencies = app.dependencies.map(Raml.toRaml(_)),
      disk = app.resources.disk,
      env = app.env.toRaml,
      executor = app.executor,
      fetch = app.fetch.toRaml,
      gpus = app.resources.gpus,
      healthChecks = app.healthChecks.toRaml,
      instances = app.instances,
      ipAddress = None, // deprecated field
      labels = app.labels,
      maxLaunchDelaySeconds = app.backoffStrategy.maxLaunchDelay.toSeconds.toInt,
      mem = app.resources.mem,
      networks = app.networks.toRaml,
      ports = None, // deprecated field
      portDefinitions = if (app.portDefinitions.nonEmpty) Some(app.portDefinitions.toRaml) else None,
      readinessChecks = app.readinessChecks.toRaml,
      residency = app.residency.toRaml,
      requirePorts = app.requirePorts,
      secrets = app.secrets.toRaml,
      storeUrls = app.storeUrls,
      taskKillGracePeriodSeconds = app.taskKillGracePeriod.map(_.toSeconds.toInt),
      upgradeStrategy = Some(app.upgradeStrategy.toRaml),
      uris = None, // deprecated field
      user = app.user,
      version = Some(app.versionInfo.version.toOffsetDateTime),
      versionInfo = app.versionInfo.toRaml,
      unreachableStrategy = Some(app.unreachableStrategy.toRaml),
      killSelection = Some(app.killSelection.toRaml)
    )
  }

  def resources(cpus: Option[Double], mem: Option[Double], disk: Option[Double], gpus: Option[Int]): Resources =
    Resources(
      cpus = cpus.getOrElse(App.DefaultCpus),
      mem = mem.getOrElse(App.DefaultMem),
      disk = disk.getOrElse(App.DefaultDisk),
      gpus = gpus.getOrElse(App.DefaultGpus)
    )

  implicit val taskLostBehaviorReader: Reads[TaskLostBehavior, ResidencyDefinition.TaskLostBehavior] = Reads { taskLost =>
    import ResidencyDefinition.TaskLostBehavior._
    taskLost match {
      case TaskLostBehavior.RelaunchAfterTimeout => RELAUNCH_AFTER_TIMEOUT
      case TaskLostBehavior.WaitForever => WAIT_FOREVER
    }
  }

  implicit val residencyRamlReader: Reads[AppResidency, Residency] = Reads { residency =>
    Residency(
      relaunchEscalationTimeoutSeconds = residency.relaunchEscalationTimeoutSeconds,
      taskLostBehavior = residency.taskLostBehavior.fromRaml
    )
  }

  implicit val fetchUriReader: Reads[Artifact, FetchUri] = Reads { artifact =>
    FetchUri(
      uri = artifact.uri,
      extract = artifact.extract,
      executable = artifact.executable,
      cache = artifact.cache,
      outputFile = artifact.destPath
    )
  }

  implicit val portDefinitionRamlReader: Reads[PortDefinition, state.PortDefinition] = Reads { portDef =>
    val protocol: String = portDef.protocol match {
      case NetworkProtocol.Tcp => "tcp"
      case NetworkProtocol.Udp => "udp"
      case NetworkProtocol.UdpTcp => "udp,tcp"
    }

    state.PortDefinition(
      port = portDef.port,
      protocol = protocol,
      name = portDef.name,
      labels = portDef.labels
    )
  }

  implicit val upgradeStrategyRamlReader: Reads[UpgradeStrategy, state.UpgradeStrategy] = Reads { us =>
    state.UpgradeStrategy(
      maximumOverCapacity = us.maximumOverCapacity,
      minimumHealthCapacity = us.minimumHealthCapacity
    )
  }

  implicit val appRamlReader: Reads[App, AppDefinition] = Reads[App, AppDefinition] { app =>
    val selectedStrategy = ResidencyAndUpgradeStrategy(
      app.residency.map(Raml.fromRaml(_)),
      app.upgradeStrategy.map(Raml.fromRaml(_)),
      app.container.exists(_.volumes.exists(_.persistent.nonEmpty)),
      app.container.exists(_.volumes.exists(_.external.nonEmpty))
    )

    val backoffStrategy = BackoffStrategy(
      backoff = app.backoffSeconds.seconds,
      maxLaunchDelay = app.maxLaunchDelaySeconds.seconds,
      factor = app.backoffFactor
    )

    val versionInfo = state.VersionInfo.OnlyVersion(app.version.map(Timestamp(_)).getOrElse(Timestamp.now()))

    val result: AppDefinition = AppDefinition(
      id = PathId(app.id),
      cmd = app.cmd,
      args = app.args,
      user = app.user,
      env = Raml.fromRaml(app.env),
      instances = app.instances,
      resources = resources(Some(app.cpus), Some(app.mem), Some(app.disk), Some(app.gpus)),
      executor = app.executor,
      constraints = app.constraints.map(Raml.fromRaml(_))(collection.breakOut),
      fetch = app.fetch.map(Raml.fromRaml(_)),
      storeUrls = app.storeUrls,
      portDefinitions = app.portDefinitions.map(_.map(Raml.fromRaml(_))).getOrElse(Nil),
      requirePorts = app.requirePorts,
      backoffStrategy = backoffStrategy,
      container = app.container.map(Raml.fromRaml(_)),
      healthChecks = app.healthChecks.map(Raml.fromRaml(_)),
      readinessChecks = app.readinessChecks.map(Raml.fromRaml(_)),
      taskKillGracePeriod = app.taskKillGracePeriodSeconds.map(_.second),
      dependencies = app.dependencies.map(PathId(_))(collection.breakOut),
      upgradeStrategy = selectedStrategy.upgradeStrategy,
      labels = app.labels,
      acceptedResourceRoles = app.acceptedResourceRoles.getOrElse(AppDefinition.DefaultAcceptedResourceRoles),
      networks = app.networks.map(Raml.fromRaml(_)),
      versionInfo = versionInfo,
      residency = selectedStrategy.residency,
      secrets = Raml.fromRaml(app.secrets),
      unreachableStrategy = app.unreachableStrategy.map(_.fromRaml).getOrElse(AppDefinition.DefaultUnreachableStrategy),
      killSelection = app.killSelection.getOrElse(KillSelection.DefaultValue).fromRaml
    )
    result
  }

  implicit val appUpdateRamlReader: Reads[(AppUpdate, AppDefinition), App] = Reads { src =>
    val (update: AppUpdate, appDef: AppDefinition) = src
    // for validating and converting the returned App API object
    val app: App = appDef.toRaml
    app.copy(
      // id stays the same
      cmd = update.cmd.orElse(app.cmd),
      args = update.args.getOrElse(app.args),
      user = update.user.orElse(app.user),
      env = update.env.getOrElse(app.env),
      instances = update.instances.getOrElse(app.instances),
      cpus = update.cpus.getOrElse(app.cpus),
      mem = update.mem.getOrElse(app.mem),
      disk = update.disk.getOrElse(app.disk),
      gpus = update.gpus.getOrElse(app.gpus),
      executor = update.executor.getOrElse(app.executor),
      constraints = update.constraints.getOrElse(app.constraints),
      fetch = update.fetch.getOrElse(app.fetch),
      storeUrls = update.storeUrls.getOrElse(app.storeUrls),
      portDefinitions = update.portDefinitions.orElse(app.portDefinitions),
      requirePorts = update.requirePorts.getOrElse(app.requirePorts),
      backoffFactor = update.backoffFactor.getOrElse(app.backoffFactor),
      backoffSeconds = update.backoffSeconds.getOrElse(app.backoffSeconds),
      maxLaunchDelaySeconds = update.maxLaunchDelaySeconds.getOrElse(app.maxLaunchDelaySeconds),
      container = update.container.orElse(app.container),
      healthChecks = update.healthChecks.getOrElse(app.healthChecks),
      readinessChecks = update.readinessChecks.getOrElse(app.readinessChecks),
      dependencies = update.dependencies.getOrElse(app.dependencies),
      upgradeStrategy = update.upgradeStrategy.orElse(app.upgradeStrategy),
      labels = update.labels.getOrElse(app.labels),
      acceptedResourceRoles = update.acceptedResourceRoles.orElse(app.acceptedResourceRoles),
      networks = update.networks.getOrElse(app.networks),
      // versionInfo doesn't change - it's never overridden by an AppUpdate.
      // Setting the version in AppUpdate means that the user wants to revert to that version. In that
      // case, we do not update the current AppDefinition but revert completely to the specified version.
      // For all other updates, the GroupVersioningUtil will determine a new version if the AppDefinition
      // has really changed.
      // Since we return an App, and conversion from App to AppDefinition loses versionInfo, we don't take
      // any special steps here to preserve it; that's the caller's responsibility.
      residency = update.residency.orElse(app.residency),
      secrets = update.secrets.getOrElse(app.secrets),
      taskKillGracePeriodSeconds = update.taskKillGracePeriodSeconds.orElse(app.taskKillGracePeriodSeconds),
      unreachableStrategy = update.unreachableStrategy.orElse(app.unreachableStrategy),
      killSelection = update.killSelection.orElse(app.killSelection)
    )
  }
}

object AppConversion extends AppConversion {

  case class ResidencyAndUpgradeStrategy(residency: Option[Residency], upgradeStrategy: state.UpgradeStrategy)

  object ResidencyAndUpgradeStrategy {
    def apply(
      residency: Option[Residency],
      upgradeStrategy: Option[state.UpgradeStrategy],
      hasPersistentVolumes: Boolean,
      hasExternalVolumes: Boolean): ResidencyAndUpgradeStrategy = {

      import state.UpgradeStrategy.{ empty, forResidentTasks }

      val residencyOrDefault: Option[Residency] =
        residency.orElse(if (hasPersistentVolumes) Some(Residency.default) else None)

      val selectedUpgradeStrategy = upgradeStrategy.getOrElse {
        if (residencyOrDefault.isDefined || hasExternalVolumes) forResidentTasks else empty
      }

      ResidencyAndUpgradeStrategy(residencyOrDefault, selectedUpgradeStrategy)
    }
  }
}
