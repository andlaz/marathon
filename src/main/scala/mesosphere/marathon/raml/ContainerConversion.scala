package mesosphere.marathon
package raml

import mesosphere.marathon.core.pod.MesosContainer
import mesosphere.marathon.state.Parameter
import mesosphere.marathon.stream.Implicits._
import mesosphere.mesos.protos.Implicits._
import org.apache.mesos.{ Protos => Mesos }

trait ContainerConversion extends HealthCheckConversion with VolumeConversion {

  implicit val containerRamlWrites: Writes[MesosContainer, PodContainer] = Writes { c =>
    PodContainer(
      name = c.name,
      exec = c.exec,
      resources = c.resources,
      endpoints = c.endpoints,
      image = c.image,
      environment = Raml.toRaml(c.env),
      user = c.user,
      healthCheck = c.healthCheck.toRaml[Option[HealthCheck]],
      volumeMounts = c.volumeMounts,
      artifacts = c.artifacts,
      labels = c.labels,
      lifecycle = c.lifecycle
    )
  }

  implicit val containerRamlReads: Reads[PodContainer, MesosContainer] = Reads { c =>
    MesosContainer(
      name = c.name,
      exec = c.exec,
      resources = c.resources,
      endpoints = c.endpoints,
      image = c.image,
      env = Raml.fromRaml(c.environment),
      user = c.user,
      healthCheck = c.healthCheck.map(Raml.fromRaml(_)),
      volumeMounts = c.volumeMounts,
      artifacts = c.artifacts,
      labels = c.labels,
      lifecycle = c.lifecycle
    )
  }

  implicit val parameterWrites: Writes[state.Parameter, DockerParameter] = Writes { param =>
    DockerParameter(param.key, param.value)
  }

  implicit val containerWrites: Writes[state.Container, Container] = Writes { container =>

    implicit val credentialWrites: Writes[state.Container.Credential, DockerCredentials] = Writes { credentials =>
      DockerCredentials(credentials.principal, credentials.secret)
    }

    import Mesos.ContainerInfo.DockerInfo.{ Network => DockerNetworkMode }
    implicit val dockerNetworkInfoWrites: Writes[DockerNetworkMode, DockerNetwork] = Writes {
      case DockerNetworkMode.BRIDGE => DockerNetwork.Bridge
      case DockerNetworkMode.HOST => DockerNetwork.Host
      case DockerNetworkMode.USER => DockerNetwork.User
      case DockerNetworkMode.NONE => DockerNetwork.None
    }

    implicit val dockerDockerContainerWrites: Writes[state.Container.Docker, DockerContainer] = Writes { container =>
      DockerContainer(
        forcePullImage = container.forcePullImage,
        image = container.image,
        parameters = container.parameters.toRaml,
        privileged = container.privileged)
    }

    implicit val mesosDockerContainerWrites: Writes[state.Container.MesosDocker, DockerContainer] = Writes { container =>
      DockerContainer(
        image = container.image,
        credential = container.credential.toRaml,
        forcePullImage = container.forcePullImage)
    }

    implicit val mesosContainerWrites: Writes[state.Container.MesosAppC, AppCContainer] = Writes { container =>
      AppCContainer(container.image, container.id, container.labels, container.forcePullImage)
    }

    def create(kind: EngineType, docker: Option[DockerContainer] = None, appc: Option[AppCContainer] = None): Container = {
      Container(kind, docker = docker, appc = appc, volumes = container.volumes.toRaml, portMappings = container.portMappings.toRaml)
    }

    container match {
      case docker: state.Container.Docker => create(EngineType.Docker, docker = Some(docker.toRaml[DockerContainer]))
      case mesos: state.Container.MesosDocker => create(EngineType.Mesos, docker = Some(mesos.toRaml[DockerContainer]))
      case mesos: state.Container.MesosAppC => create(EngineType.Mesos, appc = Some(mesos.toRaml[AppCContainer]))
      case mesos: state.Container.Mesos => create(EngineType.Mesos)
    }
  }

  implicit val portMappingRamlReader: Reads[ContainerPortMapping, state.Container.PortMapping] = Reads {
    case ContainerPortMapping(containerPort, hostPort, labels, name, protocol, servicePort) =>
      import state.Container.PortMapping._
      val decodedProto = protocol match {
        case NetworkProtocol.Tcp => TCP
        case NetworkProtocol.Udp => UDP
        case NetworkProtocol.UdpTcp => UDP_TCP
      }
      state.Container.PortMapping(
        containerPort = containerPort,
        hostPort = hostPort.orElse(defaultInstance.hostPort),
        servicePort = servicePort,
        protocol = decodedProto,
        name = name,
        labels = labels
      )
  }

  implicit val appContainerRamlReader: Reads[Container, state.Container] = Reads { container =>
    val volumes = container.volumes.map(Raml.fromRaml(_))
    val portMappings = container.portMappings.map(Raml.fromRaml(_))

    val result: state.Container = (container.`type`, container.docker, container.appc) match {
      case (EngineType.Docker, Some(docker), None) =>
        state.Container.Docker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see AppNormalization
          privileged = docker.privileged,
          parameters = docker.parameters.map(p => Parameter(p.key, p.value)),
          forcePullImage = docker.forcePullImage
        )
      case (EngineType.Mesos, Some(docker), None) =>
        state.Container.MesosDocker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see AppNormalization
          credential = docker.credential.map(c => state.Container.Credential(principal = c.principal, secret = c.secret)),
          forcePullImage = docker.forcePullImage
        )
      case (EngineType.Mesos, None, Some(appc)) =>
        state.Container.MesosAppC(
          volumes = volumes,
          image = appc.image,
          portMappings = portMappings,
          id = appc.id,
          labels = appc.labels,
          forcePullImage = appc.forcePullImage
        )
      case (EngineType.Mesos, None, None) =>
        state.Container.Mesos(
          volumes = volumes,
          portMappings = portMappings
        )
      case ct => throw SerializationFailedException(s"illegal container specification $ct")
    }
    result
  }

  implicit val containerTypeProtoToRamlWriter: Writes[org.apache.mesos.Protos.ContainerInfo.Type, EngineType] = Writes { ctype =>
    import org.apache.mesos.Protos.ContainerInfo.Type._
    ctype match {
      case MESOS => EngineType.Mesos
      case DOCKER => EngineType.Docker
      case badContainerType => throw new IllegalStateException(s"unsupported container type $badContainerType")
    }
  }

  implicit val appcProtoToRamlWriter: Writes[Protos.ExtendedContainerInfo.MesosAppCInfo, AppCContainer] = Writes { appc =>
    AppCContainer(
      image = appc.getImage,
      id = if (appc.hasId) Option(appc.getId) else AppCContainer.DefaultId,
      labels = appc.whenOrElse(_.getLabelsCount > 0, _.getLabelsList.flatMap(_.fromProto)(collection.breakOut), AppCContainer.DefaultLabels),
      forcePullImage = if (appc.hasForcePullImage) appc.getForcePullImage else AppCContainer.DefaultForcePullImage
    )
  }

  implicit val dockerNetworkProtoToRamlWriter: Writes[Mesos.ContainerInfo.DockerInfo.Network, DockerNetwork] = Writes { net =>
    import Mesos.ContainerInfo.DockerInfo.Network._
    net match {
      case HOST => DockerNetwork.Host
      case BRIDGE => DockerNetwork.Bridge
      case USER => DockerNetwork.User
      case NONE => DockerNetwork.None
    }
  }

  implicit val dockerParameterProtoRamlWriter: Writes[Mesos.Parameter, DockerParameter] = Writes { param =>
    DockerParameter(
      key = param.getKey,
      value = param.getValue
    )
  }

  implicit val dockerPortMappingProtoRamlWriter: Writes[Protos.ExtendedContainerInfo.DockerInfo.ObsoleteDockerPortMapping, ContainerPortMapping] = Writes { mapping =>
    ContainerPortMapping(
      containerPort = mapping.whenOrElse(_.hasContainerPort, _.getContainerPort, ContainerPortMapping.DefaultContainerPort),
      hostPort = mapping.when(_.hasHostPort, _.getHostPort).orElse(ContainerPortMapping.DefaultHostPort),
      labels = mapping.whenOrElse(_.getLabelsCount > 0, _.getLabelsList.flatMap(_.fromProto)(collection.breakOut), ContainerPortMapping.DefaultLabels),
      name = mapping.when(_.hasName, _.getName).orElse(ContainerPortMapping.DefaultName),
      protocol = mapping.whenOrElse(_.hasProtocol, _.getProtocol.toRaml[NetworkProtocol], ContainerPortMapping.DefaultProtocol),
      servicePort = mapping.whenOrElse(_.hasServicePort, _.getServicePort, ContainerPortMapping.DefaultServicePort)
    )
  }

  implicit val dockerProtoToRamlWriter: Writes[Protos.ExtendedContainerInfo.DockerInfo, DockerContainer] = Writes { docker =>
    DockerContainer(
      credential = DockerContainer.DefaultCredential, // we don't store credentials in protobuf
      forcePullImage = docker.when(_.hasForcePullImage, _.getForcePullImage).getOrElse(DockerContainer.DefaultForcePullImage),
      image = docker.getImage,
      network = docker.when(_.hasOBSOLETENetwork, _.getOBSOLETENetwork.toRaml).orElse(DockerContainer.DefaultNetwork),
      parameters = docker.whenOrElse(_.getParametersCount > 0, _.getParametersList.map(_.toRaml)(collection.breakOut), DockerContainer.DefaultParameters),
      portMappings = docker.whenOrElse(_.getOBSOLETEPortMappingsCount > 0, _.getOBSOLETEPortMappingsList.map(_.toRaml)(collection.breakOut), DockerContainer.DefaultPortMappings),
      privileged = docker.when(_.hasPrivileged, _.getPrivileged).getOrElse(DockerContainer.DefaultPrivileged)
    )
  }

  implicit val dockerCredentialsProtoRamlWriter: Writes[Mesos.Credential, DockerCredentials] = Writes { cred =>
    DockerCredentials(
      principal = cred.getPrincipal,
      secret = cred.when(_.hasSecret, _.getSecret).orElse(DockerCredentials.DefaultSecret)
    )
  }

  implicit val mesosDockerProtoToRamlWriter: Writes[Protos.ExtendedContainerInfo.MesosDockerInfo, DockerContainer] = Writes { docker =>
    DockerContainer(
      credential = docker.when(_.hasCredential, _.getCredential.toRaml).orElse(DockerContainer.DefaultCredential),
      forcePullImage = docker.when(_.hasForcePullImage, _.getForcePullImage).getOrElse(DockerContainer.DefaultForcePullImage),
      image = docker.getImage
    // was never stored for mesos containers:
    // - network
    // - parameters
    // - portMappings
    // - privileged
    )
  }

  implicit val containerPortMappingProtoRamlWriter: Writes[Protos.ExtendedContainerInfo.PortMapping, ContainerPortMapping] = Writes { mapping =>
    ContainerPortMapping(
      containerPort = mapping.whenOrElse(_.hasContainerPort, _.getContainerPort, ContainerPortMapping.DefaultContainerPort),
      hostPort = mapping.when(_.hasHostPort, _.getHostPort).orElse(ContainerPortMapping.DefaultHostPort),
      labels = mapping.whenOrElse(_.getLabelsCount > 0, _.getLabelsList.flatMap(_.fromProto)(collection.breakOut), ContainerPortMapping.DefaultLabels),
      name = mapping.when(_.hasName, _.getName).orElse(ContainerPortMapping.DefaultName),
      protocol = mapping.when(_.hasProtocol, _.getProtocol).flatMap(NetworkProtocol.fromString).getOrElse(ContainerPortMapping.DefaultProtocol),
      servicePort = mapping.whenOrElse(_.hasServicePort, _.getServicePort, ContainerPortMapping.DefaultServicePort)
    )
  }

  implicit val containerProtoToRamlWriter: Writes[Protos.ExtendedContainerInfo, Container] = Writes { container =>
    Container(
      `type` = container.when(_.hasType, _.getType.toRaml).getOrElse(Container.DefaultType),
      docker =
        container.collect {
          case x if x.hasDocker => x.getDocker.toRaml
          case x if x.hasMesosDocker => x.getMesosDocker.toRaml
        }.orElse(Container.DefaultDocker),
      appc = container.when(_.hasMesosAppC, _.getMesosAppC.toRaml).orElse(Container.DefaultAppc),
      volumes = container.whenOrElse(_.getVolumesCount > 0, _.getVolumesList.map(_.toRaml)(collection.breakOut), Container.DefaultVolumes),
      portMappings = container.whenOrElse(_.getPortMappingsCount > 0, _.getPortMappingsList.map(_.toRaml)(collection.breakOut), Container.DefaultPortMappings)
    )
  }
}

object ContainerConversion extends ContainerConversion
