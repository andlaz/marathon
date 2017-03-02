package mesosphere.marathon
package core.async

import org.slf4j.MDC

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import java.util.concurrent.{Callable, Executor, ExecutorService, ThreadFactory}

import akka.dispatch.{ExecutorServiceConfigurator, ExecutorServiceFactory}
import java.util
import mesosphere.marathon.functional._

private[async] object Propagate {
  def apply[T](mdc: Option[util.Map[String, String]], context: Map[Context.ContextName[_], _])(f: => T): T = {
    val oldMdc = Option(MDC.getCopyOfContextMap)
    try {
      mdc.fold(MDC.clear())(MDC.setContextMap)
      Context.withContext(context)(f)
    } finally {
      oldMdc.fold(MDC.clear())(MDC.setContextMap)
    }
  }
}

/**
  * Mixin that enables org.slf4j.MDC and [[Context]] propagation across threads.
  */
trait ContextPropagatingExecutionContext extends ExecutionContext { self =>
  override def prepare(): ExecutionContext = new ExecutionContext {
    val mdcContext = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore

    override def execute(runnable: Runnable): Unit = self.execute(new Runnable {
      def run(): Unit = {
        Propagate(mdcContext, context)(runnable.run())
      }
    })

    override def reportFailure(cause: Throwable): Unit = self.reportFailure(cause)
  }
}

/**
  * Wrapper around another Execution Context that will Propagate MDC and Context.
  */
case class ContextPropagatingExecutionContextWrapper(wrapped: ExecutionContext)
    extends ExecutionContext with ContextPropagatingExecutionContext {
  override def execute(runnable: Runnable): Unit = wrapped.execute(runnable)

  override def reportFailure(cause: Throwable): Unit = wrapped.reportFailure(cause)
}

object CallerThreadExecutionContext {
  val executor: Executor = new Executor {
    override def execute(command: Runnable): Unit = command.run()
  }

  lazy val callerThreadExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

  def apply(): ExecutionContext = callerThreadExecutionContext
}

private[async] trait ContextPropagatingExecutorService extends ExecutorService { self =>
  override def submit[T](task: Callable[T]): util.concurrent.Future[T] = {
    val mdcContext = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore

    val callable = new Callable[T] {
      def call(): T = {
        Propagate(mdcContext, context)(task.call)
      }
    }
    self.submit(callable)
  }

  override def submit[T](task: Runnable, result: T): util.concurrent.Future[T] = {
    val mdc = Option(MDC.getCopyOfContextMap)
    val context = Context.copy
    val runnable = new Runnable {
      override def run(): Unit = Propagate(mdc, context)(task.run())
    }
    self.submit(runnable, result)
  }

  override def submit(task: Runnable): util.concurrent.Future[_] = {
    val mdc = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore
    val runnable = new Runnable {
      override def run(): Unit = Propagate(mdc, context)(task.run())
    }
    self.submit(runnable)
  }

  override def invokeAll[T](tasks: util.Collection[_ <: util.concurrent.Callable[T]]): util.List[util.concurrent.Future[T]] = {
    val mdc = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore
    val collection = tasks.stream().map { (c: Callable[T]) => new Callable[T] {
        override def call(): T = Propagate(mdc, context)(c.call())
      }
    }.collect(util.stream.Collectors.toList[Callable[T]]())
    self.invokeAll(collection)
  }

  override def invokeAll[T](tasks: util.Collection[_ <: util.concurrent.Callable[T]], timeout: Long, unit: util.concurrent.TimeUnit): util.List[util.concurrent.Future[T]] = {
    val mdc = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore
    val collection = tasks.stream().map { (c: Callable[T]) => new Callable[T] {
      override def call(): T = Propagate(mdc, context)(c.call())
    }
    }.collect(util.stream.Collectors.toList[Callable[T]]())
    self.invokeAll(collection, timeout, unit)
  }

  override def invokeAny[T](tasks: util.Collection[_ <: util.concurrent.Callable[T]]): T = {
    val mdc = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore
    val collection = tasks.stream().map { (c: Callable[T]) => new Callable[T] {
      override def call(): T = Propagate(mdc, context)(c.call())
    }
    }.collect(util.stream.Collectors.toList[Callable[T]]())
    self.invokeAny(collection)
  }

  override def invokeAny[T](tasks: util.Collection[_ <: _root_.java.util.concurrent.Callable[T]], timeout: Long, unit: util.concurrent.TimeUnit): T = {
    val mdc = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore
    val collection = tasks.stream().map { (c: Callable[T]) => new Callable[T] {
      override def call(): T = Propagate(mdc, context)(c.call())
    }
    }.collect(util.stream.Collectors.toList[Callable[T]]())
    self.invokeAny(collection, timeout, unit)
  }

  override def execute(command: Runnable): Unit = {
    val mdc = Option(MDC.getCopyOfContextMap)
    val context = Context.copy // linter:ignore
    val runnable = new Runnable {
      override def run(): Unit = Propagate(mdc, context)(command.run())
    }
    self.execute(command)
  }
}

private[async] class ContextPropagatingExecutorServiceWrapper(service: ExecutorService) extends ContextPropagatingExecutorService with ExecutorService {
  override def submit[T](task: util.concurrent.Callable[T]): util.concurrent.Future[T] = service.submit(task)
  override def submit[T](task: Runnable, result: T): util.concurrent.Future[T] = service.submit(task, result)
  override def submit(task: Runnable): util.concurrent.Future[_] = service.submit(task)
  override def invokeAll[T](tasks: util.Collection[_ <: util.concurrent.Callable[T]]): util.List[util.concurrent.Future[T]] = service.invokeAll(tasks)
  override def invokeAll[T](tasks: util.Collection[_ <: util.concurrent.Callable[T]], timeout: Long, unit: util.concurrent.TimeUnit): util.List[util.concurrent.Future[T]] = service.invokeAll(tasks, timeout, unit)
  override def invokeAny[T](tasks: util.Collection[_ <: util.concurrent.Callable[T]]): T = service.invokeAny(tasks)
  override def invokeAny[T](tasks: util.Collection[_ <: util.concurrent.Callable[T]], timeout: Long, unit: util.concurrent.TimeUnit): T = service.invokeAny(tasks, timeout, unit)
  override def execute(command: _root_.java.lang.Runnable): Unit = service.execute(command)
  override def isTerminated: Boolean = service.isTerminated
  override def awaitTermination(timeout: Long, unit: util.concurrent.TimeUnit): Boolean = service.awaitTermination(timeout, unit)
  override def shutdownNow(): util.List[Runnable] = service.shutdownNow()
  override def shutdown(): Unit = service.shutdown()
  override def isShutdown: Boolean = service.isShutdown
}

class AkkaExecutorServiceConfigurator extends  { self =>
  private class FactoryWrapper(factory: ExecutorServiceFactory) extends ExecutorServiceFactory {
    override def createExecutorService: ExecutorService = new ContextPropagatingExecutorServiceWrapper(factory.createExecutorService)
  }
  override def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = {
    new FactoryWrapper(self.createExecutorServiceFactory(id, threadFactory))
  }
}

class AkkaExecutorServiceConfiguratorWrapper(wrapped: ExecutorServiceConfigurator) extends ExecutorServiceConfigurator with AkkaExecutorServiceConfigurator {
  override def createExecutorServiceFactory(id: _root_.scala.Predef.String, threadFactory: _root_.java.util.concurrent.ThreadFactory): _root_.akka.dispatch.ExecutorServiceFactory = ???
}

object ExecutionContexts {
  /** Prefer this context over the default scala one as it can propagate org.slf4j.MDC and [[Context]] */
  implicit lazy val global: ExecutionContext = ContextPropagatingExecutionContextWrapper(ExecutionContext.global)

  lazy val callerThread: ExecutionContext = CallerThreadExecutionContext()
}
