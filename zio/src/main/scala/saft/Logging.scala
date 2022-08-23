package saft

import zio.*
import zio.ZIOAppDefault
import zio.logging.{LogColor, LogFormat, console, file}
import zio.logging.LogFormat.*

import java.nio.file.Paths
import java.time.format.{DateTimeFormatter, FormatStyle}

private val StateLogAnnotation = "state"
private val NodeIdLogAnnotation = "nodeId"

private def setLogAnnotation(key: String, value: String): UIO[Unit] = FiberRef.currentLogAnnotations.update(_ + (key -> value))
def setNodeLogAnnotation(nodeId: NodeId): UIO[Unit] = setLogAnnotation(NodeIdLogAnnotation, "node" + nodeId.number)
def setStateLogAnnotation(state: String): UIO[Unit] = setLogAnnotation(StateLogAnnotation, state)

trait Logging { this: ZIOAppDefault =>
  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>>
    console(Logging.logFormat, LogLevel.Info) >>>
    file(Paths.get("saft.log"), Logging.logFormat, LogLevel.Debug)
}

object Logging {
  val logFormat: LogFormat = timestamp(DateTimeFormatter.ISO_OFFSET_DATE_TIME).fixed(32).color(LogColor.BLUE) |-|
    (text("[") + level + text("]")).color(LogColor.GREEN) |-|
    fiberId.fixed(14).color(LogColor.WHITE) |-|
    annotationValue(NodeIdLogAnnotation).fixed(5).color(LogColor.RED) |-|
    annotation(StateLogAnnotation) |-|
    line.highlight |-|
    cause.color(LogColor.RED)

  private def annotationValue(name: String): LogFormat =
    LogFormat.make { (builder, _, _, _, _, _, _, _, annotations) =>
      annotations.get(name).foreach { value =>
        builder.appendText(value)
      }
    }
}
