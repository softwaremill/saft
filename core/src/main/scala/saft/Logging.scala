package saft

import zio.*
import zio.ZIOAppDefault
import zio.logging.{LogColor, LogFormat, console, file}
import zio.logging.LogFormat.*

import java.nio.file.Paths
import java.time.format.{DateTimeFormatter, FormatStyle}

val StateLogAnnotation = "state"
private val NodeIdLogAnnotation = "nodeId"

def setLogAnnotation(key: String, value: String): UIO[Unit] = FiberRef.currentLogAnnotations.update(_ + (key -> value))
def setNodeLogAnnotation(nodeId: NodeId): UIO[Unit] = setLogAnnotation(NodeIdLogAnnotation, "node" + nodeId.number)

trait Logging { this: ZIOAppDefault =>
  private val logFormat = timestamp(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG)).fixed(32).color(LogColor.BLUE) |-|
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

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>>
    console(logFormat, LogLevel.Info) >>>
    file(Paths.get("saft.log"), logFormat, LogLevel.Debug)
}
