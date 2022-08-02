package saft

import zio.*
import zio.ZIOAppDefault
import zio.logging.{LogColor, LogFormat, console}
import zio.logging.LogFormat.*

import java.time.format.{DateTimeFormatter, FormatStyle}

val StateLogAnnotation = "state"
val NodeIdLogAnnotation = "nodeId"

def setLogAnnotation(key: String, value: String): UIO[Unit] = FiberRef.currentLogAnnotations.update(_ + (key -> value))

trait Logging { this: ZIOAppDefault =>
  private val logFormat = timestamp(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG)).fixed(32).color(LogColor.BLUE) |-|
    (text("[") + level + text("]")).color(LogColor.GREEN) |-|
    fiberId.color(LogColor.WHITE) |-|
    annotationValue(NodeIdLogAnnotation).color(LogColor.RED) |-|
    annotation(StateLogAnnotation) |-|
    line.highlight |-|
    cause.color(LogColor.RED)

  private def annotationValue(name: String): LogFormat =
    LogFormat.make { (builder, _, _, _, _, _, _, _, annotations) =>
      annotations.get(name).foreach { value =>
        builder.appendText(value)
      }
    }

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> console(logFormat, LogLevel.Info)
}
