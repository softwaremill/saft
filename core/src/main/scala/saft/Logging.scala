package saft

import zio.*
import zio.ZIOAppDefault
import zio.logging.{LogColor, console}
import zio.logging.LogFormat.*

import java.time.format.{DateTimeFormatter, FormatStyle}

trait Logging { this: ZIOAppDefault =>
  private val logFormat = timestamp(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG)).fixed(32).color(LogColor.BLUE) |-|
    (text("[") + level + text("]")).color(LogColor.GREEN) |-|
    fiberId.color(LogColor.WHITE) |-| annotations |-| line.highlight |-| cause.color(LogColor.RED)

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> console(logFormat, LogLevel.Debug)
}
