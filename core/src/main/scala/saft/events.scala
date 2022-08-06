package saft

import zio.UIO

sealed trait ServerEvent

/** Used when an election or heartbeat timeout occurs. */
case object Timeout extends ServerEvent

/** @param respond
  *   Sends the given [[message]] back to the server or client that sent this request.
  */
case class RequestReceived(message: ToServerMessage, respond: ResponseMessage => UIO[Unit]) extends ServerEvent
