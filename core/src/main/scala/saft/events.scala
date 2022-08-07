package saft

import zio.UIO

sealed trait ServerEvent

/** Used when an election or heartbeat timeout occurs. */
case object Timeout extends ServerEvent

/** @param respond Sends a response back to the server or client that sent this request. */
case class RequestReceived(message: RequestMessage, respond: ResponseMessage => UIO[Unit]) extends ServerEvent

case class ResponseReceived(message: ResponseMessage with ToServerMessage) extends ServerEvent
