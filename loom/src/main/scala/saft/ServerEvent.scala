package saft

enum ServerEvent:
  /** Used when an election or heartbeat timeout occurs. */
  case Timeout extends ServerEvent

  /** @param respond Sends a response back to the server or client that sent this request. */
  case RequestReceived(message: RequestMessage, respond: ResponseMessage => Unit) extends ServerEvent
  case ResponseReceived(message: ResponseMessage with ToServerMessage) extends ServerEvent
