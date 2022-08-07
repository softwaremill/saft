package saft

import zio.*
import zio.json.*
import zhttp.http.*
import zhttp.service.{ChannelFactory, Client, EventLoopGroup, Server}

object SaftHttp1 extends SaftHttp(1)
object SaftHttp2 extends SaftHttp(2)
object SaftHttp3 extends SaftHttp(3)

class SaftHttp(nodeNumber: Int) extends JsonCodecs with ZIOAppDefault with Logging {
  def decodingEndpoint[T <: RequestMessage with ToServerMessage](
      request: Request,
      decode: String => Either[String, T],
      queue: Queue[ServerEvent]
  ): Task[Response] =
    request.bodyAsString.map(decode).flatMap {
      case Right(msg) =>
        for {
          p <- Promise.make[Nothing, ResponseMessage]
          _ <- queue.offer(RequestReceived(msg, r => p.succeed(r).unit))
          r <- p.await.timeoutFail(new RuntimeException("Timed out"))(Duration.fromSeconds(5))
        } yield Response.text(encodeResponse(r))
      case Left(error) => ZIO.succeed(Response.text(error).setStatus(Status.BadRequest))
    }

  def app(queue: Queue[ServerEvent]): HttpApp[Any, Throwable] = Http.collectZIO[Request] {
    case r @ Method.POST -> !! / "append-entries" => decodingEndpoint(r, _.fromJson[AppendEntries], queue)
    case r @ Method.POST -> !! / "request-vote"   => decodingEndpoint(r, _.fromJson[RequestVote], queue)
    case r @ Method.POST -> !! / "new-entry"      => decodingEndpoint(r, _.fromJson[NewEntry], queue)
  }

  override val run: Task[Nothing] =
    // configuration
    val conf = Conf.default(3)
    val applyLogData = (nodeId: NodeId) => (data: LogData) => setNodeLogAnnotation(nodeId) *> ZIO.log(s"Apply: $data")

    // setup node
    val nodeId = NodeId(nodeNumber)
    val clientEnv = ChannelFactory.auto ++ EventLoopGroup.auto()

    for {
      stateMachine <- StateMachine.background(applyLogData(nodeId))
      persistence <- InMemoryPersistence(List(nodeId)).map(_.forNodeId(nodeId))
      queue <- Queue.sliding[ServerEvent](16)
      comms = new Comms {
        override def nextEvent: UIO[ServerEvent] = queue.take
        override def send(toNodeId: NodeId, msg: RequestMessage with ToServerMessage): UIO[Unit] =
          Client
            .request(
              url = s"http://localhost:${nodePort(toNodeId)}/${endpoint(msg)}",
              method = Method.POST,
              content = HttpData.fromString(encodeRequestMessage(msg))
            )
            .provide(clientEnv)
            .timeoutFail(new RuntimeException("Timed out"))(Duration.fromSeconds(5))
            .flatMap(_.bodyAsString)
            .flatMap { body =>
              decodeResponse(msg, body) match
                case Left(value)    => ???
                case Right(decoded) => queue.offer(ResponseReceived(decoded))
            }
            .unit
            .catchAll { case e: Exception => ZIO.logErrorCause(s"Cannot send $msg to $toNodeId", Cause.fail(e)) }
        override def add(event: ServerEvent): UIO[Unit] = queue.offer(event).unit
      }
      node = new Node(nodeId, comms, stateMachine, conf, persistence)
      port = nodePort(nodeId)
      _ <- ZIO.log(s"Starting SaftHttp on localhost:$port")
      _ <- ZIO.log(s"Configuration: ${conf.show}")
      _ <- node.start.fork
      result <- Server.start(port, app(queue))
    } yield result

  private def nodePort(nodeId: NodeId): Int = 8080 + nodeId.number

  private def endpoint(msg: RequestMessage): String = msg match
    case r: RequestVote   => "request-vote"
    case r: AppendEntries => "append-entries"
    case r: NewEntry      => "new-entry"
}

private trait JsonCodecs {
  given JsonDecoder[Term] = JsonDecoder[Int].map(Term(_))
  given JsonEncoder[Term] = JsonEncoder[Int].contramap(identity)
  given JsonDecoder[LogIndex] = JsonDecoder[Int].map(LogIndex(_))
  given JsonEncoder[LogIndex] = JsonEncoder[Int].contramap(identity)
  given JsonDecoder[LogData] = JsonDecoder[String].map(LogData(_))
  given JsonEncoder[LogData] = JsonEncoder[String].contramap(identity)
  given JsonDecoder[NodeId] = JsonDecoder[Int].map(NodeId.apply)
  given JsonEncoder[NodeId] = JsonEncoder[Int].contramap(_.number)
  given JsonDecoder[LogIndexTerm] = DeriveJsonDecoder.gen[LogIndexTerm]
  given JsonEncoder[LogIndexTerm] = DeriveJsonEncoder.gen[LogIndexTerm]
  given JsonDecoder[LogEntry] = DeriveJsonDecoder.gen[LogEntry]
  given JsonEncoder[LogEntry] = DeriveJsonEncoder.gen[LogEntry]
  given JsonDecoder[AppendEntries] = DeriveJsonDecoder.gen[AppendEntries]
  given JsonEncoder[AppendEntries] = DeriveJsonEncoder.gen[AppendEntries]
  given JsonDecoder[AppendEntriesResponse] = DeriveJsonDecoder.gen[AppendEntriesResponse]
  given JsonEncoder[AppendEntriesResponse] = DeriveJsonEncoder.gen[AppendEntriesResponse]
  given JsonDecoder[RequestVote] = DeriveJsonDecoder.gen[RequestVote]
  given JsonEncoder[RequestVote] = DeriveJsonEncoder.gen[RequestVote]
  given JsonDecoder[RequestVoteResponse] = DeriveJsonDecoder.gen[RequestVoteResponse]
  given JsonEncoder[RequestVoteResponse] = DeriveJsonEncoder.gen[RequestVoteResponse]
  given JsonDecoder[NewEntry] = DeriveJsonDecoder.gen[NewEntry]
  given JsonEncoder[NewEntry] = DeriveJsonEncoder.gen[NewEntry]
  given JsonDecoder[NewEntryAddedResponse.type] = DeriveJsonDecoder.gen[NewEntryAddedResponse.type]
  given JsonEncoder[NewEntryAddedResponse.type] = DeriveJsonEncoder.gen[NewEntryAddedResponse.type]
  given JsonDecoder[RedirectToLeaderResponse] = DeriveJsonDecoder.gen[RedirectToLeaderResponse]
  given JsonEncoder[RedirectToLeaderResponse] = DeriveJsonEncoder.gen[RedirectToLeaderResponse]

  def encodeResponse(r: ResponseMessage): String = r match
    case r: RequestVoteResponse        => r.toJson
    case r: AppendEntriesResponse      => r.toJson
    case r: NewEntryAddedResponse.type => r.toJson
    case r: RedirectToLeaderResponse   => r.toJson

  def encodeRequestMessage(m: RequestMessage): String = m match
    case r: RequestVote   => r.toJson
    case r: AppendEntries => r.toJson
    case r: NewEntry      => r.toJson

  def decodeResponse(toRequest: RequestMessage with ToServerMessage, data: String): Either[String, ResponseMessage with ToServerMessage] =
    toRequest match
      case _: RequestVote   => data.fromJson[RequestVoteResponse]
      case _: AppendEntries => data.fromJson[AppendEntriesResponse]
      case _: NewEntry      => ??? // TODO
}
