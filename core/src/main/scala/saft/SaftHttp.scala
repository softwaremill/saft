package saft

import sttp.client3.SttpBackend
import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio.*
import zio.json.*
import zhttp.http.*
import zhttp.service.Server

import java.io.File
import java.nio.file.{Files, Path => JPath}

object SaftHttp1 extends SaftHttp(1, JPath.of("saft1.json"))
object SaftHttp2 extends SaftHttp(2, JPath.of("saft2.json"))
object SaftHttp3 extends SaftHttp(3, JPath.of("saft3.json"))

/** A Raft implementation using json-over-http for inter-node communication and json file-based persistence. */
class SaftHttp(nodeNumber: Int, persistencePath: JPath) extends ZIOAppDefault with JsonCodecs with Logging {
  override val run: Task[Nothing] =
    // configuration
    val conf = Conf.default(3)
    val clientTimeout = Duration.fromSeconds(5)
    val applyLogData = (nodeId: NodeId) => (data: LogData) => setNodeLogAnnotation(nodeId) *> ZIO.log(s"Apply: $data")

    // setup node
    val nodeId = NodeId(nodeNumber)
    def nodePort(nodeId: NodeId): Int = 8080 + nodeId.number
    val port = nodePort(nodeId)

    for {
      stateMachine <- StateMachine.background(applyLogData(nodeId))
      persistence = new FileJsonPersistence(persistencePath)
      queue <- Queue.sliding[ServerEvent](16)
      backend <- HttpClientZioBackend()
      comms = new HttpComms(queue, backend, clientTimeout, nodePort)
      node = new Node(nodeId, comms, stateMachine, conf, persistence)
      _ <- ZIO.log(s"Starting SaftHttp on localhost:$port")
      _ <- ZIO.log(s"Configuration: ${conf.show}")
      _ <- node.start.fork
      result <- Server.start(port, app(queue))
    } yield result

  private def app(queue: Queue[ServerEvent]): HttpApp[Any, Throwable] = Http
    .collectZIO[Request] {
      case r @ Method.POST -> !! / EndpointNames.AppendEntries => decodingEndpoint(r, _.fromJson[AppendEntries], queue)
      case r @ Method.POST -> !! / EndpointNames.RequestVote   => decodingEndpoint(r, _.fromJson[RequestVote], queue)
      case r @ Method.POST -> !! / EndpointNames.NewEntry      => decodingEndpoint(r, _.fromJson[NewEntry], queue)
    }
    .catchAll {
      case e: TimedOutException => Http.fromZIO(ZIO.logErrorCause(Cause.fail(e)).as(Response(Status.RequestTimeout)))
      case e: DecodeException   => Http.fromZIO(ZIO.logErrorCause(Cause.fail(e)).as(Response(Status.BadRequest)))
      case e: Exception         => Http.fromZIO(ZIO.logErrorCause(Cause.fail(e)).as(Response(Status.InternalServerError)))
    }

  private def decodingEndpoint[T <: RequestMessage with ToServerMessage](
      request: Request,
      decode: String => Either[String, T],
      queue: Queue[ServerEvent]
  ): Task[Response] =
    request.bodyAsString.map(decode).flatMap {
      case Right(msg) =>
        for {
          p <- Promise.make[Nothing, ResponseMessage]
          _ <- queue.offer(ServerEvent.RequestReceived(msg, r => p.succeed(r).unit))
          r <- p.await.timeoutFail(TimedOutException(s"Handling request message: $msg"))(Duration.fromSeconds(1))
        } yield Response.text(encodeResponse(r))
      case Left(errorMsg) => ZIO.fail(DecodeException(s"Handling request message: $errorMsg"))
    }
}

private case class TimedOutException(msg: String) extends Exception(msg)
private case class DecodeException(msg: String) extends Exception(msg)

private class HttpComms(queue: Queue[ServerEvent], backend: SttpBackend[Task, Any], clientTimeout: Duration, nodePort: NodeId => Int)
    extends Comms
    with JsonCodecs {
  override def next: UIO[ServerEvent] = queue.take
  override def send(toNodeId: NodeId, msg: RequestMessage with FromServerMessage): UIO[Unit] =
    import sttp.client3.*
    val url = uri"http://localhost:${nodePort(toNodeId)}/${endpoint(msg)}"
    backend
      .send(basicRequest.post(url).body(encodeRequest(msg)).response(asStringAlways))
      .timeoutFail(TimedOutException(s"Client request $msg to $url"))(clientTimeout)
      .map(_.body)
      .flatMap { body =>
        decodeResponse(msg, body) match
          case Left(errorMsg) => ZIO.fail(DecodeException(s"Client request $msg to $url, response: $body, error: $errorMsg"))
          case Right(decoded) => queue.offer(ServerEvent.ResponseReceived(decoded))
      }
      .unit
      .catchAll { case e: Exception => ZIO.logErrorCause(s"Cannot send $msg to $toNodeId", Cause.fail(e)) }
  override def add(event: ServerEvent): UIO[Unit] = queue.offer(event).unit

  private def endpoint(msg: RequestMessage): String = msg match
    case _: AppendEntries => EndpointNames.AppendEntries
    case _: RequestVote   => EndpointNames.RequestVote
    case _: NewEntry      => EndpointNames.NewEntry
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
  given JsonDecoder[NewEntryAddedSuccessfullyResponse.type] = DeriveJsonDecoder.gen[NewEntryAddedSuccessfullyResponse.type]
  given JsonEncoder[NewEntryAddedSuccessfullyResponse.type] = DeriveJsonEncoder.gen[NewEntryAddedSuccessfullyResponse.type]
  given JsonDecoder[RedirectToLeaderResponse] = DeriveJsonDecoder.gen[RedirectToLeaderResponse]
  given JsonEncoder[RedirectToLeaderResponse] = DeriveJsonEncoder.gen[RedirectToLeaderResponse]

  def encodeResponse(r: ResponseMessage): String = r match
    case r: RequestVoteResponse                    => r.toJson
    case r: AppendEntriesResponse                  => r.toJson
    case r: NewEntryAddedSuccessfullyResponse.type => r.toJson
    case r: RedirectToLeaderResponse               => r.toJson

  def encodeRequest(m: RequestMessage): String = m match
    case r: RequestVote   => r.toJson
    case r: AppendEntries => r.toJson
    case r: NewEntry      => r.toJson

  def decodeResponse(toRequest: RequestMessage with FromServerMessage, data: String): Either[String, ResponseMessage with ToServerMessage] =
    toRequest match
      case _: RequestVote   => data.fromJson[RequestVoteResponse]
      case _: AppendEntries => data.fromJson[AppendEntriesResponse]
}

private object EndpointNames {
  val AppendEntries = "append-entries"
  val RequestVote = "request-vote"
  val NewEntry = "new-entry"
}

private class FileJsonPersistence(path: JPath) extends Persistence with JsonCodecs {
  private given JsonDecoder[ServerState] = DeriveJsonDecoder.gen[ServerState]
  private given JsonEncoder[ServerState] = DeriveJsonEncoder.gen[ServerState]

  override def apply(oldState: ServerState, newState: ServerState): UIO[Unit] =
    ZIO
      .attempt {
        val json = newState.copy(commitIndex = None, lastApplied = None).toJsonPretty
        Files.writeString(path, json)
      }
      .unit
      .orDie

  override def get: UIO[ServerState] =
    if path.toFile.exists()
    then ZIO.fromEither(Files.readString(path).fromJson[ServerState]).mapError(DecodeException.apply).orDie
    else ZIO.succeed(ServerState.Initial)
}
