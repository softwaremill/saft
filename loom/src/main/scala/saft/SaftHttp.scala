package saft

import com.typesafe.scalalogging.StrictLogging
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.server.{Handler, Request, Server, ServerConnector}
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.util.thread.ThreadPool
import saft.Logging.setNodeLogAnnotation
import zio.json.*
import zio.json.internal.Write

import java.io.File
import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path as JPath}
import java.time.Duration
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, CompletableFuture, ExecutorService, Executors, TimeUnit}
import scala.concurrent.TimeoutException

@main def saftHttp1(): Unit = new SaftHttp(1, JPath.of("saft1.json")).start()
@main def saftHttp2(): Unit = new SaftHttp(2, JPath.of("saft2.json")).start()
@main def saftHttp3(): Unit = new SaftHttp(3, JPath.of("saft3.json")).start()

/** A Raft implementation using json-over-http for inter-node communication and json file-based persistence. */
class SaftHttp(nodeNumber: Int, persistencePath: JPath) extends JsonCodecs with StrictLogging:
  def start(): Unit =
    // configuration
    val conf = Conf.default(3)
    val clientTimeout = Duration.ofSeconds(5)
    val applyLogData = (nodeId: NodeId) => (data: LogData) => { setNodeLogAnnotation(nodeId); logger.info(s"Apply: $data") }

    // setup node
    val nodeId = NodeId(nodeNumber)
    def nodePort(nodeId: NodeId): Int = 8080 + nodeId.number
    val port = nodePort(nodeId)

    val stateMachine = StateMachine.background(applyLogData(nodeId))
    val persistence = new FileJsonPersistence(persistencePath)
    val queue = new ArrayBlockingQueue[ServerEvent](16)
    val comms = new HttpComms(queue, clientTimeout, nodePort)
    val node = new Node(nodeId, comms, stateMachine, conf, persistence)
    logger.info(s"Starting SaftHttp on localhost:$port")
    logger.info(s"Configuration: ${conf.show}")

    // start node
    node.start()

    // start server
    val server = new Server(new LoomThreadPool)
    val connector = new ServerConnector(server); connector.setPort(port)

    server.setHandler(handler(queue))
    server.setConnectors(Array(connector)); server.start(); server.join()

  private def handler(queue: BlockingQueue[ServerEvent]): Handler = new AbstractHandler:
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse): Unit =
      def errorResponse(e: Exception, statusCode: Int): Unit =
        logger.error(s"Error when processing request to $target", e)
        response.setStatus(statusCode)

      try
        val responseBody =
          if target.contains(EndpointNames.AppendEntries) then Some(decodingHandler(baseRequest, _.fromJson[AppendEntries], queue))
          else if target.contains(EndpointNames.RequestVote) then Some(decodingHandler(baseRequest, _.fromJson[RequestVote], queue))
          else if target.contains(EndpointNames.NewEntry) then Some(decodingHandler(baseRequest, _.fromJson[NewEntry], queue))
          else None

        responseBody match
          case None => response.setStatus(HttpServletResponse.SC_NOT_FOUND)
          case Some(body) =>
            response.setContentType("application/json")
            response.setStatus(HttpServletResponse.SC_OK)
            response.getWriter.println(body)
      catch
        case e: TimeoutException => errorResponse(e, HttpServletResponse.SC_REQUEST_TIMEOUT)
        case e: DecodeException  => errorResponse(e, HttpServletResponse.SC_BAD_REQUEST)
        case e: Exception        => errorResponse(e, HttpServletResponse.SC_INTERNAL_SERVER_ERROR)

      baseRequest.setHandled(true)
    end handle

  private def decodingHandler[T <: RequestMessage with ToServerMessage](
      request: Request,
      decode: String => Either[String, T],
      queue: BlockingQueue[ServerEvent]
  ): String =
    val body = new String(request.getInputStream.readAllBytes())
    decode(body) match
      case Right(msg) =>
        val p = new CompletableFuture[ResponseMessage]
        queue.offer(ServerEvent.RequestReceived(msg, p.complete))
        encodeResponse(p.get(1, TimeUnit.SECONDS))
      case Left(errorMsg) => throw DecodeException(s"Handling request message: $errorMsg")
end SaftHttp

private case class DecodeException(msg: String) extends Exception(msg)

private class HttpComms(queue: BlockingQueue[ServerEvent], clientTimeout: Duration, nodePort: NodeId => Int)
    extends Comms
    with JsonCodecs
    with StrictLogging {
  private val client = HttpClient.newHttpClient()

  override def next: ServerEvent = queue.take
  override def send(toNodeId: NodeId, msg: RequestMessage with FromServerMessage): Unit =
    try
      val url = s"http://localhost:${nodePort(toNodeId)}/${endpoint(msg)}"
      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(url))
        .POST(BodyPublishers.ofString(encodeRequest(msg)))
        .timeout(clientTimeout)
        .build()
      val responseBody = client.send(request, HttpResponse.BodyHandlers.ofString()).body()

      decodeResponse(msg, responseBody) match
        case Left(errorMsg) => throw DecodeException(s"Client request $msg to $url, response: $responseBody, error: $errorMsg")
        case Right(decoded) => queue.offer(ServerEvent.ResponseReceived(decoded))
    catch case e: Exception => logger.error(s"Cannot send $msg to $toNodeId: ${e.getClass}")

  override def add(event: ServerEvent): Unit = queue.offer(event)

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
  given JsonEncoder[RedirectToLeaderResponse] = {
    val delegate = JsonEncoder.option(summon[JsonEncoder[NodeId]])
    given JsonEncoder[Option[NodeId]] = (a: Option[NodeId], indent: Option[Int], out: Write) => delegate.unsafeEncode(a, indent, out)
    DeriveJsonEncoder.gen[RedirectToLeaderResponse]
  }

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

  override def apply(oldState: ServerState, newState: ServerState): Unit =
    val json = newState.copy(commitIndex = None, lastApplied = None).toJsonPretty
    Files.writeString(path, json)

  override def get: ServerState =
    if path.toFile.exists()
    then
      Files.readString(path).fromJson[ServerState] match
        case Left(errorMsg) => throw DecodeException(errorMsg)
        case Right(state)   => state
    else ServerState.Initial
}

// Jetty on Loom, based on: https://github.com/rodrigovedovato/jetty-loom
class LoomThreadPool extends ThreadPool {
  val executorService: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  def join(): Unit = executorService.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
  def getThreads = 1
  def getIdleThreads = 1
  def isLowOnThreads = false
  def execute(command: Runnable): Unit = executorService.submit(command)
}
