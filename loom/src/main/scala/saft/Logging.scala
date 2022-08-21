package saft

import org.slf4j.MDC

object Logging {
  def setNodeLogAnnotation(nodeId: NodeId): Unit = MDC.put("nodeId", "node" + nodeId.number)
  def setStateLogAnnotation(state: String): Unit = MDC.put("state", state)
}
