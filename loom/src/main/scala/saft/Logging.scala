package saft

import org.slf4j.MDC

object Logging {
  def setNodeLogAnnotation(nodeId: NodeId): Unit = MDC.put("nodeId", "node" + nodeId.number)
  def setRoleLogAnnotation(state: String): Unit = MDC.put("role", state)
}
