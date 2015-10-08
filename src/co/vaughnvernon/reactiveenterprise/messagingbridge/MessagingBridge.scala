//   Copyright 2012,2015 Vaughn Vernon
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package co.vaughnvernon.reactiveenterprise.messagingbridge

import akka.event.Logging
import akka.event.LoggingAdapter
import co.vaughnvernon.reactiveenterprise.CompletableApp

class MessageBridgeException(
    message: String,
    cause: Throwable,
    retry: Boolean)
  extends RuntimeException(message, cause) {

  def this(message: String, cause: Throwable) =
    this(message, cause, false)

  def retry(): Boolean = retry
}

object RabbitMQMessagingBridgeRunner extends CompletableApp(1) {
}

class InventoryProductAllocationBridge(config: RabbitMQBridgeConfig)
        extends RabbitMQBridgeActor(config) {

  private val log: LoggingAdapter = Logging.getLogger(context.system, self)
  
  def receive = {
    case message: RabbitMQBinaryMessage =>
      log.error("Binary messages not supported.")
    case message: RabbitMQTextMessage =>
      log.error(s"Received text message: ${message.textMessage}")
    case invalid: Any =>
      log.error(s"Don't understand message: $invalid")
  }
}
