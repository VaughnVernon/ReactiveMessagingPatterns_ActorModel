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

package co.vaughnvernon.reactiveenterprise.contentfilter

import akka.actor._
import co.vaughnvernon.reactiveenterprise.CompletableApp

case class FilteredMessage(light: String, and: String, fluffy: String, message: String) {
  override def toString = {
    s"FilteredMessage(" + light + " " + and + " " + fluffy + " " + message + ")"
  }
}

case class UnfilteredPayload(largePayload: String)

object ContentFilterDriver extends CompletableApp(3) {
  val messageExchangeDispatcher = system.actorOf(Props[MessageExchangeDispatcher], "messageExchangeDispatcher")
  
  messageExchangeDispatcher ! UnfilteredPayload("A very large message with complex structure...")
  
  awaitCompletion
  println("RequestReply: is completed.")
}

class MessageExchangeDispatcher extends Actor {
  val messageContentFilter = context.actorOf(Props[MessageContentFilter], "messageContentFilter")

  def receive = {
    case message: UnfilteredPayload =>
      println("MessageExchangeDispatcher: received unfiltered message: " + message.largePayload)
      messageContentFilter ! message
      ContentFilterDriver.completedStep()
    case message: FilteredMessage =>
      println("MessageExchangeDispatcher: dispatching: " + message)
      ContentFilterDriver.completedStep()
    case _ =>
      println("MessageExchangeDispatcher: received unexpected message")
  }
}

class MessageContentFilter extends Actor {
  def receive = {
    case message: UnfilteredPayload =>
      println("MessageContentFilter: received unfiltered message: " + message.largePayload)
      // filtering occurs...
      sender ! FilteredMessage("this", "feels", "so", "right")
      ContentFilterDriver.completedStep()
    case _ =>
      println("MessageContentFilter: received unexpected message")
  }
}