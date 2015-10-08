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

package co.vaughnvernon.reactiveenterprise.wiretap

import akka.actor._
import co.vaughnvernon.reactiveenterprise.CompletableApp

object WireTapDriver extends CompletableApp(2) {
	val order = Order("123")

	val orderProcessor = system.actorOf(Props[OrderProcessor], "orderProcessor")
	
	val logger = system.actorOf(Props(classOf[MessageLogger], orderProcessor), "logger")

	val orderProcessorWireTap = logger
	
	orderProcessorWireTap ! ProcessOrder(order)
	
	awaitCompletion
	
	println("WireTap: is completed.")
}

class MessageLogger(messageReceiver: ActorRef) extends Actor {
  def receive = {
    case m: Any =>
      println(s"LOG: $m")
      messageReceiver forward m
      WireTapDriver.completedStep()
  }
}

case class Order(orderId: String)
case class ProcessOrder(order: Order)

class OrderProcessor extends Actor {
  def receive = {
    case command: ProcessOrder =>
      println(s"OrderProcessor: received: $command")
      WireTapDriver.completedStep()
  }
}
