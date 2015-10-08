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

package co.vaughnvernon.reactiveenterprise.pipesandfilters

import akka.actor._
import co.vaughnvernon.reactiveenterprise._

case class ProcessIncomingOrder(orderInfo: Array[Byte])

object PipesAndFiltersDriver extends CompletableApp(9) {
  val orderText = "(encryption)(certificate)<order id='123'>...</order>"
  val rawOrderBytes = orderText.toCharArray.map(_.toByte)

  val filter5 = system.actorOf(Props[OrderManagementSystem], "orderManagementSystem")
  val filter4 = system.actorOf(Props(classOf[Deduplicator], filter5), "deduplicator")
  val filter3 = system.actorOf(Props(classOf[Authenticator], filter4), "authenticator")
  val filter2 = system.actorOf(Props(classOf[Decrypter], filter3), "decrypter")
  val filter1 = system.actorOf(Props(classOf[OrderAcceptanceEndpoint], filter2), "orderAcceptanceEndpoint")
  
  filter1 ! rawOrderBytes
  filter1 ! rawOrderBytes
  
  awaitCompletion
  
  println("PipesAndFilters: is completed.")
}

class Authenticator(nextFilter: ActorRef) extends Actor {
  def receive = {
    case message: ProcessIncomingOrder =>
      val text = new String(message.orderInfo)
      println(s"Authenticator: processing $text")
      val orderText = text.replace("(certificate)", "")
      nextFilter ! ProcessIncomingOrder(orderText.toCharArray.map(_.toByte))
      PipesAndFiltersDriver.completedStep()
  }
}

class Decrypter(nextFilter: ActorRef) extends Actor {
  def receive = {
    case message: ProcessIncomingOrder =>
      val text = new String(message.orderInfo)
      println(s"Decrypter: processing $text")
      val orderText = text.replace("(encryption)", "")
      nextFilter ! ProcessIncomingOrder(orderText.toCharArray.map(_.toByte))
      PipesAndFiltersDriver.completedStep()
  }
}

class Deduplicator(nextFilter: ActorRef) extends Actor {
  val processedOrderIds = scala.collection.mutable.Set[String]()
  
  def orderIdFrom(orderText: String): String = {
    val orderIdIndex = orderText.indexOf("id='") + 4
    val orderIdLastIndex = orderText.indexOf("'", orderIdIndex)
    orderText.substring(orderIdIndex, orderIdLastIndex)
  }
  
  def receive = {
    case message: ProcessIncomingOrder =>
      val text = new String(message.orderInfo)
      println(s"Deduplicator: processing $text")
      val orderId = orderIdFrom(text)
      if (processedOrderIds.add(orderId)) {
        nextFilter ! message
      } else {
        println(s"Deduplicator: found duplicate order $orderId")
      }
      PipesAndFiltersDriver.completedStep()
  }
}

class OrderAcceptanceEndpoint(nextFilter: ActorRef) extends Actor {
  def receive = {
    case rawOrder: Array[Byte] =>
      val text = new String(rawOrder)
      println(s"OrderAcceptanceEndpoint: processing $text")
      nextFilter ! ProcessIncomingOrder(rawOrder)
      PipesAndFiltersDriver.completedStep()
  }
}

class OrderManagementSystem extends Actor {
  def receive = {
    case message: ProcessIncomingOrder =>
      val text = new String(message.orderInfo)
      println(s"OrderManagementSystem: processing unique order: $text")
      PipesAndFiltersDriver.completedStep()
  }
}
