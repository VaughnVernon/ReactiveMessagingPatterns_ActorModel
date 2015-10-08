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

package co.vaughnvernon.reactiveenterprise.messagefilter

import akka.actor._
import co.vaughnvernon.reactiveenterprise.CompletableApp

case class Order(id: String, orderType: String, orderItems: Map[String, OrderItem]) {
  val grandTotal: Double = orderItems.values.map(orderItem => orderItem.price).sum
  
  def isType(prefix: String): Boolean = {
    this.orderType.startsWith(prefix)
  }
  
  override def toString = {
    s"Order($id, $orderType, $orderItems, Totaling: $grandTotal)"
  }
}

case class OrderItem(id: String, itemType: String, description: String, price: Double) {
  override def toString = {
    s"OrderItem($id, $itemType, '$description', $price)"
  }
}

case class OrderPlaced(order: Order)

object MessageFilterDriver extends CompletableApp (4) {
  val inventorySystemA = system.actorOf(Props[InventorySystemA], "inventorySystemA")
  val actualInventorySystemX = system.actorOf(Props[InventorySystemX], "inventorySystemX")
  val inventorySystemX = system.actorOf(Props(classOf[InventorySystemXMessageFilter], actualInventorySystemX), "inventorySystemXMessageFilter")

  val orderItem1 = OrderItem("1", "TypeABC.4", "An item of type ABC.4.", 29.95)
  val orderItem2 = OrderItem("2", "TypeABC.1", "An item of type ABC.1.", 99.95)
  val orderItem3 = OrderItem("3", "TypeABC.9", "An item of type ABC.9.", 14.95)
  val orderItemsOfTypeA = Map(orderItem1.itemType -> orderItem1, orderItem2.itemType -> orderItem2, orderItem3.itemType -> orderItem3)
  inventorySystemA ! OrderPlaced(Order("123", "TypeABC", orderItemsOfTypeA))
  inventorySystemX ! OrderPlaced(Order("123", "TypeABC", orderItemsOfTypeA))
  
  val orderItem4 = OrderItem("4", "TypeXYZ.2", "An item of type XYZ.2.", 74.95)
  val orderItem5 = OrderItem("5", "TypeXYZ.1", "An item of type XYZ.1.", 59.95)
  val orderItem6 = OrderItem("6", "TypeXYZ.7", "An item of type XYZ.7.", 29.95)
  val orderItem7 = OrderItem("7", "TypeXYZ.5", "An item of type XYZ.5.", 9.95)
  val orderItemsOfTypeX = Map(orderItem4.itemType -> orderItem4, orderItem5.itemType -> orderItem5, orderItem6.itemType -> orderItem6, orderItem7.itemType -> orderItem7)
  inventorySystemA ! OrderPlaced(Order("124", "TypeXYZ", orderItemsOfTypeX))
  inventorySystemX ! OrderPlaced(Order("124", "TypeXYZ", orderItemsOfTypeX))
  
  awaitCompletion
  println("MessageFilter: is completed.")
}

class InventorySystemA extends Actor {
  def receive = {
    case OrderPlaced(order) if (order.isType("TypeABC")) =>
      println(s"InventorySystemA: handling $order")
      MessageFilterDriver.completedStep()
    case incompatibleOrder =>
      println(s"InventorySystemA: filtering out: $incompatibleOrder")
      MessageFilterDriver.completedStep()
  }
}

class InventorySystemX extends Actor {
  def receive = {
    case OrderPlaced(order) =>
      println(s"InventorySystemX: handling $order")
      MessageFilterDriver.completedStep()
    case _ =>
      println("InventorySystemX: received unexpected message")
      MessageFilterDriver.completedStep()
  }
}

class InventorySystemXMessageFilter(actualInventorySystemX: ActorRef) extends Actor {
  def receive = {
    case orderPlaced: OrderPlaced if (orderPlaced.order.isType("TypeXYZ")) =>
      actualInventorySystemX forward orderPlaced
      MessageFilterDriver.completedStep()
    case incompatibleOrder =>
      println(s"InventorySystemXMessageFilter: filtering out: $incompatibleOrder")
      MessageFilterDriver.completedStep()
  }
}
