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

package co.vaughnvernon.reactiveenterprise.transactionalactor

import akka.persistence._
import co.vaughnvernon.reactiveenterprise.CompletableApp

object EventSourcedDriver extends CompletableApp(4) {

}

trait DomainEvent

case class StartOrder(orderId: String, customerInfo: String)
case class OrderStarted(orderId: String, customerInfo: String) extends DomainEvent

case class AddOrderLineItem(productId: String, units: Int, price: Double)
case class OrderLineItemAdded(productId: String, units: Int, price: Double) extends DomainEvent

case class PlaceOrder()
case class OrderPlaced() extends DomainEvent

class Order extends EventsourcedProcessor {
  var state: OrderState = _ // new OrderState()
  
  def updateState(event: DomainEvent): Unit =
    state = state.update(event)

  val receiveRecover: Receive = {
    case event: DomainEvent =>
      updateState(event)
    case SnapshotOffer(_, snapshot: OrderState) =>
      state = snapshot
  }
  
  val receiveCommand: Receive = {
    case startOrder: StartOrder =>
      persist(OrderStarted(startOrder.orderId, startOrder.customerInfo)) (updateState)
    case addOrderLineItem @ AddOrderLineItem =>
      //persist(OrderStarted(addOrderLineItem., startOrder.customerInfo)) (updateState)
    case placeOrder @ PlaceOrder =>
  }
}

case class OrderState(
    customerInfo: String,
    lineItems: List[OrderLineItem],
    placed: Boolean) {
  
  def update(event: DomainEvent): OrderState = {
    null
  }
}

case class OrderLineItem()
