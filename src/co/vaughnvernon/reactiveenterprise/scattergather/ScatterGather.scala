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

package co.vaughnvernon.reactiveenterprise.scattergather

import java.util.concurrent.TimeUnit
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import akka.actor._
import co.vaughnvernon.reactiveenterprise._

case class RequestForQuotation(rfqId: String, retailItems: Seq[RetailItem]) {
  val totalRetailPrice: Double = retailItems.map(retailItem => retailItem.retailPrice).sum
}

case class RetailItem(itemId: String, retailPrice: Double)

case class RequestPriceQuote(rfqId: String, itemId: String, retailPrice: Double, orderTotalRetailPrice: Double)

case class PriceQuote(quoterId: String, rfqId: String, itemId: String, retailPrice: Double, discountPrice: Double)

case class PriceQuoteFulfilled(priceQuote: PriceQuote)

case class PriceQuoteTimedOut(rfqId: String)

case class RequiredPriceQuotesForFulfillment(rfqId: String, quotesRequested: Int)

case class QuotationFulfillment(rfqId: String, quotesRequested: Int, priceQuotes: Seq[PriceQuote], requester: ActorRef)

case class BestPriceQuotation(rfqId: String, priceQuotes: Seq[PriceQuote])

case class SubscribeToPriceQuoteRequests(quoterId: String, quoteProcessor: ActorRef)

object ScatterGatherDriver extends CompletableApp(5) {
  val priceQuoteAggregator = system.actorOf(Props[PriceQuoteAggregator], "priceQuoteAggregator")
  
  val orderProcessor = system.actorOf(Props(classOf[MountaineeringSuppliesOrderProcessor], priceQuoteAggregator), "orderProcessor")

  system.actorOf(Props(classOf[BudgetHikersPriceQuotes], orderProcessor), "budgetHikers")
  system.actorOf(Props(classOf[HighSierraPriceQuotes], orderProcessor), "highSierra")
  system.actorOf(Props(classOf[MountainAscentPriceQuotes], orderProcessor), "mountainAscent")
  system.actorOf(Props(classOf[PinnacleGearPriceQuotes], orderProcessor), "pinnacleGear")
  system.actorOf(Props(classOf[RockBottomOuterwearPriceQuotes], orderProcessor), "rockBottomOuterwear")

  orderProcessor ! RequestForQuotation("123",
      Vector(RetailItem("1", 29.95),
             RetailItem("2", 99.95),
             RetailItem("3", 14.95)))

  orderProcessor ! RequestForQuotation("125",
      Vector(RetailItem("4", 39.99),
             RetailItem("5", 199.95),
             RetailItem("6", 149.95),
             RetailItem("7", 724.99)))

  orderProcessor ! RequestForQuotation("129",
      Vector(RetailItem("8", 119.99),
             RetailItem("9", 499.95),
             RetailItem("10", 519.00),
             RetailItem("11", 209.50)))

  orderProcessor ! RequestForQuotation("135",
      Vector(RetailItem("12", 0.97),
             RetailItem("13", 9.50),
             RetailItem("14", 1.99)))

  orderProcessor ! RequestForQuotation("140",
      Vector(RetailItem("15", 1295.50),
             RetailItem("16", 9.50),
             RetailItem("17", 599.99),
             RetailItem("18", 249.95),
             RetailItem("19", 789.99)))

  awaitCompletion
  println("Scatter-Gather: is completed.")
}

class MountaineeringSuppliesOrderProcessor(priceQuoteAggregator: ActorRef) extends Actor {
  val subscribers = scala.collection.mutable.Map[String, SubscribeToPriceQuoteRequests]()

  def dispatch(rfq: RequestForQuotation) = {
    subscribers.values.foreach { subscriber =>
      val quoteProcessor = subscriber.quoteProcessor
      rfq.retailItems.foreach { retailItem =>
        println("OrderProcessor: " + rfq.rfqId + " item: " + retailItem.itemId + " to: " + subscriber.quoterId)
        quoteProcessor ! RequestPriceQuote(rfq.rfqId, retailItem.itemId, retailItem.retailPrice, rfq.totalRetailPrice)
      }
    }
  }
  
  def receive = {
    case subscriber: SubscribeToPriceQuoteRequests =>
      subscribers(subscriber.quoterId) = subscriber
    case priceQuote: PriceQuote =>
      priceQuoteAggregator ! PriceQuoteFulfilled(priceQuote)
      println(s"OrderProcessor: received: $priceQuote")
    case rfq: RequestForQuotation =>
      priceQuoteAggregator ! RequiredPriceQuotesForFulfillment(rfq.rfqId, subscribers.size * rfq.retailItems.size)
      dispatch(rfq)
    case bestPriceQuotation: BestPriceQuotation =>
      println(s"OrderProcessor: received: $bestPriceQuotation")
      ScatterGatherDriver.completedStep()
    case message: Any =>
      println(s"OrderProcessor: received unexpected message: $message")
  }
}

class PriceQuoteAggregator extends Actor {
  val fulfilledPriceQuotes = scala.collection.mutable.Map[String, QuotationFulfillment]()

  def bestPriceQuotationFrom(quotationFulfillment: QuotationFulfillment): BestPriceQuotation = {
    val bestPrices = scala.collection.mutable.Map[String, PriceQuote]()
    
    quotationFulfillment.priceQuotes.foreach { priceQuote =>
      if (bestPrices.contains(priceQuote.itemId)) {
        if (bestPrices(priceQuote.itemId).discountPrice > priceQuote.discountPrice) {
          bestPrices(priceQuote.itemId) = priceQuote
        }
      } else {
        bestPrices(priceQuote.itemId) = priceQuote
      }
    }
    
    BestPriceQuotation(quotationFulfillment.rfqId, bestPrices.values.toVector)
  }
  
  def receive = {
    case required: RequiredPriceQuotesForFulfillment =>
      fulfilledPriceQuotes(required.rfqId) = QuotationFulfillment(required.rfqId, required.quotesRequested, Vector(), sender)
      val duration = Duration.create(2, TimeUnit.SECONDS)
      context.system.scheduler.scheduleOnce(duration, self, PriceQuoteTimedOut(required.rfqId))
    case priceQuoteFulfilled: PriceQuoteFulfilled =>
      priceQuoteRequestFulfilled(priceQuoteFulfilled)
      println(s"PriceQuoteAggregator: fulfilled price quote: $PriceQuoteFulfilled")
    case priceQuoteTimedOut: PriceQuoteTimedOut =>
      priceQuoteRequestTimedOut(priceQuoteTimedOut.rfqId)
    case message: Any =>
      println(s"PriceQuoteAggregator: received unexpected message: $message")
  }
  
  def priceQuoteRequestFulfilled(priceQuoteFulfilled: PriceQuoteFulfilled) = {
    if (fulfilledPriceQuotes.contains(priceQuoteFulfilled.priceQuote.rfqId)) {
      val previousFulfillment = fulfilledPriceQuotes(priceQuoteFulfilled.priceQuote.rfqId)
      val currentPriceQuotes = previousFulfillment.priceQuotes :+ priceQuoteFulfilled.priceQuote
      val currentFulfillment =
          QuotationFulfillment(
              previousFulfillment.rfqId,
              previousFulfillment.quotesRequested,
              currentPriceQuotes,
              previousFulfillment.requester)

      if (currentPriceQuotes.size >= currentFulfillment.quotesRequested) {
        quoteBestPrice(currentFulfillment)
      } else {
        fulfilledPriceQuotes(priceQuoteFulfilled.priceQuote.rfqId) = currentFulfillment
      }
    }
  }

  def priceQuoteRequestTimedOut(rfqId: String) = {
    if (fulfilledPriceQuotes.contains(rfqId)) {
      quoteBestPrice(fulfilledPriceQuotes(rfqId))
    }
  }
  
  def quoteBestPrice(quotationFulfillment: QuotationFulfillment) = {
    if (fulfilledPriceQuotes.contains(quotationFulfillment.rfqId)) {
      quotationFulfillment.requester ! bestPriceQuotationFrom(quotationFulfillment)
      fulfilledPriceQuotes.remove(quotationFulfillment.rfqId)
    }
  }
}

class BudgetHikersPriceQuotes(priceQuoteRequestPublisher: ActorRef) extends Actor {
  val quoterId = self.path.name
  priceQuoteRequestPublisher ! SubscribeToPriceQuoteRequests(quoterId, self)
  
  def receive = {
    case rpq: RequestPriceQuote =>
      if (rpq.orderTotalRetailPrice < 1000.00) {
        val discount = discountPercentage(rpq.orderTotalRetailPrice) * rpq.retailPrice
        sender ! PriceQuote(quoterId, rpq.rfqId, rpq.itemId, rpq.retailPrice, rpq.retailPrice - discount)
      } else {
        println(s"BudgetHikersPriceQuotes: ignoring: $rpq")
      }

    case message: Any =>
      println(s"BudgetHikersPriceQuotes: received unexpected message: $message")
  }
  
  def discountPercentage(orderTotalRetailPrice: Double) = {
    if (orderTotalRetailPrice <= 100.00) 0.02
    else if (orderTotalRetailPrice <= 399.99) 0.03
    else if (orderTotalRetailPrice <= 499.99) 0.05
    else if (orderTotalRetailPrice <= 799.99) 0.07
    else 0.075
  }
}

class HighSierraPriceQuotes(priceQuoteRequestPublisher: ActorRef) extends Actor {
  val quoterId = self.path.name
  priceQuoteRequestPublisher ! SubscribeToPriceQuoteRequests(quoterId, self)
  
  def receive = {
    case rpq: RequestPriceQuote =>
      val discount = discountPercentage(rpq.orderTotalRetailPrice) * rpq.retailPrice
      sender ! PriceQuote(quoterId, rpq.rfqId, rpq.itemId, rpq.retailPrice, rpq.retailPrice - discount)

    case message: Any =>
      println(s"HighSierraPriceQuotes: received unexpected message: $message")
  }
  
  def discountPercentage(orderTotalRetailPrice: Double): Double = {
    if (orderTotalRetailPrice <= 150.00) 0.015
    else if (orderTotalRetailPrice <= 499.99) 0.02
    else if (orderTotalRetailPrice <= 999.99) 0.03
    else if (orderTotalRetailPrice <= 4999.99) 0.04
    else 0.05
  }
}

class MountainAscentPriceQuotes(priceQuoteRequestPublisher: ActorRef) extends Actor {
  val quoterId = self.path.name
  priceQuoteRequestPublisher ! SubscribeToPriceQuoteRequests(quoterId, self)
  
  def receive = {
    case rpq: RequestPriceQuote =>
      val discount = discountPercentage(rpq.orderTotalRetailPrice) * rpq.retailPrice
      sender ! PriceQuote(quoterId, rpq.rfqId, rpq.itemId, rpq.retailPrice, rpq.retailPrice - discount)

    case message: Any =>
      println(s"MountainAscentPriceQuotes: received unexpected message: $message")
  }
  
  def discountPercentage(orderTotalRetailPrice: Double) = {
    if (orderTotalRetailPrice <= 99.99) 0.01
    else if (orderTotalRetailPrice <= 199.99) 0.02
    else if (orderTotalRetailPrice <= 499.99) 0.03
    else if (orderTotalRetailPrice <= 799.99) 0.04
    else if (orderTotalRetailPrice <= 999.99) 0.045
    else if (orderTotalRetailPrice <= 2999.99) 0.0475
    else 0.05
  }
}

class PinnacleGearPriceQuotes(priceQuoteRequestPublisher: ActorRef) extends Actor {
  val quoterId = self.path.name
  priceQuoteRequestPublisher ! SubscribeToPriceQuoteRequests(quoterId, self)
  
  def receive = {
    case rpq: RequestPriceQuote =>
      val discount = discountPercentage(rpq.orderTotalRetailPrice) * rpq.retailPrice
      sender ! PriceQuote(quoterId, rpq.rfqId, rpq.itemId, rpq.retailPrice, rpq.retailPrice - discount)

    case message: Any =>
      println(s"PinnacleGearPriceQuotes: received unexpected message: $message")
  }
  
  def discountPercentage(orderTotalRetailPrice: Double) = {
    if (orderTotalRetailPrice <= 299.99) 0.015
    else if (orderTotalRetailPrice <= 399.99) 0.0175
    else if (orderTotalRetailPrice <= 499.99) 0.02
    else if (orderTotalRetailPrice <= 999.99) 0.03
    else if (orderTotalRetailPrice <= 1199.99) 0.035
    else if (orderTotalRetailPrice <= 4999.99) 0.04
    else if (orderTotalRetailPrice <= 7999.99) 0.05
    else 0.06
  }
}

class RockBottomOuterwearPriceQuotes(priceQuoteRequestPublisher: ActorRef) extends Actor {
  val quoterId = self.path.name
  priceQuoteRequestPublisher ! SubscribeToPriceQuoteRequests(quoterId, self)
  
  def receive = {
    case rpq: RequestPriceQuote =>
      if (rpq.orderTotalRetailPrice < 2000.00) {
        val discount = discountPercentage(rpq.orderTotalRetailPrice) * rpq.retailPrice
        sender ! PriceQuote(quoterId, rpq.rfqId, rpq.itemId, rpq.retailPrice, rpq.retailPrice - discount)
      } else {
        println(s"RockBottomOuterwearPriceQuotes: ignoring: $rpq")
      }

    case message: Any =>
      println(s"RockBottomOuterwearPriceQuotes: received unexpected message: $message")
  }
  
  def discountPercentage(orderTotalRetailPrice: Double) = {
    if (orderTotalRetailPrice <= 100.00) 0.015
    else if (orderTotalRetailPrice <= 399.99) 0.02
    else if (orderTotalRetailPrice <= 499.99) 0.03
    else if (orderTotalRetailPrice <= 799.99) 0.04
    else if (orderTotalRetailPrice <= 999.99) 0.05
    else if (orderTotalRetailPrice <= 2999.99) 0.06
    else if (orderTotalRetailPrice <= 4999.99) 0.07
    else if (orderTotalRetailPrice <= 5999.99) 0.075
    else 0.08
  }
}
