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

package co.vaughnvernon.reactiveenterprise.aggregator

import akka.actor._
import co.vaughnvernon.reactiveenterprise._

case class RequestForQuotation(rfqId: String, retailItems: Seq[RetailItem]) {
  val totalRetailPrice: Double = retailItems.map(retailItem => retailItem.retailPrice).sum
}

case class RetailItem(itemId: String, retailPrice: Double)

case class PriceQuoteInterest(quoterId: String, quoteProcessor: ActorRef, lowTotalRetail: Double, highTotalRetail: Double)

case class RequestPriceQuote(rfqId: String, itemId: String, retailPrice: Double, orderTotalRetailPrice: Double)

case class PriceQuote(quoterId: String, rfqId: String, itemId: String, retailPrice: Double, discountPrice: Double)

case class PriceQuoteFulfilled(priceQuote: PriceQuote)

case class RequiredPriceQuotesForFulfillment(rfqId: String, quotesRequested: Int)

case class QuotationFulfillment(rfqId: String, quotesRequested: Int, priceQuotes: Seq[PriceQuote], requester: ActorRef)

object AggregatorDriver extends CompletableApp(5) {
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
      Vector(RetailItem("15", 107.50),
             RetailItem("16", 9.50),
             RetailItem("17", 599.99),
             RetailItem("18", 249.95),
             RetailItem("19", 789.99)))

  awaitCompletion
  println("Aggregator: is completed.")
}

class MountaineeringSuppliesOrderProcessor(priceQuoteAggregator: ActorRef) extends Actor {
  val interestRegistry = scala.collection.mutable.Map[String, PriceQuoteInterest]()

  def calculateRecipientList(rfq: RequestForQuotation): Iterable[ActorRef] = {
    for {
      interest <- interestRegistry.values
      if (rfq.totalRetailPrice >= interest.lowTotalRetail)
      if (rfq.totalRetailPrice <= interest.highTotalRetail)
    } yield interest.quoteProcessor
  }
  
  def dispatchTo(rfq: RequestForQuotation, recipientList: Iterable[ActorRef]) = {
    var totalRequestedQuotes = 0
    recipientList.foreach { recipient =>
      rfq.retailItems.foreach { retailItem =>
        println("OrderProcessor: " + rfq.rfqId + " item: " + retailItem.itemId + " to: " + recipient.path.toString)
        recipient ! RequestPriceQuote(rfq.rfqId, retailItem.itemId, retailItem.retailPrice, rfq.totalRetailPrice)
      }
    }
  }
  
  def receive = {
    case interest: PriceQuoteInterest =>
      interestRegistry(interest.quoterId) = interest
    case priceQuote: PriceQuote =>
      priceQuoteAggregator ! PriceQuoteFulfilled(priceQuote)
      println(s"OrderProcessor: received: $priceQuote")
    case rfq: RequestForQuotation =>
      val recipientList = calculateRecipientList(rfq)
      priceQuoteAggregator ! RequiredPriceQuotesForFulfillment(rfq.rfqId, recipientList.size * rfq.retailItems.size)
      dispatchTo(rfq, recipientList)
    case fulfillment: QuotationFulfillment =>
      println(s"OrderProcessor: received: $fulfillment")
      AggregatorDriver.completedStep()
    case message: Any =>
      println(s"OrderProcessor: received unexpected message: $message")
  }
}

class PriceQuoteAggregator extends Actor {
  val fulfilledPriceQuotes = scala.collection.mutable.Map[String, QuotationFulfillment]()
  
  def receive = {
    case required: RequiredPriceQuotesForFulfillment =>
      fulfilledPriceQuotes(required.rfqId) = QuotationFulfillment(required.rfqId, required.quotesRequested, Vector(), sender)
    case priceQuoteFulFilled: PriceQuoteFulfilled =>
      val previousFulfillment = fulfilledPriceQuotes(priceQuoteFulFilled.priceQuote.rfqId)
      val currentPriceQuotes = previousFulfillment.priceQuotes :+ priceQuoteFulFilled.priceQuote
      val currentFulfillment =
        QuotationFulfillment(
            previousFulfillment.rfqId,
            previousFulfillment.quotesRequested,
            currentPriceQuotes,
            previousFulfillment.requester)

      if (currentPriceQuotes.size >= currentFulfillment.quotesRequested) {
        currentFulfillment.requester ! currentFulfillment
        fulfilledPriceQuotes.remove(priceQuoteFulFilled.priceQuote.rfqId)
      } else {
    	fulfilledPriceQuotes(priceQuoteFulFilled.priceQuote.rfqId) = currentFulfillment
      }
      
      println(s"PriceQuoteAggregator: fulfilled price quote: $priceQuoteFulFilled")
    case message: Any =>
      println(s"PriceQuoteAggregator: received unexpected message: $message")
  }
}

class BudgetHikersPriceQuotes(interestRegistrar: ActorRef) extends Actor {
  val quoterId = self.path.toString.split("/").last
  interestRegistrar ! PriceQuoteInterest(quoterId, self, 1.00, 1000.00)
  
  def receive = {
    case rpq: RequestPriceQuote =>
      val discount = discountPercentage(rpq.orderTotalRetailPrice) * rpq.retailPrice
      sender ! PriceQuote(quoterId, rpq.rfqId, rpq.itemId, rpq.retailPrice, rpq.retailPrice - discount)

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

class HighSierraPriceQuotes(interestRegistrar: ActorRef) extends Actor {
  val quoterId = self.path.toString.split("/").last
  interestRegistrar ! PriceQuoteInterest(quoterId, self, 100.00, 10000.00)
  
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

class MountainAscentPriceQuotes(interestRegistrar: ActorRef) extends Actor {
  val quoterId = self.path.toString.split("/").last
  interestRegistrar ! PriceQuoteInterest(quoterId, self, 70.00, 5000.00)
  
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

class PinnacleGearPriceQuotes(interestRegistrar: ActorRef) extends Actor {
  val quoterId = self.path.toString.split("/").last
  interestRegistrar ! PriceQuoteInterest(quoterId, self, 250.00, 500000.00)
  
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

class RockBottomOuterwearPriceQuotes(interestRegistrar: ActorRef) extends Actor {
  val quoterId = self.path.toString.split("/").last
  interestRegistrar ! PriceQuoteInterest(quoterId, self, 0.50, 7500.00)
  
  def receive = {
    case rpq: RequestPriceQuote =>
      val discount = discountPercentage(rpq.orderTotalRetailPrice) * rpq.retailPrice
      sender ! PriceQuote(quoterId, rpq.rfqId, rpq.itemId, rpq.retailPrice, rpq.retailPrice - discount)

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
