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

package co.vaughnvernon.reactiveenterprise.publishsubscribe

import akka.actor._
import akka.event._
import akka.util._
import co.vaughnvernon.reactiveenterprise._
import java.math.RoundingMode

object SubClassificationDriver extends CompletableApp(6) {
  val allSubscriber = system.actorOf(Props[AllMarketsSubscriber], "AllMarketsSubscriber")
  val nasdaqSubscriber = system.actorOf(Props[NASDAQSubscriber], "NASDAQSubscriber")
  val nyseSubscriber = system.actorOf(Props[NYSESubscriber], "NYSESubscriber")

  val quotesBus = new QuotesEventBus

  quotesBus.subscribe(allSubscriber, Market("quotes"))
  quotesBus.subscribe(nasdaqSubscriber, Market("quotes/NASDAQ"))
  quotesBus.subscribe(nyseSubscriber, Market("quotes/NYSE"))
  
  quotesBus.publish(PriceQuoted(Market("quotes/NYSE"), Symbol("ORCL"), new Money("37.84")))
  quotesBus.publish(PriceQuoted(Market("quotes/NASDAQ"), Symbol("MSFT"), new Money("37.16")))
  quotesBus.publish(PriceQuoted(Market("quotes/DAX"), Symbol("SAP:GR"), new Money("61.95")))
  quotesBus.publish(PriceQuoted(Market("quotes/NKY"), Symbol("6701:JP"), new Money("237")))
  
  awaitCompletion
}

case class Money(amount: BigDecimal) {
  def this(amount: String) = this(new java.math.BigDecimal(amount))
  
  amount.setScale(4, BigDecimal.RoundingMode.HALF_UP)
}

case class Market(name: String)

case class PriceQuoted(market: Market, ticker: Symbol, price: Money)

class QuotesEventBus extends EventBus with SubchannelClassification {
  type Classifier = Market
  type Event = PriceQuoted
  type Subscriber = ActorRef

  protected def classify(event: Event): Classifier = {
    event.market
  }

  protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber ! event
  }

  protected def subclassification = new Subclassification[Classifier] {
    def isEqual(
        subscribedToClassifier: Classifier,
        eventClassifier: Classifier): Boolean = {
      
      subscribedToClassifier.equals(eventClassifier) 
    }
    
    def isSubclass(
        subscribedToClassifier: Classifier,
        eventClassifier: Classifier): Boolean = {
      
      subscribedToClassifier.name.startsWith(eventClassifier.name)
    }
  }
}

class AllMarketsSubscriber extends Actor {
  def receive = {
    case quote: PriceQuoted =>
      println(s"AllMarketsSubscriber received: $quote")
      SubClassificationDriver.completedStep
  }
}

class NASDAQSubscriber extends Actor {
  def receive = {
    case quote: PriceQuoted =>
      println(s"NASDAQSubscriber received: $quote")
      SubClassificationDriver.completedStep
  }
}

class NYSESubscriber extends Actor {
  def receive = {
    case quote: PriceQuoted =>
      println(s"NYSESubscriber received: $quote")
      SubClassificationDriver.completedStep
  }
}
