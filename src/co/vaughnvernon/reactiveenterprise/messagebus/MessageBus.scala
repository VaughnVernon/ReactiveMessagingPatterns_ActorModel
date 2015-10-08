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

package co.vaughnvernon.reactiveenterprise.messagebus

import akka.actor._
import co.vaughnvernon.reactiveenterprise._

object MessageBusDriver extends CompletableApp(9) {
  val tradingBus = system.actorOf(Props(classOf[TradingBus], 6), "tradingBus")
  val marketAnalysisTools = system.actorOf(Props(classOf[MarketAnalysisTools], tradingBus), "marketAnalysisTools")
  val portfolioManager = system.actorOf(Props(classOf[PortfolioManager], tradingBus), "portfolioManager")
  val stockTrader = system.actorOf(Props(classOf[StockTrader], tradingBus), "stockTrader")

  awaitCanStartNow
  
  tradingBus ! Status()
  
  tradingBus ! TradingCommand("ExecuteBuyOrder", ExecuteBuyOrder("p123", "MSFT", 100, 31.85))
  tradingBus ! TradingCommand("ExecuteSellOrder", ExecuteSellOrder("p456", "MSFT", 200, 31.80))
  tradingBus ! TradingCommand("ExecuteBuyOrder", ExecuteBuyOrder("p789", "MSFT", 100, 31.83))
  
  awaitCompletion
  println("MessageBus: is completed.")
}

case class CommandHandler(applicationId: String, handler: ActorRef)
case class ExecuteBuyOrder(portfolioId: String, symbol: String, quantity: Int, price: Double)
case class BuyOrderExecuted(portfolioId: String, symbol: String, quantity: Int, price: Double)
case class ExecuteSellOrder(portfolioId: String, symbol: String, quantity: Int, price: Double)
case class SellOrderExecuted(portfolioId: String, symbol: String, quantity: Int, price: Double)
case class NotificationInterest(applicationId: String, interested: ActorRef)
case class RegisterCommandHandler(applicationId: String, commandId: String, handler: ActorRef)
case class RegisterNotificationInterest(applicationId: String, notificationId: String, interested: ActorRef)
case class TradingCommand(commandId: String, command: Any)
case class TradingNotification(notificationId: String, notification: Any)

case class Status()

class TradingBus(canStartAfterRegistered: Int) extends Actor {
  val commandHandlers = scala.collection.mutable.Map[String, Vector[CommandHandler]]()
  val notificationInterests = scala.collection.mutable.Map[String, Vector[NotificationInterest]]()

  var totalRegistered = 0
  
  def dispatchCommand(command: TradingCommand) = {
    if (commandHandlers.contains(command.commandId)) {
      commandHandlers(command.commandId) map { commandHandler =>
        commandHandler.handler ! command.command
      }
    }
  }
  
  def dispatchNotification(notification: TradingNotification) = {
    if (notificationInterests.contains(notification.notificationId)) {
      notificationInterests(notification.notificationId) map { notificationInterest =>
        notificationInterest.interested ! notification.notification
      }
    }
  }
  
  def notifyStartWhenReady() = {
    totalRegistered += 1
    
    if (totalRegistered == this.canStartAfterRegistered) {
      println(s"TradingBus: is ready with registered actors: $totalRegistered")
      MessageBusDriver.canStartNow()
    }
  }
  
  def receive = {
    case register: RegisterCommandHandler =>
      println(s"TradingBus: registering: $register")
      registerCommandHandler(register.commandId, register.applicationId, register.handler)
      notifyStartWhenReady()
      
    case register: RegisterNotificationInterest =>
      println(s"TradingBus: registering: $register")
      registerNotificationInterest(register.notificationId, register.applicationId, register.interested)
      notifyStartWhenReady()
      
    case command: TradingCommand =>
      println(s"TradingBus: dispatching command: $command")
      dispatchCommand(command)
      
    case notification: TradingNotification =>
      println(s"TradingBus: dispatching notification: $notification")
      dispatchNotification(notification)
      
    case status: Status =>
      println(s"TradingBus: STATUS: has commandHandlers: $commandHandlers")
      println(s"TradingBus: STATUS: has notificationInterests: $notificationInterests")
      
    case message: Any =>
      println(s"TradingBus: received unexpected: $message")
  }
  
  def registerCommandHandler(commandId: String, applicationId: String, handler: ActorRef) = {
    if (!commandHandlers.contains(commandId)) {
      commandHandlers(commandId) = Vector[CommandHandler]()
    }
    
    commandHandlers(commandId) =
        commandHandlers(commandId) :+ CommandHandler(applicationId, handler)
  }
  
  def registerNotificationInterest(notificationId: String, applicationId: String, interested: ActorRef) = {
    if (!notificationInterests.contains(notificationId)) {
      notificationInterests(notificationId) = Vector[NotificationInterest]()
    }
    
    notificationInterests(notificationId) =
        notificationInterests(notificationId) :+ NotificationInterest(applicationId, interested)
  }
}

class MarketAnalysisTools(tradingBus: ActorRef) extends Actor {
  val applicationId = self.path.name
  
  tradingBus ! RegisterNotificationInterest(applicationId, "BuyOrderExecuted", self)
  tradingBus ! RegisterNotificationInterest(applicationId, "SellOrderExecuted", self)

  def receive = {
    case executed: BuyOrderExecuted =>
      println(s"MarketAnalysisTools: adding analysis for: $executed")
      MessageBusDriver.completedStep()
      
    case executed: SellOrderExecuted =>
      println(s"MarketAnalysisTools: adjusting analysis for: $executed")
      MessageBusDriver.completedStep()
      
    case message: Any =>
      println(s"MarketAnalysisTools: received unexpected: $message")
  }
}

class PortfolioManager(tradingBus: ActorRef) extends Actor {
  val applicationId = self.path.name
  
  tradingBus ! RegisterNotificationInterest(applicationId, "BuyOrderExecuted", self)
  tradingBus ! RegisterNotificationInterest(applicationId, "SellOrderExecuted", self)

  def receive = {
    case executed: BuyOrderExecuted =>
      println(s"PortfolioManager: adding holding to portfolio for: $executed")
      MessageBusDriver.completedStep()
      
    case executed: SellOrderExecuted =>
      println(s"PortfolioManager: adjusting holding in portfolio for: $executed")
      MessageBusDriver.completedStep()
      
    case message: Any =>
      println(s"PortfolioManager: received unexpected: $message")
  }
}

class StockTrader(tradingBus: ActorRef) extends Actor {
  val applicationId = self.path.name
  
  tradingBus ! RegisterCommandHandler(applicationId, "ExecuteBuyOrder", self)
  tradingBus ! RegisterCommandHandler(applicationId, "ExecuteSellOrder", self)

  def receive = {
    case buy: ExecuteBuyOrder =>
      println(s"StockTrader: buying for: $buy")
      tradingBus ! TradingNotification("BuyOrderExecuted", BuyOrderExecuted(buy.portfolioId, buy.symbol, buy.quantity, buy.price))
      MessageBusDriver.completedStep()
      
    case sell: ExecuteSellOrder =>
      println(s"StockTrader: selling for: $sell")
      tradingBus ! TradingNotification("SellOrderExecuted", SellOrderExecuted(sell.portfolioId, sell.symbol, sell.quantity, sell.price))
      MessageBusDriver.completedStep()
      
    case message: Any =>
      println(s"StockTrader: received unexpected: $message")
  }
}
