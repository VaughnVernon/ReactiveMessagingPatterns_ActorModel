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

package co.vaughnvernon.reactiveenterprise.domainmodel

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import co.vaughnvernon.reactiveenterprise.CompletableApp

class Order extends Actor {
  var amount: Double = _
  
  def receive = {
    case init: InitializeOrder =>
      println(s"Initializing Order with $init")
      this.amount = init.amount
    case processOrder: ProcessOrder =>
      println(s"Processing Order is $processOrder")
      DomainModelPrototype.completedStep()
  }
}

case class InitializeOrder(amount: Double)
case class ProcessOrder()

object DomainModelPrototype extends CompletableApp(1) {

  val orderType = "co.vaughnvernon.reactiveenterprise.domainmodel.Order"
  
  val model = DomainModel("OrderProcessing")
  
  model.registerAggregateType(orderType)
  
  val order = model.aggregateOf(orderType, "123")
  
  order ! InitializeOrder(249.95)
  
  order ! ProcessOrder()

  awaitCompletion
  
  model.shutdown()
  
  println("DomainModelPrototype: is completed.")
}

object DomainModel {
  def apply(name: String): DomainModel = {
    new DomainModel(name)
  }
}

class DomainModel(name: String) {
  val aggregateTypeRegistry = scala.collection.mutable.Map[String, AggregateType]()
  val system = ActorSystem(name)
  
  def aggregateOf(typeName: String, id: String): AggregateRef = {
    if (aggregateTypeRegistry.contains(typeName)) {
      val aggregateType = aggregateTypeRegistry(typeName)
      aggregateType.cacheActor ! RegisterAggregateId(id)
      AggregateRef(id, aggregateType.cacheActor)
    } else {
      throw new IllegalStateException(s"DomainModel type registry does not have a $typeName")
    }
  }
  
  def registerAggregateType(typeName: String): Unit = {
    if (!aggregateTypeRegistry.contains(typeName)) {
      val actorRef = system.actorOf(Props(classOf[AggregateCache], typeName), typeName)
      aggregateTypeRegistry(typeName) = AggregateType(actorRef)
    }
  }
  
  def shutdown() = {
    system.shutdown()
  }
}

class AggregateCache(typeName: String) extends Actor {
  val aggregateClass: Class[Actor] = Class.forName(typeName).asInstanceOf[Class[Actor]]
  val aggregateIds = scala.collection.mutable.Set[String]()
  
  def receive = {
    case message: CacheMessage =>
      val aggregate = context.child(message.id).getOrElse {
        if (!aggregateIds.contains(message.id)) {
          throw new IllegalStateException(s"No aggregate of type $typeName and id ${message.id}")
        } else {
          context.actorOf(Props(aggregateClass), message.id)
        }
      }
      aggregate.tell(message.actualMessage, message.sender)
    
    case register: RegisterAggregateId =>
      this.aggregateIds.add(register.id)
  }
}

case class AggregateRef(id: String, cache: ActorRef) {
  def tell(message: Any)(implicit sender: ActorRef = null): Unit = {
    cache ! CacheMessage(id, message, sender)
  }
  
  def !(message: Any)(implicit sender: ActorRef = null): Unit = {
    cache ! CacheMessage(id, message, sender)
  }
}

case class AggregateType(cacheActor: ActorRef)

case class CacheMessage(id: String, actualMessage: Any, sender: ActorRef)

case class RegisterAggregateId(id: String)
