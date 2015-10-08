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

package co.vaughnvernon.reactiveenterprise.selectiveconsumer

import akka.actor._
import co.vaughnvernon.reactiveenterprise.CompletableApp

object SelectiveConsumerDriver extends CompletableApp(3) {
  val consumerOfA =
    system.actorOf(
        Props[ConsumerOfMessageTypeA],
        "consumerOfA")

  val consumerOfB =
    system.actorOf(
        Props[ConsumerOfMessageTypeB],
        "consumerOfB")

  val consumerOfC =
    system.actorOf(
        Props[ConsumerOfMessageTypeC],
        "consumerOfC")

  val selectiveConsumer =
    system.actorOf(
        Props(classOf[SelectiveConsumer],
            consumerOfA, consumerOfB, consumerOfC),
        "selectiveConsumer")
  
  selectiveConsumer ! MessageTypeA()
  selectiveConsumer ! MessageTypeB()
  selectiveConsumer ! MessageTypeC()
  
  awaitCompletion
  
  println("SelectiveConsumer: completed.")
}

case class MessageTypeA()
case class MessageTypeB()
case class MessageTypeC()

class SelectiveConsumer(
    consumerOfA: ActorRef,
    consumerOfB: ActorRef,
    consumerOfC: ActorRef) extends Actor {
  
  def receive = {
    case message: MessageTypeA => consumerOfA forward message
    case message: MessageTypeB => consumerOfB forward message
    case message: MessageTypeC => consumerOfC forward message
  }
}

class ConsumerOfMessageTypeA extends Actor {
  def receive = {
    case message: MessageTypeA =>
      println(s"ConsumerOfMessageTypeA: $message")
      SelectiveConsumerDriver.completedStep
  }
}

class ConsumerOfMessageTypeB extends Actor {
  def receive = {
    case message: MessageTypeB =>
      println(s"ConsumerOfMessageTypeB: $message")
      SelectiveConsumerDriver.completedStep
  }
}

class ConsumerOfMessageTypeC extends Actor {
  def receive = {
    case message: MessageTypeC =>
      println(s"ConsumerOfMessageTypeC: $message")
      SelectiveConsumerDriver.completedStep
  }
}
