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

package co.vaughnvernon.reactiveenterprise.messagerouter

import co.vaughnvernon.reactiveenterprise._
import akka.actor._

object MessageRouterDriver extends CompletableApp(20) {
  val processor1 = system.actorOf(Props[Processor], "processor1")
  val processor2 = system.actorOf(Props[Processor], "processor2")
  
  val alternatingRouter = system.actorOf(Props(classOf[AlternatingRouter], processor1, processor2), "alternatingRouter")
  
  for (count <- 1 to 10) {
    alternatingRouter ! "Message #" + count
  }
  
  awaitCompletion
  
  println("MessageRouter: is completed.")
}

class AlternatingRouter(processor1: ActorRef, processor2: ActorRef) extends Actor {
  var alternate = 1;
  
  def alternateProcessor() = {
    if (alternate == 1) {
      alternate = 2
      processor1
    } else {
      alternate = 1
      processor2
    }
  }
  
  def receive = {
    case message: Any =>
      val processor = alternateProcessor
      println(s"AlternatingRouter: routing $message to ${processor.path.name}")
      processor ! message
      MessageRouterDriver.completedStep()
  }
}

class Processor extends Actor {
  def receive = {
    case message: Any =>
      println(s"Processor: ${self.path.name} received $message")
      MessageRouterDriver.completedStep()
  }
}
