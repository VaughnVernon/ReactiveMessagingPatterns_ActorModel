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

package co.vaughnvernon.reactiveenterprise.pointtopointchannel

import akka.actor._
import co.vaughnvernon.reactiveenterprise._

object PointToPointChannelDriver extends CompletableApp(4) {
  val actorB = system.actorOf(Props[ActorB])
  
  actorB ! "Goodbye, from actor C!"
  actorB ! "Hello, from actor A!"
  actorB ! "Goodbye again, from actor C!"
  actorB ! "Hello again, from actor A!"
  
  awaitCompletion
  
  println("PointToPointChannel: completed.")
}

class ActorB extends Actor {
  var goodbye = 0
  var goodbyeAgain = 0
  var hello = 0
  var helloAgain = 0

  def receive = {
    case message: String =>
      hello = hello +
        (if (message.contains("Hello")) 1 else 0)
      helloAgain = helloAgain +
        (if (message.startsWith("Hello again")) 1 else 0)
      assert(hello == 0 || hello > helloAgain)

      goodbye = goodbye +
        (if (message.contains("Goodbye")) 1 else 0)
      goodbyeAgain = goodbyeAgain +
        (if (message.startsWith("Goodbye again")) 1 else 0)
      assert(goodbye == 0 || goodbye > goodbyeAgain)
      
      PointToPointChannelDriver.completedStep
  }
}