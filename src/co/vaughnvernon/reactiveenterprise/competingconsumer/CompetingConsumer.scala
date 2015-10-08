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

package co.vaughnvernon.reactiveenterprise.competingconsumer

import co.vaughnvernon.reactiveenterprise.CompletableApp
import akka.actor._
import akka.routing.SmallestMailboxPool

object CompetingConsumerDriver extends CompletableApp(100) {
  val workItemsProvider = system.actorOf(
        Props[WorkConsumer]
            .withRouter(SmallestMailboxPool(nrOfInstances = 5)))

  for (itemCount <- 1 to 100) {
    workItemsProvider ! WorkItem("WorkItem" + itemCount)
  }

  awaitCompletion
}

case class WorkItem(name: String)

class WorkConsumer extends Actor {
  def receive = {
    case workItem: WorkItem =>
      println(s"${self.path.name} for: ${workItem.name}")
      
      CompetingConsumerDriver.completedStep
  }
}