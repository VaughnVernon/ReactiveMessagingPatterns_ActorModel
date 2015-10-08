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

package co.vaughnvernon.reactiveenterprise.messagemetadata

import java.util.{Date, Random}
import akka.actor._
import co.vaughnvernon.reactiveenterprise.CompletableApp

object MessageMetadataDriver extends CompletableApp(3) {
  import Metadata._

  val processor3 = system.actorOf(Props(classOf[Processor], None), "processor3")
  val processor2 = system.actorOf(Props(classOf[Processor], Some(processor3)), "processor2")
  val processor1 = system.actorOf(Props(classOf[Processor], Some(processor2)), "processor1")
  
  val entry = Entry(
      Who("driver"),
      What("Started"),
      Where(this.getClass.getSimpleName, "driver"),
      new Date(),
      Why("Running processors"))

  processor1 ! SomeMessage("Data...", entry.asMetadata)
  
  awaitCompletion
  
  println("Completed.")
}

object Metadata {
  def apply() = new Metadata()
  
  case class Who(name: String)
  case class What(happened: String)
  case class Where(actorType: String, actorName: String)
  case class Why(explanation: String)
  
  case class Entry(who: Who, what: What, where: Where, when: Date, why: Why) {
    def this(who: Who, what: What, where: Where, why: Why) =
      this(who, what, where, new Date(), why)

    def this(who: String, what: String, actorType: String, actorName: String, why: String) =
      this(Who(who), What(what), Where(actorType, actorName), new Date(), Why(why))
    
    def asMetadata = (new Metadata(List[Entry](this)))
  }
}

import Metadata._

case class Metadata(entries: List[Entry]) {
  def this() = this(List.empty[Entry])
  def this(entry: Entry) = this(List[Entry](entry))

  def including(entry: Entry): Metadata = {
    Metadata(entries :+ entry)
  }
}

case class SomeMessage(
    payload: String,
    metadata: Metadata = new Metadata()) {

  def including(entry: Entry): SomeMessage = {
    SomeMessage(payload, metadata.including(entry))
  }
}

class Processor(next: Option[ActorRef]) extends Actor {
  import Metadata._
  
  val random = new Random()

  def receive = {
    case message: SomeMessage =>
      report(message)
      
      val nextMessage = message.including(entry)
              
      if (next.isDefined) {
        next.get ! nextMessage
      } else {
        report(nextMessage, "complete")
      }

      MessageMetadataDriver.completedStep
  }

  def because = s"Because: ${random.nextInt(10)}"

  def entry =
    Entry(Who(user),
          What(wasProcessed),
          Where(this.getClass.getSimpleName, self.path.name),
          new Date(),
          Why(because))
  
  def report(message: SomeMessage, heading: String = "received") =
    println(s"${self.path.name} $heading: $message")
  
  def user = s"user${random.nextInt(100)}"

  def wasProcessed = s"Processed: ${random.nextInt(5)}"
}
