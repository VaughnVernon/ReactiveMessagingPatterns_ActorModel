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

package co.vaughnvernon.reactiveenterprise.claimcheck

import akka.actor._
import co.vaughnvernon.reactiveenterprise._
import java.util.UUID

case class Part(name: String)

case class CompositeMessage(id: String, part1: Part, part2: Part, part3: Part)

case class ProcessStep(id: String, claimCheck: ClaimCheck)

case class StepCompleted(id: String, claimCheck: ClaimCheck, stepName: String)

object ClaimCheckDriver extends CompletableApp(3) {
  val itemChecker = new ItemChecker()
  
  val step1 = system.actorOf(Props(classOf[Step1], itemChecker), "step1")
  val step2 = system.actorOf(Props(classOf[Step2], itemChecker), "step2")
  val step3 = system.actorOf(Props(classOf[Step3], itemChecker), "step3")
  
  val process = system.actorOf(Props(classOf[Process], Vector(step1, step2, step3), itemChecker), "process")
  
  process ! CompositeMessage("ABC", Part("partA1"), Part("partB2"), Part("partC3"))
  
  awaitCompletion
  println("ClaimCheck: is completed.")
}

case class ClaimCheck() {
  val number = UUID.randomUUID().toString
  
  override def toString = {
    "ClaimCheck(" + number + ")"
  }
}

case class CheckedItem(claimCheck: ClaimCheck, businessId: String, parts: Map[String, Any])

case class CheckedPart(claimCheck: ClaimCheck, part: Any)

class ItemChecker {
  val checkedItems = scala.collection.mutable.Map[ClaimCheck, CheckedItem]()
  
  def checkedItemFor(businessId: String, parts: Map[String, Any]) = {
      CheckedItem(ClaimCheck(), businessId, parts)
  }
  
  def checkItem(item: CheckedItem) = {
    checkedItems.update(item.claimCheck, item)
  }
  
  def claimItem(claimCheck: ClaimCheck): CheckedItem = {
    checkedItems(claimCheck)
  }
  
  def claimPart(claimCheck: ClaimCheck, partName: String): CheckedPart = {
    val checkedItem = checkedItems(claimCheck)
    
    CheckedPart(claimCheck, checkedItem.parts(partName))
  }
  
  def removeItem(claimCheck: ClaimCheck) = {
    if (checkedItems.contains(claimCheck)) {
      checkedItems.remove(claimCheck)
    }
  }
}

class Process(steps: Vector[ActorRef], itemChecker: ItemChecker) extends Actor {
  var stepIndex = 0
  
  def receive = {
    case message: CompositeMessage =>
      val parts =
        Map(
            message.part1.name -> message.part1,
            message.part2.name -> message.part2,
            message.part3.name -> message.part3)

      val checkedItem = itemChecker.checkedItemFor(message.id, parts)
      
      itemChecker.checkItem(checkedItem)
      
      steps(stepIndex) ! ProcessStep(message.id, checkedItem.claimCheck)
      
    case message: StepCompleted =>
      stepIndex += 1
      
      if (stepIndex < steps.size) {
        steps(stepIndex) ! ProcessStep(message.id, message.claimCheck)
      } else {
        itemChecker.removeItem(message.claimCheck)
      }
      
      ClaimCheckDriver.completedStep()
      
    case message: Any =>
      println(s"Process: received unexpected: $message")
  }
}

class Step1(itemChecker: ItemChecker) extends Actor {
  def receive = {
    case processStep: ProcessStep =>
      val claimedPart = itemChecker.claimPart(processStep.claimCheck, "partA1")
      
      println(s"Step1: processing $processStep\n with $claimedPart")
      
      sender ! StepCompleted(processStep.id, processStep.claimCheck, "step1")
      
    case message: Any =>
      println(s"Step1: received unexpected: $message")
  }
}

class Step2(itemChecker: ItemChecker) extends Actor {
  def receive = {
    case processStep: ProcessStep =>
      val claimedPart = itemChecker.claimPart(processStep.claimCheck, "partB2")
      
      println(s"Step2: processing $processStep\n with $claimedPart")
      
      sender ! StepCompleted(processStep.id, processStep.claimCheck, "step2")
      
    case message: Any =>
      println(s"Step2: received unexpected: $message")
  }
}

class Step3(itemChecker: ItemChecker) extends Actor {
  def receive = {
    case processStep: ProcessStep =>
      val claimedPart = itemChecker.claimPart(processStep.claimCheck, "partC3")
      
      println(s"Step3: processing $processStep\n with $claimedPart")
      
      sender ! StepCompleted(processStep.id, processStep.claimCheck, "step3")
      
    case message: Any =>
      println(s"Step3: received unexpected: $message")
  }
}
