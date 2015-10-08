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

package co.vaughnvernon.reactiveenterprise.idempotentreceiver

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.util.Timeout
import akka.pattern.ask
import co.vaughnvernon.reactiveenterprise.CompletableApp
import akka.actor._

object RiskAssessmentDriver extends CompletableApp(2) {
  implicit val timeout = Timeout(5 seconds)
  
  val riskAssessment = system.actorOf(Props[RiskAssessment], "riskAssessment")

  val futureAssessment1 = riskAssessment ? ClassifyRisk()
  printRisk(futureAssessment1)
  
  riskAssessment ! AttachDocument("This is a HIGH risk.")

  val futureAssessment2 = riskAssessment ? ClassifyRisk()
  printRisk(futureAssessment2)
  
  awaitCompletion
  
  def printRisk(futureAssessment: Future[Any]): Unit = {
    val classification =
      Await.result(futureAssessment, timeout.duration)
           .asInstanceOf[RiskClassified]
    println(s"$classification")

    completedStep
  }
}

case class AttachDocument(documentText: String)
case class ClassifyRisk()
case class RiskClassified(classification: String)

case class Document(documentText: Option[String]) {
  if (documentText.isDefined) {
    val text = documentText.get
    if (text == null || text.trim.isEmpty) {
      throw new IllegalStateException("Document must have text.")
    }
  }
  
  def determineClassification = {
    val text = documentText.get.toLowerCase
    
    if (text.contains("low")) "Low"
    else if (text.contains("medium")) "Medium"
    else if (text.contains("high")) "High"
    else "Unknown"
  }
  
  def isNotAttached = documentText.isEmpty
  def isAttached = documentText.isDefined
}

class RiskAssessment extends Actor {
  var document = Document(None)
  
  def documented: Receive = {
    case attachment: AttachDocument =>
      // already received; ignore
      
    case classify: ClassifyRisk =>
      sender ! RiskClassified(document.determineClassification)
  }
  
  def undocumented: Receive = {
    case attachment: AttachDocument =>
      document = Document(Some(attachment.documentText))
      context.become(documented)
    case classify: ClassifyRisk =>
      sender ! RiskClassified("Unknown")
  }
  
  def receive = undocumented
}
