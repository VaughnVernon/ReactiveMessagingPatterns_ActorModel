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

package co.vaughnvernon.reactiveenterprise.contentenricher

import akka.actor._
import co.vaughnvernon.reactiveenterprise._
import java.util.Date

case class DoctorVisitCompleted(
    val patientId: String,
    val firstName: String,
    val date: Date,
    val patientDetails: PatientDetails) {
  def this(patientId: String, firstName: String, date: Date) = {
    this(patientId, firstName, date, PatientDetails(null, null, null))
  }

  def carrier = patientDetails.carrier
  def lastName = patientDetails.lastName
  def socialSecurityNumber = patientDetails.socialSecurityNumber
}

case class PatientDetails(val lastName: String, val socialSecurityNumber: String, val carrier: String)

case class VisitCompleted(dispatcher: ActorRef)

object ContentEnricherDriver extends CompletableApp(3) {
  val accountingSystemDispatcher = system.actorOf(Props[AccountingSystemDispatcher], "accountingSystem")
  val accountingEnricherDispatcher = system.actorOf(Props(classOf[AccountingEnricherDispatcher], accountingSystemDispatcher), "accountingDispatcher")
  val scheduledDoctorVisit = system.actorOf(Props(classOf[ScheduledDoctorVisit], "123456789", "John"), "scheduledVisit")
  scheduledDoctorVisit ! VisitCompleted(accountingEnricherDispatcher)
  
  awaitCompletion
  println("ContentEnricher: is completed.")
}

class AccountingEnricherDispatcher(val accountingSystemDispatcher: ActorRef) extends Actor {
  def receive = {
    case doctorVisitCompleted: DoctorVisitCompleted =>
      println("AccountingEnricherDispatcher: querying and forwarding.")
      // query the enriching patient information...
      // ...
      val lastName = "Doe"
      val carrier = "Kaiser"
      val socialSecurityNumber = "111-22-3333"
      val enrichedDoctorVisitCompleted = DoctorVisitCompleted(
          doctorVisitCompleted.patientId,
          doctorVisitCompleted.firstName,
          doctorVisitCompleted.date,
          PatientDetails(lastName, socialSecurityNumber, carrier))
      accountingSystemDispatcher forward enrichedDoctorVisitCompleted
      ContentEnricherDriver.completedStep()
    case _ =>
      println("AccountingEnricherDispatcher: received unexpected message")
  }
}

class AccountingSystemDispatcher extends Actor {
  def receive = {
    case doctorVisitCompleted: DoctorVisitCompleted =>
      println("AccountingSystemDispatcher: sending to Accounting System...")
      ContentEnricherDriver.completedStep()
    case _ =>
      println("AccountingSystemDispatcher: received unexpected message")
  }
}

class ScheduledDoctorVisit(val patientId: String, val firstName: String) extends Actor {
  var completedOn: Date = _

  def receive = {
    case visitCompleted: VisitCompleted =>
      println("ScheduledDoctorVisit: completing visit.")
      completedOn = new Date()
      visitCompleted.dispatcher ! new DoctorVisitCompleted(patientId, firstName, completedOn)
      ContentEnricherDriver.completedStep()
    case _ =>
      println("ScheduledDoctorVisit: received unexpected message")
  }
}
