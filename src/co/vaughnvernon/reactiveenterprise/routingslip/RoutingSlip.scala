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

package co.vaughnvernon.reactiveenterprise.routingslip

import akka.actor._
import co.vaughnvernon.reactiveenterprise._

case class CustomerInformation(val name: String, val federalTaxId: String)

case class ContactInformation(val postalAddress: PostalAddress, val telephone: Telephone)

case class PostalAddress(
    val address1: String, val address2: String,
    val city: String, val state: String, val zipCode: String)

case class Telephone(val number: String)

case class ServiceOption(val id: String, val description: String)

case class RegistrationData(val customerInformation: CustomerInformation, val contactInformation: ContactInformation, val serviceOption: ServiceOption)

case class ProcessStep(val name: String, val processor: ActorRef)

case class RegistrationProcess(val processId: String, val processSteps: Seq[ProcessStep], val currentStep: Int) {
  def this(processId: String, processSteps: Seq[ProcessStep]) {
    this(processId, processSteps, 0)
  }

  def isCompleted: Boolean = {
    currentStep >= processSteps.size
  }

  def nextStep(): ProcessStep = {
    if (isCompleted) {
      throw new IllegalStateException("Process had already completed.")
    }
    
    processSteps(currentStep)
  }

  def stepCompleted(): RegistrationProcess = {
    new RegistrationProcess(processId, processSteps, currentStep + 1)
  }
}

case class RegisterCustomer(val registrationData: RegistrationData, val registrationProcess: RegistrationProcess) {
  def advance():Unit = {
    val advancedProcess = registrationProcess.stepCompleted
    if (!advancedProcess.isCompleted) {
      advancedProcess.nextStep().processor ! RegisterCustomer(registrationData, advancedProcess)
    }
    RoutingSlipDriver.completedStep()
  }
}

object RoutingSlipDriver extends CompletableApp(4) {
  val processId = java.util.UUID.randomUUID().toString
  
  val step1 = ProcessStep("create_customer", ServiceRegistry.customerVault(system, processId))
  val step2 = ProcessStep("set_up_contact_info", ServiceRegistry.contactKeeper(system, processId))
  val step3 = ProcessStep("select_service_plan", ServiceRegistry.servicePlanner(system, processId))
  val step4 = ProcessStep("check_credit", ServiceRegistry.creditChecker(system, processId))
  
  val registrationProcess = new RegistrationProcess(processId, Vector(step1, step2, step3, step4))
  
  val registrationData =
          new RegistrationData(
            CustomerInformation("ABC, Inc.", "123-45-6789"),
            ContactInformation(
                PostalAddress("123 Main Street", "Suite 100", "Boulder", "CO", "80301"),
                Telephone("303-555-1212")),
            ServiceOption("99-1203", "A description of 99-1203."))
  
  val registerCustomer = RegisterCustomer(registrationData, registrationProcess)
  
  registrationProcess.nextStep().processor ! registerCustomer

  awaitCompletion
  println("RoutingSlip: is completed.")
}

class CreditChecker extends Actor {
  def receive = {
    case registerCustomer: RegisterCustomer =>
      val federalTaxId = registerCustomer.registrationData.customerInformation.federalTaxId
      println(s"CreditChecker: handling register customer to perform credit check: $federalTaxId")
      registerCustomer.advance()
      context.stop(self)
    case message: Any =>
      println(s"CreditChecker: received unexpected message: $message")
  }
}

class ContactKeeper extends Actor {
  def receive = {
    case registerCustomer: RegisterCustomer =>
      val contactInfo = registerCustomer.registrationData.contactInformation
      println(s"ContactKeeper: handling register customer to keep contact information: $contactInfo")
      registerCustomer.advance()
      context.stop(self)
    case message: Any =>
      println(s"ContactKeeper: received unexpected message: $message")
  }
}

class CustomerVault extends Actor {
  def receive = {
    case registerCustomer: RegisterCustomer =>
      val customerInformation = registerCustomer.registrationData.customerInformation
      println(s"CustomerVault: handling register customer to create a new custoner: $customerInformation")
      registerCustomer.advance()
      context.stop(self)
    case message: Any =>
      println(s"CustomerVault: received unexpected message: $message")
  }
}

class ServicePlanner extends Actor {
  def receive = {
    case registerCustomer: RegisterCustomer =>
      val serviceOption = registerCustomer.registrationData.serviceOption
      println(s"ServicePlanner: handling register customer to plan a new customer service: $serviceOption")
      registerCustomer.advance()
      context.stop(self)
    case message: Any =>
      println(s"ServicePlanner: received unexpected message: $message")
  }
}

object ServiceRegistry {
  def contactKeeper(system: ActorSystem, id: String) = {
    system.actorOf(Props[ContactKeeper], "contactKeeper-" + id)
  }
  
  def creditChecker(system: ActorSystem, id: String) = {
    system.actorOf(Props[CreditChecker], "creditChecker-" + id)
  }
  
  def customerVault(system: ActorSystem, id: String) = {
    system.actorOf(Props[CustomerVault], "customerVault-" + id)
  }
  
  def servicePlanner(system: ActorSystem, id: String) = {
    system.actorOf(Props[ServicePlanner], "servicePlanner-" + id)
  }
}
