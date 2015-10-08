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

package co.vaughnvernon.reactiveenterprise.processmanager2

import co.vaughnvernon.reactiveenterprise.CompletableApp
import akka.actor._
import akka.event.Logging
import akka.event.LoggingAdapter
import java.util.Random
import number9._

object ProcessManagerDriver2 extends CompletableApp(5) {
  Number9.initialize(system, new InMemoryMessageJournal(system))
  
  val creditBureau = EntityRef(system.actorOf(Props[CreditBureau], "creditBureau"))
  
  val bank1 = EntityRef(system.actorOf(Props(classOf[Bank], "bank1", 2.75, 0.30), "bank1"))
  val bank2 = EntityRef(system.actorOf(Props(classOf[Bank], "bank2", 2.73, 0.31), "bank2"))
  val bank3 = EntityRef(system.actorOf(Props(classOf[Bank], "bank3", 2.80, 0.29), "bank3"))
  
  val loanBroker = EntityRef(system.actorOf(
      Props(classOf[LoanBroker], creditBureau, Vector(bank1, bank2, bank3)),
      "loanBroker"))

  loanBroker ! QuoteBestLoanRate("111-11-1111", 100000, 84)
  
  awaitCompletion
}

//=========== ProcessManager

case class ProcessStarted(processId: String, process: EntityRef)
case class ProcessStopped(processId: String, process: EntityRef)

abstract class ProcessManager extends Entity {
  private var interestHandlers = Map[String, Vector[Function1[_, Unit]]]()  
  private var processes = Map[String, EntityRef]()  
  val log: LoggingAdapter = Logging.getLogger(context.system, self)
  
  private def dispatchToHandler(message: Any, interestName: String) = {
    if (interestHandlers.contains(interestName)) {
      interestHandlers(interestName) map { handler =>
	      handler.asInstanceOf[Any => Unit](message)
      }
    } else {
      log.error(s"Cannot dispatch message to: ${self.path} of class: ${getClass.getName} for message: ${message.getClass.getName} because no handlers:\n$interestHandlers")
    }
  }

  def processOf(processId: String): EntityRef = {
    if (processes.contains(processId)) {
      processes(processId)
    } else {
      null
    }
  }
  
  def receive = confirmingReceive {
    case message =>
      dispatchToHandler(message, message.getClass.getSimpleName)
  }

  def registerInterestHandler(handlerType: Class[_], handler: Function1[_, Unit]) = {
    val interestName = handlerType.getSimpleName
    if (interestHandlers.contains(interestName)) {
	    val pastInterests = interestHandlers(interestName)
	    val currentInterests = pastInterests :+ handler
	    interestHandlers = interestHandlers + (interestName -> currentInterests)
    } else {
	    interestHandlers = interestHandlers + (interestName -> Vector(handler))
    }
  }
  
  def startProcess(processId: String, process: EntityRef) = {
    if (!processes.contains(processId)) {
      processes = processes + (processId -> process)
      self ! ProcessStarted(processId, process)
    }
  }
  
  def stopProcess(processId: String) = {
    if (processes.contains(processId)) {
      val process = processes(processId)
      processes = processes - processId
      self ! ProcessStopped(processId, process)
    }
  }
}

//=========== LoanBroker

case class QuoteBestLoanRate(taxId: String, amount: Integer, termInMonths: Integer)
	extends CommandMessage

case class BestLoanRateQuoted(bankId: String, loanRateQuoteId: String, taxId: String,
    amount: Integer, termInMonths: Integer, creditScore: Integer, interestRate: Double)
    extends EventMessage

case class BestLoanRateDenied(loanRateQuoteId: String, taxId: String,
    amount: Integer, termInMonths: Integer, creditScore: Integer)
    extends EventMessage

class LoanBroker(creditBureau: EntityRef, banks: Seq[EntityRef]) extends ProcessManager {
  var entityId = self.path.name
  
  def on(message: BankLoanRateQuoted): Unit = {
    log.info(s"$message")
    
    processOf(message.loanQuoteReferenceId) !
    	RecordLoanRateQuote(
    	    message.bankId,
    	    message.bankLoanRateQuoteId,
    	    message.interestRate)
  }
  
  def on(message: CreditChecked): Unit = {
    log.info(s"$message")
    
    processOf(message.creditProcessingReferenceId) !
    	EstablishCreditScoreForLoanRateQuote(
    	    message.creditProcessingReferenceId,
    	    message.taxId,
    	    message.score)
  }
  
  def on(message: CreditScoreForLoanRateQuoteDenied): Unit = {
    log.info(s"$message")
    
    processOf(message.loanRateQuoteId) ! TerminateLoanRateQuote()
    
    ProcessManagerDriver2.completeAll
    
	val denied =
	  BestLoanRateDenied(message.loanRateQuoteId,
	    message.taxId, message.amount, message.termInMonths,
	    message.score)
    
    log.info(s"Would be sent to original requester: $denied")
  }
  
  def on(message: CreditScoreForLoanRateQuoteEstablished): Unit = {
    log.info(s"$message")
    
    banks map { bank =>
      bank ! QuoteLoanRate(
          message.loanRateQuoteId, message.taxId,
          message.score, message.amount, message.termInMonths)
    }
    
    ProcessManagerDriver2.completedStep
  }
  
  def on(message: LoanRateBestQuoteFilled): Unit = {
    log.info(s"$message")
    
    ProcessManagerDriver2.completedStep

    stopProcess(message.loanRateQuoteId)
    
    val best = BestLoanRateQuoted(message.bestBankLoanRateQuote.bankId,
        message.loanRateQuoteId, message.taxId,
        message.amount, message.termInMonths,
        message.creditScore,
        message.bestBankLoanRateQuote.interestRate)
    
    log.info(s"Would be sent to original requester: $best")
  }
  
  def on(message: LoanRateQuoteRecorded): Unit = {
    log.info(s"$message")
    
    ProcessManagerDriver2.completedStep
  }
  
  def on(message: LoanRateQuoteStarted): Unit = {
    log.info(s"$message")
    
    creditBureau ! CheckCredit(message.loanRateQuoteId, message.taxId)
  }
  
  def on(message: LoanRateQuoteTerminated): Unit = {
    log.info(s"$message")

    stopProcess(message.loanRateQuoteId)
  }
  
  def on(message: ProcessStarted): Unit = {
    log.info(s"$message")
    
	message.process ! StartLoanRateQuote(banks.size)
  }
  
  def on(message: ProcessStopped): Unit = {
    log.info(s"$message")
    
    //context.stop(message.process)
  }
  
  def on(message: QuoteBestLoanRate): Unit = {
    val loanRateQuoteId = LoanRateQuote.id

    log.info(s"$message for: $loanRateQuoteId")
    
    val loanRateQuote =
      LoanRateQuote(
          context.system,
          loanRateQuoteId,
          message.taxId,
          message.amount,
          message.termInMonths,
          EntityRef(self))
    
    startProcess(loanRateQuoteId, loanRateQuote)
  }
  
  registerInterestHandler(classOf[BankLoanRateQuoted], (message: BankLoanRateQuoted) => on(message))
  registerInterestHandler(classOf[CreditChecked], (message: CreditChecked) => on(message))
  registerInterestHandler(classOf[CreditScoreForLoanRateQuoteDenied], (message: CreditScoreForLoanRateQuoteDenied) => on(message))
  registerInterestHandler(classOf[CreditScoreForLoanRateQuoteEstablished], (message: CreditScoreForLoanRateQuoteEstablished) => on(message))
  registerInterestHandler(classOf[LoanRateBestQuoteFilled], (message: LoanRateBestQuoteFilled) => on(message))
  registerInterestHandler(classOf[LoanRateQuoteRecorded], (message: LoanRateQuoteRecorded) => on(message))
  registerInterestHandler(classOf[LoanRateQuoteStarted], (message: LoanRateQuoteStarted) => on(message))
  registerInterestHandler(classOf[LoanRateQuoteTerminated], (message: LoanRateQuoteTerminated) => on(message))
  registerInterestHandler(classOf[ProcessStarted], (message: ProcessStarted) => on(message))
  registerInterestHandler(classOf[ProcessStopped], (message: ProcessStopped) => on(message))
  registerInterestHandler(classOf[QuoteBestLoanRate], (message: QuoteBestLoanRate) => on(message))
}

//=========== LoanRateQuote

case class StartLoanRateQuote(expectedLoanRateQuotes: Integer)
	extends CommandMessage
case class LoanRateQuoteStarted(loanRateQuoteId: String, taxId: String)
	extends EventMessage
case class TerminateLoanRateQuote()
	extends CommandMessage
case class LoanRateQuoteTerminated(loanRateQuoteId: String, taxId: String)
	extends EventMessage

case class EstablishCreditScoreForLoanRateQuote(
    loanRateQuoteId: String, taxId: String, score: Integer)
    extends CommandMessage
case class CreditScoreForLoanRateQuoteEstablished(
    loanRateQuoteId: String, taxId: String,
    score: Integer, amount: Integer, termInMonths: Integer)
    extends EventMessage
case class CreditScoreForLoanRateQuoteDenied(
    loanRateQuoteId: String, taxId: String,
    amount: Integer, termInMonths: Integer, score: Integer)
    extends EventMessage

case class RecordLoanRateQuote(
    bankId: String, bankLoanRateQuoteId: String, interestRate: Double)
    extends CommandMessage
case class LoanRateQuoteRecorded(
    loanRateQuoteId: String, taxId: String, bankLoanRateQuote: BankLoanRateQuote)
    extends EventMessage
case class LoanRateBestQuoteFilled(
    loanRateQuoteId: String, taxId: String,
    amount: Integer, termInMonths: Integer,
    creditScore: Integer, bestBankLoanRateQuote: BankLoanRateQuote)
    extends EventMessage
case class BankLoanRateQuote(
    bankId: String, bankLoanRateQuoteId: String, interestRate: Double)
    extends DocumentMessage

object LoanRateQuote {
  val randomLoanRateQuoteId = new Random()
  
  def apply(
      system: ActorSystem,
      loanRateQuoteId: String,
      taxId: String,
      amount: Integer, termInMonths: Integer,
      loanBroker: EntityRef): EntityRef = {
    
	val loanRateQuote =
	  system.actorOf(
	    Props(
	        classOf[LoanRateQuote],
	            loanRateQuoteId, taxId,
	            amount, termInMonths, loanBroker),
	        "loanRateQuote-" + loanRateQuoteId)
    
    EntityRef(loanRateQuote)
  }
  
  def id() = {
    randomLoanRateQuoteId.nextInt(1000).toString
  }
}

class LoanRateQuote(
    loanRateQuoteId: String,
    taxId: String,
    amount: Integer,
    termInMonths: Integer,
    loanBroker: EntityRef) extends Entity {
  
  var entityId = self.path.name
  
  var bankLoanRateQuotes = Vector[BankLoanRateQuote]()
  var creditRatingScore: Int = _
  var expectedLoanRateQuotes: Int = _
  
  private def bestBankLoanRateQuote() = {
    var best = bankLoanRateQuotes(0)
    
    bankLoanRateQuotes map { bankLoanRateQuote =>
      if (best.interestRate > bankLoanRateQuote.interestRate) {
        best = bankLoanRateQuote
      }
    }
    
    best
  }
  
  private def quotableCreditScore(score: Integer): Boolean = {
    score > 399
  }
  
  def receive = confirmingReceive {
    case message: StartLoanRateQuote =>
      expectedLoanRateQuotes = message.expectedLoanRateQuotes
      loanBroker ! LoanRateQuoteStarted(loanRateQuoteId, taxId)

    case message: EstablishCreditScoreForLoanRateQuote =>
      creditRatingScore = message.score
      if (quotableCreditScore(creditRatingScore))
    	loanBroker ! CreditScoreForLoanRateQuoteEstablished(
    	    loanRateQuoteId, taxId,
    	    creditRatingScore, amount, termInMonths)
      else
        loanBroker ! CreditScoreForLoanRateQuoteDenied(
            loanRateQuoteId, taxId,
            amount, termInMonths, creditRatingScore)

    case message: RecordLoanRateQuote =>
      val bankLoanRateQuote =
    	BankLoanRateQuote(
  		    message.bankId,
  		    message.bankLoanRateQuoteId,
  		    message.interestRate)
      bankLoanRateQuotes = bankLoanRateQuotes :+ bankLoanRateQuote
      loanBroker ! LoanRateQuoteRecorded(loanRateQuoteId, taxId, bankLoanRateQuote)
      if (bankLoanRateQuotes.size >= expectedLoanRateQuotes)
        loanBroker ! LoanRateBestQuoteFilled(
            loanRateQuoteId, taxId,
            amount, termInMonths,
            creditRatingScore, bestBankLoanRateQuote)
      
    case message: TerminateLoanRateQuote =>
      loanBroker ! LoanRateQuoteTerminated(loanRateQuoteId, taxId)
  }
}

//=========== CreditBureau

case class CheckCredit(creditProcessingReferenceId: String, taxId: String)
    extends CommandMessage

case class CreditChecked(creditProcessingReferenceId: String, taxId: String, score: Integer)
    extends EventMessage

class CreditBureau extends Entity {
  var entityId = self.path.name
  
  val creditRanges = Vector(300, 400, 500, 600, 700)
  val randomCreditRangeGenerator = new Random()
  val randomCreditScoreGenerator = new Random()
  
  def receive = confirmingReceive {
    case message: CheckCredit =>
      val range = creditRanges(randomCreditRangeGenerator.nextInt(5))
      val score = range + randomCreditScoreGenerator.nextInt(20)
      sender ! CreditChecked(message.creditProcessingReferenceId, message.taxId, score)
  }
}

//=========== Bank

case class QuoteLoanRate(loanQuoteReferenceId: String, taxId: String,
    creditScore: Integer, amount: Integer, termInMonths: Integer)
    extends CommandMessage

case class BankLoanRateQuoted(bankId: String, bankLoanRateQuoteId: String,
    loanQuoteReferenceId: String, taxId: String,
    interestRate: Double)
    extends EventMessage

class Bank(bankId: String, primeRate: Double, ratePremium: Double) extends Entity {
  var entityId = self.path.name
  
  val randomDiscount = new Random()
  val randomQuoteId = new Random()
  
  private def calculateInterestRate(
      amount: Double,
      months: Double,
      creditScore: Double): Double = {
    
    val creditScoreDiscount = creditScore / 100.0 / 10.0 -
            (randomDiscount.nextInt(5) * 0.05)

    primeRate + ratePremium + ((months / 12.0) / 10.0) -
            creditScoreDiscount
  }
  
  def receive = confirmingReceive {
    case message: QuoteLoanRate =>
      val interestRate =
        calculateInterestRate(
            message.amount.toDouble,
            message.termInMonths.toDouble,
            message.creditScore.toDouble)
            
      sender ! BankLoanRateQuoted(
          bankId, randomQuoteId.nextInt(1000).toString,
          message.loanQuoteReferenceId, message.taxId, interestRate)
  }
}
