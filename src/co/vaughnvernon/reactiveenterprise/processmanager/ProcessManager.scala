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

package co.vaughnvernon.reactiveenterprise.processmanager

import co.vaughnvernon.reactiveenterprise.CompletableApp
import akka.actor._
import akka.event.Logging
import akka.event.LoggingAdapter
import java.util.Random

object ProcessManagerDriver extends CompletableApp(5) {
  val creditBureau =
          system.actorOf(
              Props[CreditBureau],
              "creditBureau")
  
  val bank1 =
          system.actorOf(
              Props(classOf[Bank], "bank1", 2.75, 0.30),
              "bank1")
  val bank2 =
          system.actorOf(
              Props(classOf[Bank], "bank2", 2.73, 0.31),
              "bank2")
  val bank3 =
          system.actorOf(
              Props(classOf[Bank], "bank3", 2.80, 0.29),
              "bank3")
  
  val loanBroker = system.actorOf(
      Props(classOf[LoanBroker],
            creditBureau,
            Vector(bank1, bank2, bank3)),
      "loanBroker")

  loanBroker ! QuoteBestLoanRate("111-11-1111", 100000, 84)
  
  awaitCompletion
}

//=========== ProcessManager

case class ProcessStarted(
    processId: String,
    process: ActorRef)
    
case class ProcessStopped(
    processId: String,
    process: ActorRef)

abstract class ProcessManager extends Actor {
  private var processes = Map[String, ActorRef]()  
  val log: LoggingAdapter = Logging.getLogger(context.system, self)
  
  def processOf(processId: String): ActorRef = {
    if (processes.contains(processId)) {
      processes(processId)
    } else {
      null
    }
  }
  
  def startProcess(processId: String, process: ActorRef) = {
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

case class QuoteBestLoanRate(
    taxId: String,
    amount: Integer,
    termInMonths: Integer)

case class BestLoanRateQuoted(
    bankId: String,
    loanRateQuoteId: String,
    taxId: String,
    amount: Integer,
    termInMonths: Integer,
    creditScore: Integer,
    interestRate: Double)

case class BestLoanRateDenied(
    loanRateQuoteId: String,
    taxId: String,
    amount: Integer,
    termInMonths: Integer,
    creditScore: Integer)

class LoanBroker(
    creditBureau: ActorRef,
    banks: Seq[ActorRef])
  extends ProcessManager {

  def receive = {
    case message: BankLoanRateQuoted =>
      log.info(s"$message")
    
      processOf(message.loadQuoteReferenceId) !
          RecordLoanRateQuote(
              message.bankId,
              message.bankLoanRateQuoteId,
              message.interestRate)
  
    case message: CreditChecked =>
      log.info(s"$message")
    
      processOf(message.creditProcessingReferenceId) !
          EstablishCreditScoreForLoanRateQuote(
              message.creditProcessingReferenceId,
              message.taxId,
              message.score)
  
    case message: CreditScoreForLoanRateQuoteDenied =>
      log.info(s"$message")
    
      processOf(message.loanRateQuoteId) !
              TerminateLoanRateQuote()
    
      ProcessManagerDriver.completeAll
    
      val denied =
            BestLoanRateDenied(
                message.loanRateQuoteId,
                message.taxId,
                message.amount,
                message.termInMonths,
                message.score)

      log.info(s"Would be sent to original requester: $denied")
  
    case message: CreditScoreForLoanRateQuoteEstablished =>
      log.info(s"$message")
    
      banks map { bank =>
        bank ! QuoteLoanRate(
          message.loanRateQuoteId,
          message.taxId,
          message.score,
          message.amount,
          message.termInMonths)
      }
    
      ProcessManagerDriver.completedStep
  
    case message: LoanRateBestQuoteFilled =>
      log.info(s"$message")
    
      ProcessManagerDriver.completedStep

      stopProcess(message.loanRateQuoteId)
    
      val best = BestLoanRateQuoted(
          message.bestBankLoanRateQuote.bankId,
          message.loanRateQuoteId,
          message.taxId,
          message.amount,
          message.termInMonths,
          message.creditScore,
          message.bestBankLoanRateQuote.interestRate)
    
      log.info(s"Would be sent to original requester: $best")
  
    case message: LoanRateQuoteRecorded =>
      log.info(s"$message")
    
      ProcessManagerDriver.completedStep
  
    case message: LoanRateQuoteStarted =>
      log.info(s"$message")
    
      creditBureau ! CheckCredit(
          message.loanRateQuoteId,
          message.taxId)
  
    case message: LoanRateQuoteTerminated =>
      log.info(s"$message")

      stopProcess(message.loanRateQuoteId)
  
    case message: ProcessStarted =>
      log.info(s"$message")
    
      message.process ! StartLoanRateQuote(banks.size)
  
    case message: ProcessStopped =>
      log.info(s"$message")
    
      context.stop(message.process)
  
    case message: QuoteBestLoanRate =>
      val loanRateQuoteId = LoanRateQuote.id

      log.info(s"$message for: $loanRateQuoteId")
    
      val loanRateQuote =
            LoanRateQuote(
                context.system,
                loanRateQuoteId,
                message.taxId,
                message.amount,
                message.termInMonths,
                self)
    
      startProcess(loanRateQuoteId, loanRateQuote)
  }
}

//=========== LoanRateQuote

case class StartLoanRateQuote(
    expectedLoanRateQuotes: Integer)
    
case class LoanRateQuoteStarted(
    loanRateQuoteId: String,
    taxId: String)
    
case class TerminateLoanRateQuote()

case class LoanRateQuoteTerminated(
    loanRateQuoteId: String,
    taxId: String)

case class EstablishCreditScoreForLoanRateQuote(
    loanRateQuoteId: String,
    taxId: String,
    score: Integer)
    
case class CreditScoreForLoanRateQuoteEstablished(
    loanRateQuoteId: String,
    taxId: String,
    score: Integer,
    amount: Integer,
    termInMonths: Integer)
    
case class CreditScoreForLoanRateQuoteDenied(
    loanRateQuoteId: String,
    taxId: String,
    amount: Integer,
    termInMonths: Integer,
    score: Integer)

case class RecordLoanRateQuote(
    bankId: String,
    bankLoanRateQuoteId: String,
    interestRate: Double)
    
case class LoanRateQuoteRecorded(
    loanRateQuoteId: String,
    taxId: String,
    bankLoanRateQuote: BankLoanRateQuote)
    
case class LoanRateBestQuoteFilled(
    loanRateQuoteId: String,
    taxId: String,
    amount: Integer,
    termInMonths: Integer,
    creditScore: Integer,
    bestBankLoanRateQuote: BankLoanRateQuote)
    
case class BankLoanRateQuote(
    bankId: String,
    bankLoanRateQuoteId: String,
    interestRate: Double)

object LoanRateQuote {
  val randomLoanRateQuoteId = new Random()
  
  def apply(
      system: ActorSystem,
      loanRateQuoteId: String,
      taxId: String,
      amount: Integer,
      termInMonths: Integer,
      loanBroker: ActorRef): ActorRef = {
    
	val loanRateQuote =
	  system.actorOf(
	    Props(
	        classOf[LoanRateQuote],
	            loanRateQuoteId, taxId,
	            amount, termInMonths, loanBroker),
	        "loanRateQuote-" + loanRateQuoteId)
    
    loanRateQuote
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
    loanBroker: ActorRef)
  extends Actor {
  
  var bankLoanRateQuotes = Vector[BankLoanRateQuote]()
  var creditRatingScore: Int = _
  var expectedLoanRateQuotes: Int = _
  
  private def bestBankLoanRateQuote() = {
    var best = bankLoanRateQuotes(0)
    
    bankLoanRateQuotes map { bankLoanRateQuote =>
      if (best.interestRate >
           bankLoanRateQuote.interestRate) {
        best = bankLoanRateQuote
      }
    }
    
    best
  }
  
  private def quotableCreditScore(
      score: Integer): Boolean = {
    score > 399
  }
  
  def receive = {
    case message: StartLoanRateQuote =>
      expectedLoanRateQuotes =
        message.expectedLoanRateQuotes
      loanBroker !
          LoanRateQuoteStarted(
              loanRateQuoteId,
              taxId)

    case message: EstablishCreditScoreForLoanRateQuote =>
      creditRatingScore = message.score
      if (quotableCreditScore(creditRatingScore))
    	loanBroker !
    	    CreditScoreForLoanRateQuoteEstablished(
                loanRateQuoteId,
                taxId,
                creditRatingScore,
                amount,
                termInMonths)
      else
        loanBroker !
            CreditScoreForLoanRateQuoteDenied(
                loanRateQuoteId,
                taxId,
                amount,
                termInMonths,
                creditRatingScore)

    case message: RecordLoanRateQuote =>
      val bankLoanRateQuote =
    	BankLoanRateQuote(
  		    message.bankId,
  		    message.bankLoanRateQuoteId,
  		    message.interestRate)
      bankLoanRateQuotes =
        bankLoanRateQuotes :+ bankLoanRateQuote
      loanBroker !
            LoanRateQuoteRecorded(
                loanRateQuoteId,
                taxId,
                bankLoanRateQuote)

      if (bankLoanRateQuotes.size >=
            expectedLoanRateQuotes)
        loanBroker !
            LoanRateBestQuoteFilled(
                loanRateQuoteId,
                taxId,
                amount,
                termInMonths,
                creditRatingScore,
                bestBankLoanRateQuote)
      
    case message: TerminateLoanRateQuote =>
      loanBroker !
          LoanRateQuoteTerminated(
              loanRateQuoteId,
              taxId)
  }
}

//=========== CreditBureau

case class CheckCredit(
    creditProcessingReferenceId: String,
    taxId: String)

case class CreditChecked(
    creditProcessingReferenceId: String,
    taxId: String,
    score: Integer)

class CreditBureau extends Actor {
  val creditRanges = Vector(300, 400, 500, 600, 700)
  val randomCreditRangeGenerator = new Random()
  val randomCreditScoreGenerator = new Random()
  
  def receive = {
    case message: CheckCredit =>
      val range =
        creditRanges(
            randomCreditRangeGenerator.nextInt(5))
      val score =
        range
        + randomCreditScoreGenerator.nextInt(20)
        
      sender !
          CreditChecked(
              message.creditProcessingReferenceId,
              message.taxId,
              score)
  }
}

//=========== Bank

case class QuoteLoanRate(
    loadQuoteReferenceId: String,
    taxId: String,
    creditScore: Integer,
    amount: Integer,
    termInMonths: Integer)

case class BankLoanRateQuoted(
    bankId: String,
    bankLoanRateQuoteId: String,
    loadQuoteReferenceId: String,
    taxId: String,
    interestRate: Double)

class Bank(
    bankId: String,
    primeRate: Double,
    ratePremium: Double)
  extends Actor {
  
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
  
  def receive = {
    case message: QuoteLoanRate =>
      val interestRate =
        calculateInterestRate(
            message.amount.toDouble,
            message.termInMonths.toDouble,
            message.creditScore.toDouble)
            
      sender ! BankLoanRateQuoted(
          bankId, randomQuoteId.nextInt(1000).toString,
          message.loadQuoteReferenceId, message.taxId, interestRate)
  }
}
