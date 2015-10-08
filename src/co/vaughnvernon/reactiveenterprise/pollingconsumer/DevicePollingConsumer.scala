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

package co.vaughnvernon.reactiveenterprise.pollingconsumer

import scala.util.Random
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._
import ExecutionContext.Implicits.global
import java.util.Date
import java.util.concurrent.TimeUnit
import akka.actor._
import co.vaughnvernon.reactiveenterprise._

object DevicePollingConsumerDriver extends CompletableApp(10) {
  val evenNumberDevice = new EvenNumberDevice()
  val monitor = system.actorOf(Props(classOf[EvenNumberMonitor], evenNumberDevice), "evenNumberMonitor")
  
  monitor ! Monitor()
  
  awaitCompletion
  
  println("DevicePollingConsumerDriver: completed.")
}

case class Monitor()

class EvenNumberMonitor(evenNumberDevice: EvenNumberDevice) extends Actor {
  val scheduler =
            new CappedBackOffScheduler(
                    500,
                    15000,
                    context.system,
                    self,
                    Monitor())
  
  def monitor = {
    val evenNumber = evenNumberDevice.nextEvenNumber(3)
    if (evenNumber.isDefined) {
      println(s"EVEN: ${evenNumber.get}")
      scheduler.reset
      DevicePollingConsumerDriver.completedStep
    } else {
      println(s"MISS")
      scheduler.backOff
    }
  }
  
  def receive = {
    case request: Monitor =>
      monitor
  }
}

class CappedBackOffScheduler(
    minimumInterval: Int,
    maximumInterval: Int,
    system: ActorSystem,
    receiver: ActorRef,
    message: Any) {
  
  var interval = minimumInterval
  
  def backOff = {
    interval = interval * 2
    if (interval > maximumInterval) interval = maximumInterval
    schedule
  }
  
  def reset = {
    interval = minimumInterval
    schedule
  }
  
  private def schedule = {
    val duration = Duration.create(interval, TimeUnit.MILLISECONDS)
    system.scheduler.scheduleOnce(duration, receiver, message)
  }
}

class EvenNumberDevice() {
  val random = new Random(99999)
  
  def nextEvenNumber(waitFor: Int): Option[Int] = {
    val timeout = new Timeout(waitFor)
    var nextEvenNumber: Option[Int] = None
    
    while (!timeout.isTimedOut && nextEvenNumber.isEmpty) {
      Thread.sleep(waitFor / 2)
      
      val number = random.nextInt(100000)
    
      if (number % 2 == 0) nextEvenNumber = Option(number)
    }
    
    nextEvenNumber
  }
  
  def nextEvenNumber(): Option[Int] = {
    nextEvenNumber(-1)
  }
}

class Timeout(withinMillis: Int) {
  val mark = currentTime
  
  def isTimedOut(): Boolean = {
    if (withinMillis == -1) false
    else currentTime - mark >= withinMillis
  }
  
  private def currentTime(): Long = {
    (new Date()).getTime
  }
}
