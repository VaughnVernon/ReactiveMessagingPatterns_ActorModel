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

package co.vaughnvernon.reactiveenterprise.returnaddress

import akka.actor._
import co.vaughnvernon.reactiveenterprise._

case class Request(what: String)
case class RequestComplex(what: String)
case class Reply(what: String)
case class ReplyToComplex(what: String)
case class StartWith(server: ActorRef)

object ReturnAddressDriver extends CompletableApp(2) {
  val client = system.actorOf(Props[Client], "client")
  val server = system.actorOf(Props[Server], "server")
  client ! StartWith(server)
  
  awaitCompletion
  println("ReturnAddress: is completed.")
}

class Client extends Actor {
  def receive = {
    case StartWith(server) =>
      println("Client: is starting...")
      server ! Request("REQ-1")
      server ! RequestComplex("REQ-20")
    case Reply(what) =>
      println("Client: received reply: " + what)
      ReturnAddressDriver.completedStep()
    case ReplyToComplex(what) =>
      println("Client: received reply to complex: " + what)
      ReturnAddressDriver.completedStep()
    case _ =>
      println("Client: received unexpected message")
  }
}

class Server extends Actor {
  val worker = context.actorOf(Props[Worker], "worker")
  
  def receive = {
    case request: Request =>
      println("Server: received request value: " + request.what)
      sender ! Reply("RESP-1 for " + request.what)
    case request: RequestComplex =>
      println("Server: received request value: " + request.what)
      worker forward request
    case _ =>
      println("Server: received unexpected message")
  }
}

class Worker extends Actor {
  def receive = {
    case RequestComplex(what) =>
      println("Worker: received complex request value: " + what)
      sender ! ReplyToComplex("RESP-2000 for " + what)
    case _ =>
      println("Worker: received unexpected message")
  }
}
