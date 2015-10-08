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

case class RequestWithReplyTo(what: String, replyTo: ActorRef)

object ReturnAddressInMessageDriver extends CompletableApp(1) {
  val client = system.actorOf(Props[ClientSendsReplyTo], "client")
  val server = system.actorOf(Props[ServerRepliesTo], "server")
  client ! StartWith(server)
  
  awaitCompletion
  println("ReturnAddressInMessage: is completed.")
}

class ClientSendsReplyTo extends Actor {
  def receive = {
	  case StartWith(server) =>
	    println("Client: is starting...")
	    server ! RequestWithReplyTo("REQ-1", self)
	  case Reply(what) =>
	    println("Client: received response: " + what)
	    ReturnAddressInMessageDriver.completedStep()
	  case _ =>
	    println("Client: received unexpected message")
  }
}

class ServerRepliesTo extends Actor {
  def receive = {
      case RequestWithReplyTo(what, replyTo) =>
        println("Server: received request value: " + what)
        replyTo ! Reply("RESP-1 for " + what)
	  case _ =>
	    println("Server: received unexpected message")
  }
}
