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

package co.vaughnvernon.reactiveenterprise.requestreply

import akka.actor._
import co.vaughnvernon.reactiveenterprise._

case class Request(what: String)
case class Reply(what: String)
case class StartWith(server: ActorRef)

object RequestReplyDriver extends CompletableApp(1) {
  val client = system.actorOf(Props[Client], "client")
  val server = system.actorOf(Props[Server], "server")
  client ! StartWith(server)
  
  awaitCompletion
  println("RequestReply: is completed.")
}

class Client extends Actor {
  def receive = {
	  case StartWith(server) =>
	    println("Client: is starting...")
	    server ! Request("REQ-1")
	  case Reply(what) =>
	    println("Client: received response: " + what)
	    RequestReplyDriver.completedStep()
	  case _ =>
	    println("Client: received unexpected message")
  }
}

class Server extends Actor {
  def receive = {
      case Request(what) =>
        println("Server: received request value: " + what)
        sender ! Reply("RESP-1 for " + what)
	  case _ =>
	    println("Server: received unexpected message")
  }
}
