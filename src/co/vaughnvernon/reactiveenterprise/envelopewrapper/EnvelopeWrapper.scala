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

package co.vaughnvernon.reactiveenterprise.envelopewrapper

import scala.collection.immutable.Map

trait ReplyToSupport {
  def reply(message: Any) = { }
  def setUpReplyToSupport(returnAddress: String) = { }
}

trait RegisterCustomer {

}

trait RabbitMQReplyToSupport extends ReplyToSupport {
  override def reply(message: Any) = {
  }

  override def setUpReplyToSupport(returnAddress: String) = {
  }
}

case class RegisterCustomerRabbitMQReplyToMapEnvelope(
            mapMessage: Map[String, String])
        extends RegisterCustomer with RabbitMQReplyToSupport {

  this.setUpReplyToSupport(mapMessage("returnAddress"))
}

object EnvelopeWrapperDriver {

}