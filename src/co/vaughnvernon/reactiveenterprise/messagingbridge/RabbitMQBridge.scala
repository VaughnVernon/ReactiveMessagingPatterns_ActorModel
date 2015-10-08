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

package co.vaughnvernon.reactiveenterprise.messagingbridge

import java.io.IOException
import java.util.Date
import akka.actor._
import com.rabbitmq.client._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.AMQP.Queue.DeclareOk
import com.rabbitmq.client.QueueingConsumer.Delivery

object RabbitMQMessageType extends Enumeration {
    type RabbitMQMessageType = Value
    val Binary, Text = Value
}

import RabbitMQMessageType._

case class RabbitMQBridgeConfig(
    messageTypes: Array[String],
    settings: RabbitMQConnectionSettings,
    name: String,
    messageType: RabbitMQMessageType,
    durable: Boolean,
    exclusive: Boolean,
    autoAcknowledged: Boolean,
    autoDeleted: Boolean) {
  
  if (messageTypes == null)
    throw new IllegalArgumentException("Must provide empty messageTypes.")
  if (settings == null)
    throw new IllegalArgumentException("Must provide settings.")
  if (name == null || name.isEmpty)
    throw new IllegalArgumentException("Must provide name.")
}

case class RabbitMQConnectionSettings(
    hostName: String,
    port: Int,
    virtualHost: String,
    username: String,
    password: String) {
  
  def this() =
    this("localhost", -1, "/", null, null)
  
  def this(hostName: String, virtualHost: String) =
    this(hostName, -1, virtualHost, null, null)
  
  def hasPort(): Boolean =
    port > 0

  def hasUserCredentials(): Boolean =
    username != null && password != null
}

case class RabbitMQBinaryMessage(
    messageType: String,
    messageId: String,
    timestamp: Date,
    binaryMessage: Array[Byte],
    deliveryTag: Long,
    redelivery: Boolean)

case class RabbitMQTextMessage(
    messageType: String,
    messageId: String,
    timestamp: Date,
    textMessage: String,
    deliveryTag: Long,
    redelivery: Boolean)

abstract class RabbitMQBridgeActor(config: RabbitMQBridgeConfig) extends Actor {
  private val queueChannel = new QueueChannel(self, config)

/*-----------------------------------------------
  def receive = {
    case message: RabbitMQBinaryMessage =>
      ...
    case message: RabbitMQTextMessage =>
      ...
    case invalid: Any =>
      ...
  }
 -----------------------------------------------*/

}

private class QueueChannel(
    bridge: ActorRef,
    config: RabbitMQBridgeConfig) {
  
  var connection = factory.newConnection
  var channel = connection.createChannel
  var queueName = openQueue
  val consumer = new DispatchingConsumer(this, bridge)
  
  def autoAcknowledged(): Boolean =
    config.autoAcknowledged
  
  def close(): Unit = {
    try {
      if (channel != null && channel.isOpen) channel.close
    } catch {
      case t: Throwable => // fall through
    }

    try {
      if (connection != null && connection.isOpen) connection.close
    } catch {
      case t: Throwable => // fall through
    }

    channel = null
    connection = null
  }
  
  def closed(): Boolean =
    channel == null && connection == null

  def config(): RabbitMQBridgeConfig =
    config

  def hostName(): String =
    config.settings.hostName
  
  def messageTypes(): Array[String] =
    config.messageTypes
  
  private def factory() = {
    val factory = new ConnectionFactory()
    
    factory.setHost(config.settings.hostName)
    
    if (config.settings.hasPort()) factory.setPort(config.settings.port)
    
    factory.setVirtualHost(config.settings.virtualHost)
    
    if (config.settings.hasUserCredentials) {
      factory.setUsername(config.settings.username)
      factory.setPassword(config.settings.password)
    }
    
    factory
  }
  
  private def openQueue(): String = {
    val result =
        channel.queueDeclare(
                config.name,
                config.durable,
                config.exclusive,
                config.autoDeleted,
                null)

    result.getQueue
  }
}

private class DispatchingConsumer(
    queueChannel: QueueChannel,
    bridge: ActorRef)
  extends DefaultConsumer(queueChannel.channel) {
  
  override def handleDelivery(
      consumerTag: String,
      envelope: Envelope,
      properties: BasicProperties,
      body: Array[Byte]): Unit = {

    if (!queueChannel.closed) {
      handle(bridge, new Delivery(envelope, properties, body));
    }
  }

  override def handleShutdownSignal(
      consumerTag: String,
      signal: ShutdownSignalException): Unit = {
    queueChannel.close
  }
  
  private def handle(
      bridge: ActorRef,
      delivery: Delivery): Unit = {
    try {
      if (this.filteredMessageType(delivery)) {
        ;
      } else if (queueChannel.config.messageType == Binary) {
        bridge !
          RabbitMQBinaryMessage(
              delivery.getProperties.getType,
              delivery.getProperties.getMessageId,
              delivery.getProperties.getTimestamp,
              delivery.getBody,
              delivery.getEnvelope.getDeliveryTag,
              delivery.getEnvelope.isRedeliver)
      } else if (queueChannel.config.messageType == Text) {
        bridge !
            RabbitMQTextMessage(
              delivery.getProperties.getType,
              delivery.getProperties.getMessageId,
              delivery.getProperties.getTimestamp,
              new String(delivery.getBody),
              delivery.getEnvelope.getDeliveryTag,
              delivery.getEnvelope.isRedeliver)
      }

      this.ack(delivery)

    } catch {
      case e: MessageBridgeException =>
        // System.out.println("MESSAGE EXCEPTION (MessageConsumer): " + e.getMessage());
        nack(delivery, e.retry)
      case t: Throwable =>
        // System.out.println("EXCEPTION (MessageConsumer): " + t.getMessage());
        this.nack(delivery, false)
    }
  }

  private def ack(delivery: Delivery): Unit = {
    try {
      if (!queueChannel.autoAcknowledged) {
        this.getChannel().basicAck(
                        delivery.getEnvelope.getDeliveryTag,
                        false);
      }
    } catch {
        case e: IOException => // fall through
    }
  }

  private def nack(delivery: Delivery, retry: Boolean): Unit = {
    try {
      if (!queueChannel.autoAcknowledged) {
        this.getChannel().basicNack(
            delivery.getEnvelope.getDeliveryTag,
            false,
            retry)
      }
    } catch {
      case ioe: IOException => // fall through
    }
  }

  private def filteredMessageType(delivery: Delivery): Boolean = {
    var filtered = false

    if (!queueChannel.messageTypes.isEmpty) {
      val messageType = delivery.getProperties.getType

      if (messageType == null || !queueChannel.messageTypes.contains(messageType)) {
        filtered = true
      }
    }

    filtered
  }
}
