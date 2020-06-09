/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.mqtt.test.server;

import io.netty.handler.codec.DecoderException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServerOptions;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.internal.ClientComms;
import org.eclipse.paho.client.mqttv3.internal.Token;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.Stream;

/**
 * MQTT server testing about the maximum message size
 */
@RunWith(VertxUnitRunner.class)
public class MqttServerMaxMessageSizeTest extends MqttServerBaseTest {

  private static final Logger log = LoggerFactory.getLogger(MqttServerMaxMessageSizeTest.class);
  private static final String MQTT_TOPIC = "/my_topic";
  private static final int MQTT_MAX_MESSAGE_SIZE = 50;
  private static final int MQTT_BIG_MESSAGE_SIZE = MQTT_MAX_MESSAGE_SIZE + 1;

  static {
    Stream.of(ClientComms.class.getName(), Token.class.getName()).forEach(n->{
      java.util.logging.Logger logger = java.util.logging.Logger.getLogger(n);
      logger.setLevel(Level.FINE);
      ConsoleHandler consoleHandler = new ConsoleHandler();
      consoleHandler.setLevel(Level.FINE);
      consoleHandler.setFormatter(new LoggerFormat());
      logger.addHandler(consoleHandler);
    });

  }

  private Async async;

  @Before
  public void before(TestContext context) {

    MqttServerOptions options = new MqttServerOptions();
    options.setMaxMessageSize(MQTT_MAX_MESSAGE_SIZE);

    this.setUp(context, options);
  }

  @Test
  public void publishBigMessage(TestContext context) {

    this.async = context.async();

    try {

      MemoryPersistence persistence = new MemoryPersistence();
      MqttAsyncClient client = new MqttAsyncClient(String.format("tcp://%s:%d", MQTT_SERVER_HOST, MQTT_SERVER_PORT), "12345", persistence);
      client.connect().waitForCompletion();

      byte[] message = new byte[MQTT_BIG_MESSAGE_SIZE];

      client.publish(MQTT_TOPIC, message, 0, false).waitForCompletion();

      context.assertTrue(true);

    } catch (MqttException e) {

      e.printStackTrace();
      context.assertTrue(false);
    }
  }

  @After
  public void after(TestContext context) {

    this.tearDown(context);
  }

  @Override
  protected void endpointHandler(MqttEndpoint endpoint, TestContext context) {

    endpoint.exceptionHandler(t -> {
      log.error("Exception raised", t);

      if (t instanceof DecoderException) {
        this.async.complete();
      }

    });

    endpoint.accept(false);
  }

  static class LoggerFormat extends Formatter {
    @Override
    public String format(LogRecord record) {
      Date date = new Date();
      String sDate = date.toString();
      return "[" + sDate + "]" + "[" + record.getLevel() + "]"
        + record.getSourceClassName() + record.getMessage() + "\n";
    }
  }
}
