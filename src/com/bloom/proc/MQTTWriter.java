package com.bloom.proc;

import com.bloom.event.Event;
import com.bloom.intf.Formatter;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MQTTWriter
{
  public static final String TOPIC_NAME_PROP_KEY = "Topic";
  public static final String MESSAGE_QOS_PROP_KEY = "QoS";
  public static final String BROKER_URI_PROP_KEY = "brokerUri";
  public static final String CLIENT_ID_PROP_KEY = "clientId";
  private static final Logger logger = Logger.getLogger(MQTTWriter.class);
  private final Formatter formatter;
  private final String brokerUri;
  private final String topic;
  private final int qos;
  private final String clientId;
  private MqttClient mqttClient = null;
  
  public MQTTWriter(Map<String, Object> writerProperties, Formatter formatter, Field[] fields)
  {
    this.formatter = formatter;
    this.brokerUri = writerProperties.get("brokerUri").toString();
    this.topic = writerProperties.get("Topic").toString();
    this.qos = ((Integer)writerProperties.get("QoS")).intValue();
    this.clientId = writerProperties.get("clientId").toString();
  }
  
  public void open()
    throws Exception
  {
    if (this.mqttClient != null) {
      throw new IllegalStateException("mqttClient expected to be null at open(), while found to be non-null");
    }
    this.mqttClient = new MqttClient(this.brokerUri, this.clientId);
    MqttConnectOptions connectOptions = new MqttConnectOptions();
    connectOptions.setCleanSession(true);
  }
  
  public void close()
    throws Exception
  {
    if (this.mqttClient == null) {
      throw new IllegalStateException("mqttClient expected to be non-null at close, while found to be null");
    }
    this.mqttClient.disconnect();
  }
  
  public void write(Event event)
    throws Exception
  {
    if (!this.mqttClient.isConnected()) {
      this.mqttClient.connect();
    }
    MqttMessage message = new MqttMessage(this.formatter.format(event));
    message.setQos(this.qos);
    this.mqttClient.publish(this.topic, message);
  }
}

