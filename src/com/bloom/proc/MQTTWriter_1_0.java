package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import com.bloom.uuid.UUID;
import java.util.Map;
import java.util.TreeMap;

@PropertyTemplate(name="MQTTWriter", type=AdapterType.target, properties={@com.bloom.anno.PropertyTemplateProperty(name="brokerUri", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Topic", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="QoS", type=Integer.class, required=false, defaultValue="0"), @com.bloom.anno.PropertyTemplateProperty(name="clientId", type=String.class, required=false, defaultValue="")}, outputType=WAEvent.class, requiresFormatter=true)
public class MQTTWriter_1_0
  extends BaseWriter
{
  MQTTWriter mqttWriter;
  
  public void init(Map<String, Object> writerProperties, Map<String, Object> formatterProperties, UUID inputStream, String distributionID)
    throws Exception
  {
    super.init(writerProperties, formatterProperties, inputStream, distributionID);
    
    Map<String, Object> localPropertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    localPropertyMap.putAll(writerProperties);
    localPropertyMap.putAll(formatterProperties);
    this.mqttWriter = new MQTTWriter(localPropertyMap, this.formatter, this.fields);
    this.mqttWriter.open();
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    this.mqttWriter.write(event);
  }
  
  public void close()
    throws Exception
  {
    this.mqttWriter.close();
  }
}

