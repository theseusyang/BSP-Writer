package com.bloom.proc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.StringArrayEvent;
import com.bloom.uuid.UUID;
import java.util.Map;
import org.apache.log4j.Logger;
/*
 * 此类负责将事件数据实时地写入到Hazelcast集群中的节点上
 * 
 */
@PropertyTemplate(name="HazelcastWriter", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="url", type=String.class, required=true, defaultValue="127.0.0.1:5701"), @com.bloom.anno.PropertyTemplateProperty(name="mapName", type=String.class, required=true, defaultValue="wactions")}, outputType=StringArrayEvent.class)
public class HazelcastWriter_1_0
  extends BaseProcess
{
  private static Logger logger = Logger.getLogger(HazelcastWriter_1_0.class);
  private HazelcastInstance client = null;
  private IMap<String, Object> map;
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    String mapName = (String)properties.get("mapName");
    String url = (String)properties.get("url");
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.addAddress(new String[] { url });
    this.client = HazelcastClient.newHazelcastClient(clientConfig);
    this.map = this.client.getMap(mapName);
    if (logger.isInfoEnabled()) {
      logger.info("Successfully connected to Hazelcast server :" + url + ", map " + mapName);
    }
  }
  /*
   * 输入的事件为 bloom stream platform 标准的事件(non-Javadoc)
   * 写入的(K, V)对应的值为(EventID, Payload).
   * @see com.bloom.proc.BaseProcess#receiveImpl(int, com.bloom.event.Event)
   */
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    if ((event == null) || (event.get_wa_SimpleEvent_ID() == null)) {
      return;
    }
    this.map.put(event.get_wa_SimpleEvent_ID().getUUIDString(), event.getPayload());
  }
  
  public void close()
    throws Exception
  {
    this.client = null;
  }
}

