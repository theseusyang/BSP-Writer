package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.StringArrayEvent;
import com.bloom.uuid.UUID;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementService;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
/*
 * 此类负责将事件数据实时地写入到Geode集群中的节点上
 * 
 */
@PropertyTemplate(name="GeodeWriter", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="url", type=String.class, required=true, defaultValue="127.0.0.1:10334"), @com.bloom.anno.PropertyTemplateProperty(name="regionName", type=String.class, required=true, defaultValue="wactions")}, outputType=StringArrayEvent.class)
public class GeodeWriter_1_0
  extends BaseProcess
{
  private static Logger logger = Logger.getLogger(GeodeWriter_1_0.class);
  private Region<String, Object> region;
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    String regionName = (String)properties.get("regionName");
    String url = (String)properties.get("url");
    
    // 获得Geode Server 的地址和所使用的Region
    Properties pr = new Properties();
	pr.put("jmx-manager", "true");
	pr.put("jmx-manager-start", "true");
	DistributedSystem ds = DistributedSystem.connect(pr);
	Cache cache = new CacheFactory().setPdxReadSerialized(false).create();
	GemFireCacheImpl impl = (GemFireCacheImpl)cache;
	RegionFactory factory = impl.createRegionFactory(RegionShortcut.PARTITION);
	
	this.region = factory.create(regionName);
    if (logger.isInfoEnabled()) {
      logger.info("Successfully connected to Geode server :" + url + ", region " + regionName);
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
    this.region.put(event.get_wa_SimpleEvent_ID().getUUIDString(), event.getPayload());
  }
  
  public void close()
    throws Exception
  {
	  GemFireCacheImpl.getExisting().close();	
  }
}


