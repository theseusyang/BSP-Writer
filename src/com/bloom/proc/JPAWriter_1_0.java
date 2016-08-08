package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.classloading.WALoader;
import com.bloom.event.Event;
import com.bloom.event.SimpleEvent;
import com.bloom.gen.RTMappingGenerator;
import com.bloom.intf.PersistenceLayer;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.persistence.DefaultRuntimeJPAPersistenceLayerImpl;
import com.bloom.persistence.HiveJPAPersistenceLayer;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.Password;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

@PropertyTemplate(name="JPAWriter", type=AdapterType.target, properties={@com.bloom.anno.PropertyTemplateProperty(name="DriverName", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="ConnectionURL", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Username", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Password", type=Password.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Table", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="CommitTransaction", type=Boolean.class, required=false, defaultValue="true")}, outputType=SimpleEvent.class)
public class JPAWriter_1_0
  extends BaseProcess
{
  private static Logger logger = Logger.getLogger(JPAWriter_1_0.class);
  private String dbDriver = null;
  private String dbUrl = null;
  private String dbUser = null;
  private String dbPasswd = null;
  private String tableName = null;
  private MetaInfo.Type type = null;
  private String typeName = null;
  private boolean commitTransaction = true;
  private PersistenceLayer jpaPersistenceLayer;
  
  public void setType(MetaInfo.Type type)
  {
    this.type = type;
  }
  
  public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID streamUUID, String did)
    throws Exception
  {
    super.init(properties, properties2, streamUUID, did);
    
    getTypeInfo(streamUUID);
    try
    {
      if ((properties.containsKey("DriverName")) && (properties.get("DriverName") != null) && (((String)properties.get("DriverName")).length() > 0))
      {
        this.dbDriver = ((String)properties.get("DriverName"));
      }
      else
      {
        logger.error("DriverName is not specified");
        throw new Exception("DriverName is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid DriverName format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid DriverName format.Value specified cannot be cast to java.lang.String");
    }
    try
    {
      if ((properties.containsKey("ConnectionURL")) && (properties.get("ConnectionURL") != null) && (((String)properties.get("ConnectionURL")).length() > 0))
      {
        this.dbUrl = ((String)properties.get("ConnectionURL"));
      }
      else
      {
        logger.error("ConnectionURL is not specified");
        throw new Exception("ConnectionURL is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid ConnectionURL format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid ConnectionURL format.Value specified cannot be cast to java.lang.String");
    }
    try
    {
      if ((properties.containsKey("Username")) && (properties.get("Username") != null) && (((String)properties.get("Username")).length() > 0))
      {
        this.dbUser = ((String)properties.get("Username"));
      }
      else
      {
        logger.error("Username is not specified");
        throw new Exception("Username is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid Username format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid Username format.Value specified cannot be cast to java.lang.String");
    }
    try
    {
      if ((properties.containsKey("Password")) && (properties.get("Password") != null) && (((Password)properties.get("Password")).getPlain().length() > 0))
      {
        this.dbPasswd = ((Password)properties.get("Password")).getPlain();
      }
      else
      {
        logger.error("Password is not specified");
        throw new Exception("Password is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid Password format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid Password format.Value specified cannot be cast to java.lang.String");
    }
    try
    {
      if ((properties.containsKey("Table")) && (properties.get("Table") != null) && (((String)properties.get("Table")).length() > 0))
      {
        this.tableName = ((String)properties.get("Table"));
      }
      else
      {
        logger.error("Table is not specified");
        throw new Exception("Table is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid Table format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid Table format.Value specified cannot be cast to java.lang.String");
    }
    String puname = "";
    if (this.type == null)
    {
      logger.error("Error Type not found!!!");
      throw new IllegalArgumentException("Enter proper type name in format 'namespace.typeName' ");
    }
    puname = this.type.name + "_" + this.type.className;
    if ((properties.containsKey("CommitTransaction")) && (properties.get("CommitTransaction") != null))
    {
      Object val = properties.get("CommitTransaction");
      if ((val instanceof Boolean))
      {
        this.commitTransaction = ((Boolean)val).booleanValue();
      }
      else if ((val instanceof String))
      {
        if ((((String)val).equalsIgnoreCase("true")) || (((String)val).equalsIgnoreCase("yes")))
        {
          this.commitTransaction = true;
        }
        else if (((String)val).equalsIgnoreCase("false"))
        {
          this.commitTransaction = false;
        }
        else
        {
          logger.error("Invalid CommitTransaction flag format.Value specified cannot be cast to java.lang.Boolean");
          
          throw new Exception("Invalid CommitTransaction flag format.Value specified cannot be cast to java.lang.Boolean");
        }
      }
      else
      {
        logger.error("Invalid CommitTransaction flag format.Value specified cannot be cast to java.lang.Boolean");
        
        throw new Exception("Invalid CommitTransaction flag format.Value specified cannot be cast to java.lang.Boolean");
      }
    }
    else
    {
      this.commitTransaction = true;
    }
    Map<String, Object> props = new HashMap();
    props.put("javax.persistence.jdbc.driver", this.dbDriver);
    props.put("javax.persistence.jdbc.url", this.dbUrl);
    props.put("javax.persistence.jdbc.user", this.dbUser);
    props.put("javax.persistence.jdbc.password", this.dbPasswd);
    props.put("eclipselink.classloader", WALoader.getDelegate());
    props.put("eclipselink.logging.level", "SEVERE");
    
    String xml = RTMappingGenerator.createOrmMappingforClassRDBMS(this.tableName, this.type);
    RTMappingGenerator.addMappings(puname, xml);
    if (this.commitTransaction) {
      this.jpaPersistenceLayer = new DefaultRuntimeJPAPersistenceLayerImpl(puname, props);
    } else {
      this.jpaPersistenceLayer = new HiveJPAPersistenceLayer(puname, props);
    }
    this.jpaPersistenceLayer.init();
  }
  
  private void getTypeInfo(UUID streamUUID)
    throws Exception
  {
    MetaInfo.Stream streamInfo = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByUUID(streamUUID, WASecurityManager.TOKEN);
    UUID typeUUID = streamInfo.dataType;
    
    this.type = ((MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(typeUUID, WASecurityManager.TOKEN));
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    try
    {
      if (this.jpaPersistenceLayer == null)
      {
        logger.error("receiveImpl() called but jpaPersistenceLayer not initialized");
        return;
      }
      SimpleEvent receivedEvent = (SimpleEvent)event;
      synchronized (this.jpaPersistenceLayer)
      {
        this.jpaPersistenceLayer.persist(receivedEvent);
      }
    }
    catch (Exception ex)
    {
      logger.error("Error writing to db with url = " + this.dbUrl, ex);
      throw ex;
    }
  }
  
  public void close()
    throws Exception
  {
    this.jpaPersistenceLayer.close();
  }
}

