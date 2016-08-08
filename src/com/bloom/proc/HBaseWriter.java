package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.classloading.WALoader;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.ConnectionException;
import com.bloom.common.exc.SystemException;
import com.bloom.event.Event;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.Acknowledgeable;
import com.bloom.recovery.Position;
import com.bloom.runtime.BuiltInFunc;
import com.bloom.runtime.components.ReceiptCallback;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

@PropertyTemplate(name="HBaseWriter", type=AdapterType.target, properties={@com.bloom.anno.PropertyTemplateProperty(name="Tables", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="FamilyNames", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="HBaseConfigurationPath", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="authenticationPolicy", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="BatchPolicy", type=String.class, required=false, defaultValue="eventCount:1000, Interval:30")}, outputType=WAEvent.class, requiresFormatter=false)
public class HBaseWriter
  extends BaseProcess
  implements Acknowledgeable
{
  private static Logger logger = Logger.getLogger(HBaseWriter.class);
  protected MetaInfo.Stream streamInfo = null;
  protected MetaInfo.Type typeInfo = null;
  protected Configuration config = null;
  protected String tableName = null;
  protected ArrayList typeFields = null;
  protected String familyName = null;
  protected String columnName = null;
  protected HBaseAdmin admin = null;
  protected HTable hTable = null;
  protected Put putToHBase = null;
  protected Connection conn = null;
  protected MDRepository mdr = null;
  protected String valueToInput = null;
  protected String connectionURL = null;
  protected String zookeeperIP = null;
  protected String[] IPandPort = null;
  protected String siteXmlPath = null;
  protected String IP = null;
  protected String port = null;
  protected List keys = null;
  protected Map typeMap = null;
  protected String batchPolicy = null;
  protected int batchSize = 1;
  protected List<Put> listOfPuts = new ArrayList();
  protected int batchCounter = 0;
  protected int connectionTimeout = 30;
  protected int batchInterval = 30000;
  protected boolean isBatchSize = false;
  protected boolean typedEvent = false;
  HTableDescriptor tableDescriptor = null;
  protected boolean isBatchInterval = false;
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock writeLock = this.readWriteLock.writeLock();
  protected String authenticationPolicy = null;
  private String authenticationName;
  private String principal;
  private String keytabPath;
  private boolean kerberosEnabled = false;
  private boolean acknowledgePosition = false;
  private Position ackCurrentPosition;
  private final String TABLE_NAME = "TableName";
  private final String OPERATION_NAME = "OperationName";
  private final String OPERATION_VALUE = "INSERT";
  private UUID typeUUID = null;
  private HashMap<String, Object> metadata = null;
  Set columnNames = null;
  private Timer batchTimer;
  private final Object lockObject = new Object();
  protected HashMap<UUID, Field[]> typeUUIDCache = new HashMap();
  
  public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID streamUUID, String did)
    throws Exception
  {
    super.init(properties, properties2, streamUUID, did);
    readProps(properties);
    TypeInfo(streamUUID);
    makeHBaseConnection();
    if (!this.admin.tableExists(this.tableName)) {
      throw new SystemException("Table \"" + this.tableName + "\"" + " does not exist in HBase");
    }
    if (this.isBatchInterval) {
      initializeBatchTimer();
    }
    if (this.receiptCallback != null) {
      this.acknowledgePosition = true;
    }
  }
  
  public void receive(int channel, Event event, Position pos)
    throws Exception
  {
    synchronized (this.lockObject)
    {
      this.ackCurrentPosition = pos;
      receiveImpl(channel, event);
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    if (this.typedEvent) {
      event = TypedeventToWAevent(event);
    }
    WAEvent waevent = (WAEvent)event;
    if (!waevent.metadata.containsKey("OperationName")) {
      throw new AdapterException("Invalid WAEvent format. HBaseWriter supports WAEvent originating from CDC and database sources only. For other sources, please send the typed event stream.");
    }
    String OpName = waevent.metadata.get("OperationName").toString();
    
    String isPKupdate = (String)waevent.metadata.get("PK_UPDATE");
    if ((isPKupdate != null) && 
      (isPKupdate.equalsIgnoreCase("true")))
    {
      logger.warn("PK_UPDATE found, ignoring event");
      return;
    }
    if ((OpName.equalsIgnoreCase("INSERT")) || (OpName.equalsIgnoreCase("UPDATE"))) {
      processInsert(waevent);
    } else if (OpName.equalsIgnoreCase("DELETE")) {
      processDelete(waevent);
    } else if (OpName.equalsIgnoreCase("TRUNCATE")) {
      processTruncate();
    }
  }
  
  private void processTruncate()
    throws IOException, AdapterException
  {
    try
    {
      if ((this.listOfPuts.size() > 0) && 
        (this.listOfPuts.size() > 0)) {
        flush();
      }
      this.admin.disableTable(Bytes.toBytes(this.tableName));
      this.admin.deleteTable(Bytes.toBytes(this.tableName));
      this.admin.createTable(this.tableDescriptor);
      if (this.acknowledgePosition) {
        this.receiptCallback.ack(this.ackCurrentPosition);
      }
    }
    catch (Exception x)
    {
      logger.warn("Failure to truncate Hbase table", x);
      throw new AdapterException("Failure to truncate Hbase table", x);
    }
  }
  
  private void processInsert(WAEvent waevent)
    throws IOException, MetaDataRepositoryException, AdapterException
  {
    try
    {
      this.typeMap = getColumnsAndDataAsMap(waevent, waevent.data);
      this.columnNames = this.typeMap.keySet();
      if ((this.keys != null) && (this.keys.size() > 0)) {
        this.putToHBase = new Put(Bytes.toBytes(String.valueOf(this.typeMap.get(this.keys.get(0)))));
      } else {
        this.putToHBase = new Put(Bytes.toBytes(String.valueOf(this.typeMap.get("0"))));
      }
      if (waevent.data != null) {
        batchProcessing();
      }
    }
    catch (Exception e)
    {
      logger.warn("Failure to write to HBase", e);
      throw new AdapterException("Failure to write to HBase", e);
    }
  }
  
  public void processDelete(WAEvent waevent)
    throws AdapterException, MetaDataRepositoryException, IOException
  {
    try
    {
      if (this.listOfPuts.size() > 0) {
        flush();
      }
      this.typeMap = getColumnsAndDataAsMap(waevent, waevent.data);
      String keyToDelete = new String();
      if ((this.keys != null) && (this.keys.size() > 0)) {
        keyToDelete = String.valueOf(this.typeMap.get(this.keys.get(0)));
      } else {
        keyToDelete = String.valueOf(this.typeMap.get("0"));
      }
      Delete d = new Delete(Bytes.toBytes(keyToDelete));
      
      this.hTable.delete(d);
      if (this.acknowledgePosition)
      {
        this.receiptCallback.ack(this.ackCurrentPosition);
        if (logger.isDebugEnabled()) {
          logger.debug("delete time ack" + this.ackCurrentPosition);
        }
      }
    }
    catch (Exception x)
    {
      logger.warn("Failure to delete row in HBase", x);
      throw new AdapterException("Failure to delete row in HBase", x);
    }
  }
  
  public void batchProcessing()
    throws IOException, AdapterException
  {
    for (Object s : this.columnNames)
    {
      this.valueToInput = String.valueOf(this.typeMap.get(s));
      this.columnName = String.valueOf(s);
      
      this.putToHBase = this.putToHBase.addColumn(Bytes.toBytes(this.familyName), Bytes.toBytes(this.columnName), Bytes.toBytes(this.valueToInput));
    }
    this.listOfPuts.add(this.putToHBase);
    if (this.isBatchSize) {
      this.batchCounter += 1;
    }
    if ((this.batchCounter == this.batchSize) && (!this.listOfPuts.isEmpty()))
    {
      flushAndAck();
      if (logger.isDebugEnabled()) {
        logger.debug("even count ack" + this.ackCurrentPosition);
      }
      if (this.isBatchInterval) {
        resetBatchIntervalTimer();
      }
      if (logger.isDebugEnabled()) {
        logger.debug("HBase Put executed on table " + this.tableName);
      }
    }
  }
  
  public void flush()
    throws IOException, AdapterException
  {
    try
    {
      this.writeLock.lock();
      if (!this.hTable.isAutoFlush()) {
        logger.warn("Autoflush needed to be turned on...");
      }
      this.hTable.put(this.listOfPuts);
      
      logger.info("Placed " + this.listOfPuts.size() + " values into Hbase table");
      this.listOfPuts = new ArrayList();
      this.batchCounter = 0;
    }
    catch (Exception e)
    {
      throw new AdapterException("Unable to flush values into HBase Table", e);
    }
    finally
    {
      this.writeLock.unlock();
    }
  }
  
  public void flushAndAck()
    throws IOException, AdapterException
  {
    flush();
    if (this.acknowledgePosition) {
      this.receiptCallback.ack(this.ackCurrentPosition);
    }
  }
  
  public WAEvent TypedeventToWAevent(Event event)
  {
    WAEvent waEvent = null;
    Object[] payload = event.getPayload();
    if (payload != null)
    {
      int payloadLength = payload.length;
      waEvent = new WAEvent(payloadLength, null);
      waEvent.metadata = this.metadata;
      waEvent.typeUUID = this.typeUUID;
      
      waEvent.data = new Object[payloadLength];
      
      int i = 0;
      for (Object o : payload) {
        waEvent.setData(i++, o);
      }
    }
    return waEvent;
  }
  
  public HashMap<String, Object> getColumnsAndDataAsMap(WAEvent event, Object[] dataOrBeforeArray)
    throws AdapterException, MetaDataRepositoryException
  {
    Field[] fieldsOfThisTable = null;
    if (event.typeUUID != null) {
      if (this.typeUUIDCache.containsKey(event.typeUUID)) {
        fieldsOfThisTable = (Field[])this.typeUUIDCache.get(event.typeUUID);
      } else {
        try
        {
          MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(event.typeUUID, WASecurityManager.TOKEN);
          
          this.keys = dataType.keyFields;
          if (this.keys.isEmpty()) {
            throw new AdapterException("Failure in retrieving the Key field name for Table :" + dataType.getName() + ". " + "At least one field should be assigned as primary key");
          }
          Class<?> typeClass = WALoader.get().loadClass(dataType.className);
          fieldsOfThisTable = typeClass.getDeclaredFields();
          this.typeUUIDCache.put(event.typeUUID, fieldsOfThisTable);
        }
        catch (Exception e)
        {
          logger.warn("Unable to fetch the type for table " + event.metadata.get("TableName") + e);
          throw new AdapterException("Unable to fetch the type for table " + event.metadata.get("TableName"), e);
        }
      }
    }
    HashMap<String, Object> columnsAndDataMap = new HashMap();
    boolean isPresent;
    Integer localInteger1;
    for (Integer i = Integer.valueOf(0); i.intValue() < dataOrBeforeArray.length; localInteger1 = i = Integer.valueOf(i.intValue() + 1))
    {
      isPresent = BuiltInFunc.IS_PRESENT(event, dataOrBeforeArray, i.intValue());
      if (isPresent) {
        if (fieldsOfThisTable != null) {
          columnsAndDataMap.put(fieldsOfThisTable[i.intValue()].getName(), dataOrBeforeArray[i.intValue()] != null ? dataOrBeforeArray[i.intValue()] : null);
        } else {
          columnsAndDataMap.put(i + "", dataOrBeforeArray[i.intValue()] != null ? dataOrBeforeArray[i.intValue()] : null);
        }
      }
      isPresent = i;
    }
    return columnsAndDataMap;
  }
  
  private void TypeInfo(UUID streamUUID)
    throws Exception
  {
    this.mdr = MetadataRepository.getINSTANCE();
    this.typeMap = new HashMap();
    
    this.streamInfo = ((MetaInfo.Stream)this.mdr.getMetaObjectByUUID(streamUUID, WASecurityManager.TOKEN));
    this.typeUUID = this.streamInfo.dataType;
    
    this.typeInfo = ((MetaInfo.Type)this.mdr.getMetaObjectByUUID(this.typeUUID, WASecurityManager.TOKEN));
    if (!this.typeInfo.className.equals("com.bloom.proc.events.WAEvent"))
    {
      this.typedEvent = true;
      this.metadata = new HashMap();
      this.metadata.put("TableName", this.typeInfo.name);
      this.metadata.put("OperationName", "INSERT");
    }
  }
  
  private void makeHBaseConnection()
    throws Exception
  {
    this.config = HBaseConfiguration.create();
    this.config.clear();
    if (this.connectionURL != null)
    {
      this.config.set("hbase.zookeeper.quorum", this.zookeeperIP);
      this.config.set("hbase.zookeeper.property.clientPort", this.port);
      this.config.set("hbase.master", this.IP);
    }
    else
    {
      File f = new File(this.siteXmlPath);
      if (f.isDirectory())
      {
        File[] files = f.listFiles();
        for (File file : files) {
          if (file.getName().endsWith(".xml"))
          {
            if (logger.isDebugEnabled()) {
              logger.debug("Loading File from directory: " + file.getAbsolutePath());
            }
            this.config.addResource(new Path(file.getAbsolutePath()));
          }
        }
      }
      else
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Loading file " + this.siteXmlPath);
        }
        this.config.addResource(new Path(this.siteXmlPath));
      }
    }
    this.config.set("hbase.rpc.timeout", "30000");
    connectHbase hBaseConnection = new connectHbase();
    
    hBaseConnection.connect();
  }
  
  public class connectHbase
  {
    public connectHbase() {}
    
    public void connect()
      throws ConnectionException
    {
      try
      {
        if (HBaseWriter.this.kerberosEnabled)
        {
          UserGroupInformation.setConfiguration(HBaseWriter.this.config);
          try
          {
            UserGroupInformation.loginUserFromKeytab(HBaseWriter.this.principal, HBaseWriter.this.keytabPath);
          }
          catch (IOException e)
          {
            ConnectionException se = new ConnectionException("Problem authenticating kerberos", e);
            throw se;
          }
        }
        HBaseAdmin.checkHBaseAvailable(HBaseWriter.this.config);
        Connection conn = ConnectionFactory.createConnection(HBaseWriter.this.config);
        HBaseWriter.this.admin = ((HBaseAdmin)conn.getAdmin());
        
        HBaseWriter.this.hTable = ((HTable)conn.getTable(TableName.valueOf(HBaseWriter.this.tableName)));
        HBaseWriter.this.hTable.setAutoFlush(true, true);
        HBaseWriter.this.tableDescriptor = HBaseWriter.this.hTable.getTableDescriptor();
      }
      catch (Exception e)
      {
        throw new ConnectionException("Unable to make HBase connection " + e.getMessage(), e.getCause());
      }
    }
  }
  
  public void close()
    throws Exception
  {
    logger.info("Closing HTable and HBaseAdmin");
    synchronized (this.lockObject)
    {
      if (this.listOfPuts.size() > 0)
      {
        flushAndAck();
        if (logger.isDebugEnabled()) {
          logger.debug("closing ack" + this.ackCurrentPosition);
        }
      }
      if (this.hTable != null) {
        this.hTable.close();
      }
      if (this.admin != null) {
        this.admin.close();
      }
      stopBatchTimer();
    }
  }
  
  private void readProps(Map<String, Object> properties)
    throws Exception
  {
    if ((properties.containsKey("ConnectionURL")) && (properties.get("ConnectionURL") != null))
    {
      this.connectionURL = ((String)properties.get("ConnectionURL"));
      
      this.IPandPort = this.connectionURL.split(":");
      this.IP = this.IPandPort[0];
      if (this.IPandPort[1] != null) {
        this.port = this.IPandPort[1];
      } else {
        throw new ConnectionException("HBase port not specified, please specify port");
      }
    }
    if (properties.get("ConnectionTimeout") != null) {
      this.connectionTimeout = ((Integer)properties.get("ConnectionTimeout")).intValue();
    }
    if ((properties.containsKey("ZooKeeperIPAddress")) && (properties.get("ZooKeeperIPAddress") != null))
    {
      this.zookeeperIP = ((String)properties.get("ZooKeeperIPAddress"));
      if (this.zookeeperIP.length() == 0) {
        this.zookeeperIP = this.IP;
      }
    }
    else
    {
      this.zookeeperIP = this.IP;
    }
    if ((properties.containsKey("HBaseConfigurationPath")) && (properties.get("HBaseConfigurationPath") != null))
    {
      this.siteXmlPath = ((String)properties.get("HBaseConfigurationPath"));
      if (this.siteXmlPath.length() == 0) {
        this.siteXmlPath = null;
      }
    }
    if ((properties.containsKey("Tables")) && (properties.get("Tables") != null)) {
      this.tableName = ((String)properties.get("Tables"));
    }
    if ((properties.containsKey("FamilyNames")) && (properties.get("FamilyNames") != null)) {
      this.familyName = ((String)properties.get("FamilyNames"));
    }
    if ((this.tableName.isEmpty()) || (this.familyName.isEmpty())) {
      throw new AdapterException("TableName or Family name are blank, please specify in application");
    }
    if ((properties.containsKey("authenticationPolicy")) && (properties.get("authenticationPolicy") != null) && (!properties.get("authenticationPolicy").toString().isEmpty()))
    {
      this.authenticationPolicy = ((String)properties.get("authenticationPolicy"));
      validateAuthenticationPolicy(this.authenticationPolicy);
    }
    if ((properties.containsKey("BatchPolicy")) && (properties.get("BatchPolicy") != null))
    {
      this.batchPolicy = ((String)properties.get("BatchPolicy"));
      
      String[] batchPolicies = this.batchPolicy.split(",");
      if (this.batchPolicy.length() == 0)
      {
        this.batchSize = 1;
        this.isBatchSize = true;
        return;
      }
      if (batchPolicies.length > 0) {
        for (String specifiedBatchPolicy : batchPolicies)
        {
          String[] policyProperties = specifiedBatchPolicy.split(":");
          if (policyProperties[0].trim().toLowerCase().contains("count"))
          {
            this.batchSize = Integer.parseInt(policyProperties[1].trim());
            if (this.batchSize == 0) {
              this.batchSize = 1;
            }
            if (this.batchSize < 0) {
              this.batchSize = (-this.batchSize);
            }
            this.isBatchSize = true;
          }
          else
          {
            this.batchInterval = (Integer.parseInt(policyProperties[1].trim()) * 1000);
            if (this.batchInterval < 0) {
              this.batchInterval = (-this.batchInterval);
            }
            this.isBatchInterval = true;
            if (this.batchInterval == 0) {
              this.isBatchInterval = false;
            }
          }
        }
      }
    }
  }
  
  private void validateAuthenticationPolicy(String authenticationPolicy)
    throws AdapterException
  {
    Map<String, Object> authenticationPropertiesMap = extractAuthenticationProperties(authenticationPolicy);
    if (this.authenticationName.equalsIgnoreCase("kerberos"))
    {
      this.kerberosEnabled = true;
      this.principal = ((String)authenticationPropertiesMap.get("principal"));
      this.keytabPath = ((String)authenticationPropertiesMap.get("keytabpath"));
      if ((this.principal == null) && (this.principal.trim().isEmpty()) && (this.keytabPath == null) && (this.keytabPath.trim().isEmpty())) {
        throw new AdapterException("Principal or Keytab path required for kerberos authentication cannot be empty or null");
      }
    }
    else
    {
      throw new AdapterException("Specified authentication " + this.authenticationName + " is not supported");
    }
  }
  
  private Map<String, Object> extractAuthenticationProperties(String authenticationPolicyName)
  {
    Map<String, Object> authenticationPropertiesMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    String[] extractedValues = authenticationPolicyName.split(",");
    boolean isAuthenticationPolicyNameExtracted = false;
    for (String value : extractedValues)
    {
      String[] properties = value.split(":");
      if (properties.length > 1)
      {
        authenticationPropertiesMap.put(properties[0], properties[1]);
      }
      else if (!isAuthenticationPolicyNameExtracted)
      {
        this.authenticationName = properties[0];
        isAuthenticationPolicyNameExtracted = true;
      }
    }
    return authenticationPropertiesMap;
  }
  
  public int getBatchSize()
  {
    return this.batchSize;
  }
  
  public int getBatchInterval()
  {
    return this.batchInterval;
  }
  
  public String getSiteXmlPath()
  {
    return this.siteXmlPath;
  }
  
  public String getAuthenticationPolicy()
  {
    return this.authenticationPolicy;
  }
  
  public String getAuthenticationName()
  {
    return this.authenticationName;
  }
  
  public String getPrincipal()
  {
    return this.principal;
  }
  
  public String getKeytabPath()
  {
    return this.keytabPath;
  }
  
  public boolean isKerberosEnabled()
  {
    return this.kerberosEnabled;
  }
  
  public class BatchTask
    extends TimerTask
  {
    private Logger logger = Logger.getLogger(BatchTask.class);
    
    public BatchTask() {}
    
    public void run()
    {
      try
      {
        synchronized (HBaseWriter.this.lockObject)
        {
          if (HBaseWriter.this.listOfPuts.size() > 0)
          {
            HBaseWriter.this.flushAndAck();
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("timer task ack" + HBaseWriter.this.ackCurrentPosition);
            }
          }
        }
      }
      catch (Exception e)
      {
        this.logger.error("Could not flush rows into HBase table");
      }
    }
  }
  
  public void initializeBatchTimer()
  {
    this.batchTimer = new Timer("HbaseBatchTimer");
    this.batchTimer.schedule(new BatchTask(), this.batchInterval, this.batchInterval);
  }
  
  public void stopBatchTimer()
  {
    if (this.batchTimer != null)
    {
      this.batchTimer.cancel();
      this.batchTimer = null;
    }
  }
  
  public void resetBatchIntervalTimer()
  {
    stopBatchTimer();
    initializeBatchTimer();
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping and initializing timer");
    }
  }
}

