package com.bloom.proc;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.classloading.WALoader;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.Event;
import com.bloom.event.SimpleEvent;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.BuiltInFunc;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.Password;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;
import org.bson.Document;

@PropertyTemplate(name="MongoWriter", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="DatabaseName", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Collections", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Username", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Password", type=Password.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="ConnectionString", type=String.class, required=false, defaultValue="localhost:27017")}, outputType=SimpleEvent.class)
public class MongoDBWriter_1_0
  extends BaseProcess
{
  private static Logger logger = Logger.getLogger(MongoDBWriter_1_0.class);
  private String dbName = null;
  private String collectionName = null;
  private String dbUser = null;
  private String dbPasswd = null;
  private String host = "localhost";
  private int port = 27017;
  public static String USERNAME = "Username";
  public static String PASSWORD = "Password";
  public static String COLLECTION_NAME = "Collections";
  public static String DATABASE_NAME = "DatabaseName";
  public static String CONNECTION = "ConnectionString";
  boolean isWAEvent = false;
  private MetaInfo.Type dataType = null;
  private MongoClient client;
  private MongoDatabase mongoDB;
  private MongoCollection<Document> mongoCollection;
  
  public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID, String distributionID)
    throws Exception
  {
    super.init(properties, properties2, sourceUUID, distributionID);
    try
    {
      MetaInfo.Stream stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByUUID(sourceUUID, WASecurityManager.TOKEN);
      this.dataType = ((MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(stream.dataType, WASecurityManager.TOKEN));
      
      Class<?> typeClass = WALoader.get().loadClass(this.dataType.className);
      if (typeClass.getSimpleName().equals("WAEvent")) {
        this.isWAEvent = true;
      }
      validateProperties(properties);
    }
    catch (Exception ex)
    {
      throw new AdapterException("Error Initializing MongoDBWriter: " + ex.getMessage());
    }
  }
  
  private void validateProperties(Map<String, Object> properties)
    throws Exception
  {
    if ((properties.get(DATABASE_NAME) != null) && (!properties.get(DATABASE_NAME).toString().trim().isEmpty())) {
      this.dbName = properties.get(DATABASE_NAME).toString().trim();
    } else {
      throw new IllegalArgumentException("Database name must be provided");
    }
    if ((properties.get(COLLECTION_NAME) != null) && (!properties.get(COLLECTION_NAME).toString().trim().isEmpty())) {
      this.collectionName = properties.get(COLLECTION_NAME).toString().trim();
    } else {
      throw new IllegalArgumentException("Collection name must be provided");
    }
    if ((properties.containsKey(CONNECTION)) && (properties.get(CONNECTION) != null))
    {
      String val = properties.get(CONNECTION).toString();
      
      String[] split = val.split(":");
      if ((!val.contains(":")) || (split[0].trim().equals("")) || (split[1].trim().equals(""))) {
        throw new IllegalArgumentException("Connection string specified " + val + " is invalid. It must be specified as host:port");
      }
      try
      {
        InetAddress.getByName(split[0].trim());
      }
      catch (UnknownHostException ex)
      {
        throw new UnknownHostException("Connection String invalid at host :" + split[0]);
      }
      this.host = split[0].trim();
      try
      {
        this.port = Integer.parseInt(split[1].trim());
      }
      catch (NumberFormatException ex)
      {
        throw new NumberFormatException("Connection String invalid at port :" + split[1]);
      }
    }
    if ((properties.containsKey(USERNAME)) && 
      (properties.get(USERNAME) != null)) {
      this.dbUser = properties.get(USERNAME).toString();
    }
    if ((properties.containsKey(PASSWORD)) && 
      (properties.get(PASSWORD) != null)) {
      this.dbPasswd = ((Password)properties.get(PASSWORD)).getPlain().toString();
    }
    open();
  }
  
  public void open()
    throws Exception
  {
    if (this.dbUser != null)
    {
      MongoCredential credential = MongoCredential.createCredential(this.dbUser, this.dbName.trim(), this.dbPasswd.toCharArray());
      this.client = new MongoClient(new ServerAddress(this.host, this.port), Arrays.asList(new MongoCredential[] { credential }));
    }
    else
    {
      this.client = new MongoClient(this.host, this.port);
    }
    boolean dbFound = false;
    MongoCursor<String> listDatabaseNames = this.client.listDatabaseNames().iterator();
    while (listDatabaseNames.hasNext())
    {
      String val = (String)listDatabaseNames.next();
      if (val.equals(this.dbName.trim()))
      {
        dbFound = true;
        break;
      }
    }
    if (dbFound) {
      this.mongoDB = this.client.getDatabase(this.dbName.trim());
    } else {
      throw new AdapterException("Failure in retrieving " + this.dbName + " database. Please specify valid DatabaseName");
    }
    if (logger.isInfoEnabled()) {
      logger.info("Successfully connected to " + this.dbName + " database");
    }
    MongoCursor<String> listCollectionNames = this.mongoDB.listCollectionNames().iterator();
    boolean collectionFound = false;
    while (listCollectionNames.hasNext())
    {
      String val = (String)listCollectionNames.next();
      if (val.equals(this.collectionName.trim()))
      {
        collectionFound = true;
        break;
      }
    }
    if (collectionFound) {
      this.mongoCollection = this.mongoDB.getCollection(this.collectionName.trim());
    } else {
      throw new AdapterException("Failure in retrieving " + this.collectionName + ". Please specify valid CollectionName");
    }
    if (logger.isInfoEnabled()) {
      logger.info("Collection " + this.collectionName + " retrieved successfully ");
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    try
    {
      if (this.isWAEvent)
      {
        Document document = new Document();
        
        WAEvent we = (WAEvent)event;
        if (we.typeUUID == null)
        {
          Object[] data = we.data;
          for (int i = 0; i < data.length; i++) {
            document.append(i + "", data[i]);
          }
          writeToDatabase(document);
          return;
        }
        handleDataEvent(BuiltInFunc.DATA(we));
        return;
      }
      Map<String, Object> data = getSimpleData((SimpleEvent)event);
      handleDataEvent(data);
    }
    catch (Exception ex)
    {
      throw new AdapterException("Error writing to db " + this.dbName + " with collectionName " + this.collectionName + ": " + ex.getMessage());
    }
  }
  
  public void writeToDatabase(Document document)
  {
    synchronized (this.mongoCollection)
    {
      this.mongoCollection.insertOne(document);
    }
  }
  
  private Map<String, Object> getSimpleData(SimpleEvent e)
  {
    Map<String, String> keys = this.dataType.fields;
    Map<String, Object> result = new HashMap();
    Object[] data = e.getPayload();
    Iterator<String> iter = keys.keySet().iterator();
    int i = 0;
    while (iter.hasNext())
    {
      String key = (String)iter.next();
      result.put(key, data[i]);
      i++;
    }
    return result;
  }
  
  private void handleDataEvent(Map<String, Object> event)
    throws Exception
  {
    Document document = new Document();
    
    Set<Map.Entry<String, Object>> set = event.entrySet();
    Iterator<Map.Entry<String, Object>> iter = set.iterator();
    while (iter.hasNext())
    {
      Map.Entry<String, Object> val = (Map.Entry)iter.next();
      document.append((String)val.getKey(), val.getValue());
    }
    writeToDatabase(document);
  }
  
  public void close()
    throws Exception
  {
    this.client.close();
  }
  
  public static void main(String[] args)
    throws Exception
  {
    HashMap<String, Object> writerProperties = new HashMap();
    writerProperties.put(DATABASE_NAME, "my_local");
    writerProperties.put(COLLECTION_NAME, "mycollection1");
    try
    {
      MongoDBWriter_1_0 dbWriter = new MongoDBWriter_1_0();
      dbWriter.validateProperties(writerProperties);
      System.out.println(dbWriter.host + ":" + dbWriter.port);
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }
  }
}

