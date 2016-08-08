package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.exc.ConnectionException;
import com.bloom.common.exc.MetadataUnavailableException;
import com.bloom.common.exc.SystemException;
import com.bloom.event.Event;
import com.bloom.intf.Formatter;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.Acknowledgeable;
import com.bloom.recovery.Position;
import com.bloom.source.kafka.KafkaProperty;
import com.bloom.source.kafka.partition.KafkaPartitioner;
import com.bloom.source.kafka.utils.KafkaUtils;
import com.bloom.source.lib.prop.Property;
import com.bloom.uuid.UUID;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

@PropertyTemplate(name="KafkaWriter", type=AdapterType.target, properties={@com.bloom.anno.PropertyTemplateProperty(name="brokerAddress", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Topic", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="KafkaConfig", type=String.class, required=false, defaultValue="")}, outputType=WAEvent.class, requiresFormatter=true)
public class KafkaWriter_1_0
  extends BaseWriter
  implements Acknowledgeable
{
  public static final String PARTITIONERCLASS = "com.bloom.source.kafka.partition.ExplicitPartitioner";
  private static Logger logger = Logger.getLogger(KafkaWriter_1_0.class);
  private String topic = null;
  private KafkaProperty kafkaProp = null;
  private int[] keyFieldIndex;
  private String charset;
  private HashSet<String> replicaList;
  private long retryBackoffms = 1000L;
  private int maxRequestSize = 1048576;
  private Producer producer;
  private KafkaPartitioner partitioner = new KafkaPartitioner();
  
  public void init(Map<String, Object> writerProperties, Map<String, Object> formatterProperties, UUID inputStream, String distributionID)
    throws Exception
  {
    super.init(writerProperties, formatterProperties, inputStream, distributionID);
    
    Map<String, Object> localPropertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    localPropertyMap.putAll(writerProperties);
    localPropertyMap.putAll(formatterProperties);
    
    this.kafkaProp = new KafkaProperty(localPropertyMap);
    this.topic = this.kafkaProp.topic;
    if (localPropertyMap.containsKey("partitionFieldIndex")) {
      this.keyFieldIndex = ((int[])localPropertyMap.get("partitionFieldIndex"));
    }
    this.charset = this.kafkaProp.getString(Property.CHARSET, Charset.defaultCharset().name());
    open();
  }
  
  public void open()
    throws Exception
  {
    Properties props = new Properties();
    props.put("acks", "1");
    props.put("retry.backoff.ms", "5000");
    props.put("reconnect.backoff.ms", "5000");
    props.put("metadata.fetch.timeout.ms", "10000");
    props.put("metadata.max.age.ms", "11000");
    String[] kafkaConfig = this.kafkaProp.getKafkaBrokerConfigList();
    if (kafkaConfig != null) {
      for (int i = 0; i < kafkaConfig.length; i++)
      {
        String[] property = kafkaConfig[i].split("=");
        if ((property == null) || (property.length < 2))
        {
          logger.warn("Kafka Property \"" + property[0] + "\" is invalid.");
          logger.warn("Invalid \"KafkaProducerConfig\" property structure " + property[0] + ". Expected structure <name>=<value>;<name>=<value>");
        }
        else if (property[0].equals("key.serializer.class"))
        {
          logger.warn("User defined Serializer is not supported.");
        }
        else
        {
          props.put(property[0], property[1]);
          if (property[0].equalsIgnoreCase("retry.backoff.ms")) {
            this.retryBackoffms = Long.parseLong(property[1]);
          }
          if ((property[0].equals("max.request.size")) && 
            (Integer.parseInt(property[1]) > this.maxRequestSize)) {
            this.maxRequestSize = Integer.parseInt(property[1]);
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Kafka Producer property \"" + property[0] + "\" is set with value \"" + property[1] + "\"");
          }
        }
      }
    }
    TopicMetadata topicMd = null;
    for (int i = 0; i < this.kafkaProp.kafkaBrokerAddress.length; i++) {
      try
      {
        String[] ipAndPort = this.kafkaProp.kafkaBrokerAddress[i].split(":");
        String brokerIpAddres = ipAndPort[0];
        int brokerPortNo = Integer.parseInt(ipAndPort[1]);
        
        topicMd = KafkaUtils.lookupTopicMetadata(brokerIpAddres, brokerPortNo, this.topic, this.retryBackoffms);
        if (topicMd != null) {
          break;
        }
      }
      catch (Exception e)
      {
        logger.error(e);
        if (i == this.kafkaProp.kafkaBrokerAddress.length - 1) {
          throw new MetadataUnavailableException("Failure in getting metadata for Kafka Topic. " + this.topic + "." + e);
        }
      }
    }
    this.replicaList = new HashSet();
    
    String bootstrapServerList = "";
    for (int i = 0; i < this.kafkaProp.kafkaBrokerAddress.length; i++)
    {
      this.replicaList.add(this.kafkaProp.kafkaBrokerAddress[i]);
      bootstrapServerList = bootstrapServerList + this.kafkaProp.kafkaBrokerAddress[i] + ",";
    }
    for (PartitionMetadata partitionMd : topicMd.partitionsMetadata())
    {
    	  String address;
      address = partitionMd.leader().host() + ":" + partitionMd.leader().port();
      if (!this.replicaList.contains(address))
      {
        this.replicaList.add(address);
        bootstrapServerList = bootstrapServerList + address + ",";
      }
      for (Broker broker : partitionMd.replicas())
      {
        address = broker.host() + ":" + broker.port();
        if (!this.replicaList.contains(address))
        {
          this.replicaList.add(address);
          bootstrapServerList = bootstrapServerList + address + ",";
        }
      }
    }
    
    if (bootstrapServerList.endsWith(",")) {
      bootstrapServerList = bootstrapServerList.substring(0, bootstrapServerList.lastIndexOf(","));
    }
    props.put("bootstrap.servers", bootstrapServerList);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    
    this.producer = Producer.initProducer(props);
    this.producer.setReceiptCallBack(this.receiptCallback);
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    if (this.running)
    {
      if (this.producer.getexception() != null) {
        throw new SystemException("Problem while send data to kafka " + this.topic, this.producer.getexception());
      }
      write(event, null);
    }
  }
  
  public void receive(int channel, Event event, Position pos)
    throws Exception
  {
    if (this.running)
    {
      if (this.producer.getexception() != null) {
        throw new SystemException("Problem while send data to kafka " + this.topic, this.producer.getexception());
      }
      write(event, pos);
    }
  }
  
  public void close()
    throws Exception
  {
    if (this.producer != null) {
      this.producer.close();
    }
  }
  
  private void write(Event event, Position thisPos)
    throws Exception
  {
    byte[] waEventBytes = null;
    int partition = 0;
    ProducerRecord<byte[], byte[]> kafkaMessage = null;
    try
    {
      List<Object> partitionList;
      if ((this.keyFieldIndex != null) && (this.keyFieldIndex.length > 0))
      {
         partitionList = new ArrayList(this.keyFieldIndex.length);
        for (int i = 0; i < this.keyFieldIndex.length; i++) {
          partitionList.add(this.fields[this.keyFieldIndex[i]].get(event));
        }
      }
      else
      {
        partitionList = null;
      }
      partition = this.partitioner.partition(this.producer.partitionsFor(this.topic), partitionList);
      
      waEventBytes = this.formatter.format(event);
      if (waEventBytes != null)
      {
        byte[] headerBytes = this.formatter.addHeader();
        byte[] footerBytes = this.formatter.addFooter();
        byte[] kafkaMessageBytes;
        if ((headerBytes != null) && (footerBytes != null))
        {
          kafkaMessageBytes = new byte[headerBytes.length + waEventBytes.length + footerBytes.length];
          System.arraycopy(headerBytes, 0, kafkaMessageBytes, 0, headerBytes.length);
          System.arraycopy(waEventBytes, 0, kafkaMessageBytes, headerBytes.length, waEventBytes.length);
          System.arraycopy(footerBytes, 0, kafkaMessageBytes, headerBytes.length + waEventBytes.length, footerBytes.length);
        }
        else
        {
          kafkaMessageBytes = waEventBytes;
        }
        kafkaMessage = new ProducerRecord(this.topic, Integer.valueOf(partition), null, kafkaMessageBytes);
        if (thisPos != null) {}
        this.producer.send(kafkaMessage, thisPos);
      }
    }
    catch (Exception e)
    {
      if (this.running)
      {
        logger.error("Problem while writing the event ", e);
        throw new ConnectionException("KafkaWriter is unable to produce events to " + this.topic, e);
      }
    }
  }
  
  public static void main(String[] args)
  {
    HashMap<String, String> wp = new HashMap();
    wp.put("brokerAddress", "localhost:9091,localhost:9092");
    wp.put("Topic", "csvtest");
    wp.put("PartitionIDList", "0");
  }
}
