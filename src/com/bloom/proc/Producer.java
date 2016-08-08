package com.bloom.proc;

import com.bloom.recovery.Position;
import com.bloom.runtime.components.ReceiptCallback;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

public abstract class Producer
{
  protected static KafkaProducer<byte[], byte[]> kafkaProducer;
  protected ReceiptCallback receiptCallback;
  private Exception ex = null;
  
  public static Producer initProducer(Properties props)
  {
    if (props.containsKey("producer.type"))
    {
      String mode = props.getProperty("producer.type");
      if (mode.equalsIgnoreCase("async"))
      {
        props.remove("producer.type");
        kafkaProducer = new KafkaProducer(props);
        return new AsyncProducer();
      }
    }
    props.remove("producer.type");
    kafkaProducer = new KafkaProducer(props);
    return new SyncProducer(props);
  }
  
  public void close()
  {
    if (kafkaProducer != null)
    {
      kafkaProducer.close();
      kafkaProducer = null;
    }
  }
  
  public List<PartitionInfo> partitionsFor(String topic)
  {
    return kafkaProducer.partitionsFor(topic);
  }
  
  public void setReceiptCallBack(ReceiptCallback rc)
  {
    this.receiptCallback = rc;
  }
  
  public Exception getexception()
  {
    return this.ex;
  }
  
  protected void setException(Exception e)
  {
    if (this.ex == null) {
      this.ex = e;
    }
  }
  
  public abstract void send(ProducerRecord<byte[], byte[]> paramProducerRecord, Position paramPosition)
    throws Exception;
}

