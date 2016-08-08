package com.bloom.proc;

import com.bloom.recovery.Position;
import com.bloom.runtime.components.ReceiptCallback;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

public class SyncProducer
  extends Producer
{
  private static Logger logger = Logger.getLogger(SyncProducer.class);
  private long RECORD_METADATA_WAIT_TIMEOUT = 20000L;
  
  public SyncProducer(Properties prop)
  {
    long retryBackOffms = Long.parseLong(prop.getProperty("retry.backoff.ms"));
    if (prop.containsKey("retries"))
    {
      if (Integer.parseInt(prop.getProperty("retries")) > 0) {
        this.RECORD_METADATA_WAIT_TIMEOUT = (retryBackOffms * Integer.parseInt(prop.getProperty("retries")) * 2L);
      }
    }
    else {
      this.RECORD_METADATA_WAIT_TIMEOUT = (retryBackOffms * 2L);
    }
  }
  
  public void send(ProducerRecord<byte[], byte[]> record, Position pos)
    throws Exception
  {
    try
    {
      if (kafkaProducer != null)
      {
        kafkaProducer.send(record).get(this.RECORD_METADATA_WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
        if (this.receiptCallback != null) {
          this.receiptCallback.ack(pos);
        }
      }
    }
    catch (Exception e)
    {
      kafkaProducer.close();
      kafkaProducer = null;
      throw e;
    }
  }
}

