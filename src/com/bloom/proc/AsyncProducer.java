package com.bloom.proc;

import com.bloom.common.exc.SystemException;
import com.bloom.recovery.OutofOrderAckReassembler;
import com.bloom.recovery.Position;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AsyncProducer
  extends Producer
{
  private OutofOrderAckReassembler ackAssembler = new OutofOrderAckReassembler();
  private int recordSeqNo = 0;
  
  public void send(ProducerRecord<byte[], byte[]> message, Position pos)
    throws SystemException
  {
    RecordCallback callback = new RecordCallback(this.recordSeqNo, pos, this);
    if (kafkaProducer != null)
    {
      kafkaProducer.send(message, callback);
      if (this.receiptCallback != null) {
        this.ackAssembler.appendWrittenObject(this.recordSeqNo, pos);
      }
      this.recordSeqNo += 1;
    }
  }
  
  public OutofOrderAckReassembler getAckAssembler()
  {
    return this.ackAssembler;
  }
}

