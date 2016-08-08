package com.bloom.proc;


import com.bloom.recovery.OutofOrderAckReassembler;
import com.bloom.recovery.Position;
import com.bloom.runtime.components.ReceiptCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

class RecordCallback
  implements Callback
{
  private Position eventPos;
  private int seqNo;
  private AsyncProducer producer;
  private static Logger logger = Logger.getLogger(RecordCallback.class);
  
  public RecordCallback(int recordSeqNo, Position pos, AsyncProducer p)
  {
    this.seqNo = recordSeqNo;
    this.eventPos = pos;
    this.producer = p;
  }
  
  public void onCompletion(RecordMetadata metadata, Exception exception)
  {
    if (exception != null)
    {
      this.producer.setException(exception);
      return;
    }
    if (this.producer.receiptCallback != null)
    {
      Object obj = this.producer.getAckAssembler().acknowledgeReceive(this.seqNo);
      if (obj != null)
      {
        if (logger.isInfoEnabled()) {
          logger.info("Acknowledging position " + obj.toString());
        }
        this.producer.receiptCallback.ack((Position)obj);
      }
    }
  }
}

