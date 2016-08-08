package com.bloom.target.jms.message;

import com.bloom.common.exc.AdapterException;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class JMSBytesMessage
  implements JMSMessage
{
  private BytesMessage bytesMessage;
  private MessageProducer messageProducer;
  
  public JMSBytesMessage(Session session, MessageProducer messageProducer)
    throws AdapterException
  {
    try
    {
      this.bytesMessage = session.createBytesMessage();
      this.messageProducer = messageProducer;
    }
    catch (JMSException e)
    {
      throw new AdapterException("Problem in constructing JMS BytesMessage", e);
    }
  }
  
  public void fillData(byte[] data)
    throws AdapterException
  {
    try
    {
      this.bytesMessage.clearBody();
      this.bytesMessage.writeBytes(data);
    }
    catch (JMSException e)
    {
      throw new AdapterException("Problem in writing bytes message", e);
    }
  }
  
  public void send()
    throws AdapterException
  {
    try
    {
      this.messageProducer.send(this.bytesMessage);
    }
    catch (JMSException e)
    {
      throw new AdapterException("Problem in sending BytesMessage", e);
    }
  }
}

