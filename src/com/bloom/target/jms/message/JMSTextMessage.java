package com.bloom.target.jms.message;

import com.bloom.common.exc.AdapterException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.log4j.Logger;

public class JMSTextMessage
  implements JMSMessage
{
  private TextMessage textMessage;
  private MessageProducer messageProducer;
  private String charset;
  private Logger logger = Logger.getLogger(JMSTextMessage.class);
  
  public JMSTextMessage(Session session, MessageProducer messageProducer, String charset)
    throws AdapterException
  {
    try
    {
      this.textMessage = session.createTextMessage();
      this.messageProducer = messageProducer;
      this.charset = charset;
      if (this.charset == null)
      {
        this.charset = Charset.defaultCharset().name();
        this.logger.warn("Charset isn't specified, using default system's default charset " + this.charset);
      }
    }
    catch (JMSException e)
    {
      throw new AdapterException("Problem in constructing JMS TextMessage", e);
    }
  }
  
  public void fillData(byte[] data)
    throws AdapterException
  {
    try
    {
      this.textMessage.clearBody();
      this.textMessage.setText(new String(data, this.charset));
    }
    catch (UnsupportedEncodingException|JMSException e)
    {
      throw new AdapterException("Problem in setting the text message " + new String(data), e);
    }
  }
  
  public void send()
    throws AdapterException
  {
    try
    {
      this.messageProducer.send(this.textMessage);
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("Sent text message is " + this.textMessage.getText());
      }
    }
    catch (JMSException e)
    {
      throw new AdapterException("Problem in sending the text message " + this.textMessage.toString(), e);
    }
  }
}

