package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.Event;
import com.bloom.intf.Formatter;
import com.bloom.proc.events.WAEvent;
import com.bloom.security.Password;
import com.bloom.source.lib.io.common.JMSCommon;
import com.bloom.source.lib.prop.Property;
import com.bloom.target.jms.message.JMSBytesMessage;
import com.bloom.target.jms.message.JMSMessage;
import com.bloom.target.jms.message.JMSTextMessage;
import com.bloom.uuid.UUID;
import java.util.Map;
import java.util.TreeMap;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.log4j.Logger;

@PropertyTemplate(name="JMSWriter", type=AdapterType.target, properties={@com.bloom.anno.PropertyTemplateProperty(name="UserName", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Password", type=Password.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Ctx", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Provider", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Topic", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="QueueName", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="connectionfactoryname", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="messagetype", type=String.class, required=false, defaultValue="TextMessage")}, outputType=WAEvent.class, requiresFormatter=true)
public class JMSWriter_1_0
  extends BaseWriter
{
  private JMSCommon jmsCommon;
  private Property property;
  private MessageProducer messageProducer;
  private JMSMessage message;
  private Logger logger = Logger.getLogger(JMSWriter_1_0.class);
  private static String TEXT_MESSAGE = "textmessage";
  private static String BYTES_MESSAGE = "bytesmessage";
  
  public void init(Map<String, Object> writerProperties, Map<String, Object> formatterProperties, UUID inputStream, String distributionID)
    throws Exception
  {
    super.init(writerProperties, formatterProperties, inputStream, distributionID);
    
    Map<String, Object> localPropertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    localPropertyMap.putAll(writerProperties);
    localPropertyMap.putAll(formatterProperties);
    if (localPropertyMap.get("Password") != null)
    {
      String plainPassword = ((Password)localPropertyMap.get("Password")).getPlain();
      localPropertyMap.put("Password", plainPassword);
    }
    this.property = new Property(localPropertyMap);
    this.jmsCommon = new JMSCommon(this.property);
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("JMSWriter is created for writing with following properties\nUserName - [" + this.property.userName + "]\n" + "Naming context - [" + this.property.context + "]\n" + "Provider URL - [" + this.property.provider + "]\n" + "Topic name - [" + this.property.topic + "]\n" + "Queue name - [" + this.property.queueName + "]\n" + "ConnectionFactory name - [" + this.property.connectionFactoryName + "]\n" + "MessageType - [" + this.property.messageType + "]");
    }
    open();
  }
  
  private void open()
    throws AdapterException
  {
    this.jmsCommon.initialize();
    if ((this.property.topic != null) && (!this.property.topic.trim().isEmpty())) {
      initializeWritingToTopic();
    } else if ((this.property.queueName != null) && (!this.property.queueName.trim().isEmpty())) {
      initializeWritingToQueue();
    } else {
      throw new AdapterException("Please provide a topic or queue name to send messages");
    }
    try
    {
      this.message = createMessage(this.property);
    }
    catch (Exception e)
    {
      throw new AdapterException("Problem in initializing JMSWriter", e);
    }
  }
  
  private void initializeWritingToTopic()
    throws AdapterException
  {
    try
    {
      Topic topic = this.jmsCommon.retrieveTopicInstance();
      this.messageProducer = this.jmsCommon.getSession().createProducer(topic);
      if (this.logger.isInfoEnabled()) {
        this.logger.info("JMSWriter is initialized for writing to the topic " + this.property.topic);
      }
    }
    catch (JMSException e)
    {
      throw new AdapterException("Problem in subscribing to the JMStopic " + this.property.topic, e);
    }
  }
  
  private void initializeWritingToQueue()
    throws AdapterException
  {
    try
    {
      Queue queue = this.jmsCommon.retrieveQueueInstance();
      this.messageProducer = this.jmsCommon.getSession().createProducer(queue);
      if (this.logger.isInfoEnabled()) {
        this.logger.info("JMSWriter is initialized for writing to the queue " + this.property.queueName);
      }
    }
    catch (JMSException e)
    {
      throw new AdapterException("Problem in establishing connection to a JMSQueue " + this.property.queueName, e);
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    try
    {
      byte[] headerMessage = this.formatter.addHeader();
      byte[] message = this.formatter.format(event);
      byte[] footerMessage = this.formatter.addFooter();
      if ((headerMessage != null) && (footerMessage != null))
      {
        int messageLength = headerMessage.length + message.length + footerMessage.length;
        byte[] messageToBeSent = new byte[messageLength];
        System.arraycopy(headerMessage, 0, messageToBeSent, 0, headerMessage.length);
        System.arraycopy(message, 0, messageToBeSent, headerMessage.length, message.length);
        System.arraycopy(footerMessage, 0, messageToBeSent, headerMessage.length + message.length, footerMessage.length);
        send(messageToBeSent);
      }
      else
      {
        send(message);
      }
    }
    catch (Exception e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_EXCEPTION, e);
      throw se;
    }
  }
  
  public void close()
    throws AdapterException
  {
    this.jmsCommon.closeConnection();
  }
  
  private void send(byte[] messageToBeSent)
    throws AdapterException
  {
    this.message.fillData(messageToBeSent);
    this.message.send();
  }
  
  private JMSMessage createMessage(Property property)
    throws AdapterException
  {
    String messageType = property.messageType;
    if (messageType != null)
    {
      messageType = messageType.toLowerCase();
      if (messageType.equals(TEXT_MESSAGE)) {
        return new JMSTextMessage(this.jmsCommon.getSession(), this.messageProducer, property.charset);
      }
      if (messageType.equals(BYTES_MESSAGE)) {
        return new JMSBytesMessage(this.jmsCommon.getSession(), this.messageProducer);
      }
      throw new AdapterException("Specified message type " + messageType + " is not supported");
    }
    throw new AdapterException("Please specify a valid message type");
  }
}
