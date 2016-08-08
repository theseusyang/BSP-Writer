package com.bloom.proc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@PropertyTemplate(name="TCPTextWriter", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="port", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="ip", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="autoreconnect", type=String.class, required=false, defaultValue="")}, outputType=Event.class)
public class TCPTextWriter_1_0
  extends BaseProcess
{
  private AsynchronousSocketChannel socket;
  private InetSocketAddress hostAddress;
  private BlockingQueue<String> queue;
  private volatile boolean running;
  
  private String eventToString(Event event)
    throws JsonProcessingException, IllegalArgumentException, IllegalAccessException
  {
    Field[] fs = event.getClass().getDeclaredFields();
    StringBuilder sb = new StringBuilder();
    sb.append(event.getClass().getSimpleName()).append("{\n");
    for (Field f : fs) {
      if (Modifier.isPublic(f.getModifiers()))
      {
        String fname = f.getName();
        if (!"mapper".equals(fname))
        {
          Object val = f.get(event);
          sb.append("  " + fname + ": " + val + "\n");
        }
      }
    }
    sb.append("};\n");
    return sb.toString();
  }
  
  private ByteBuffer strToBuffer(String s)
  {
    return ByteBuffer.wrap(s.getBytes());
  }
  
  CompletionHandler<Void, ByteBuffer> cn = new CompletionHandler()
  {
    public void completed(Void result, ByteBuffer buf)
    {
      TCPTextWriter_1_0.this.socket.write(buf, buf, TCPTextWriter_1_0.this.wh);
    }
    
    public void failed(Throwable e, ByteBuffer buf)
    {
      try
      {
        Thread.sleep(2000L);
      }
      catch (InterruptedException e1) {}
      TCPTextWriter_1_0.this.reconnect(buf);
    }
  };
  
  void reconnect(ByteBuffer buf)
  {
    if (!this.running) {
      return;
    }
    if (!this.socket.isOpen()) {
      try
      {
        this.socket = AsynchronousSocketChannel.open();
      }
      catch (IOException e1)
      {
        e1.printStackTrace();
        return;
      }
    }
    this.socket.connect(this.hostAddress, buf, this.cn);
  }
  
  CompletionHandler<Integer, ByteBuffer> wh = new CompletionHandler()
  {
    public void completed(Integer result, ByteBuffer buf)
    {
      if (result.intValue() == -1)
      {
        TCPTextWriter_1_0.this.reconnect(buf);
        return;
      }
      if (!buf.hasRemaining()) {
        try
        {
          String val = (String)TCPTextWriter_1_0.this.queue.take();
          buf = TCPTextWriter_1_0.this.strToBuffer(val);
        }
        catch (InterruptedException|IllegalArgumentException e)
        {
          return;
        }
      }
      TCPTextWriter_1_0.this.socket.write(buf, buf, this);
    }
    
    public void failed(Throwable exc, ByteBuffer buf)
    {
      if ((exc instanceof IOException)) {
        try
        {
          TCPTextWriter_1_0.this.socket.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
      TCPTextWriter_1_0.this.reconnect(buf);
    }
  };
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    this.queue = new ArrayBlockingQueue(100);
    Object portObj = properties.get("port");
    int port;
    if ((portObj instanceof String))
    {
      String portStr = (String)portObj;
      port = Integer.valueOf(portStr).intValue();
    }
    else
    {
      int port;
      if ((portObj instanceof Number))
      {
        Number num = (Number)portObj;
        port = num.intValue();
      }
      else
      {
        throw new RuntimeException("invalid type of <port> property");
      }
    }
    int port;
    Object ip = properties.get("ip");
    String addr;
    String addr;
    if ((ip instanceof String)) {
      addr = (String)ip;
    } else {
      addr = "localhost";
    }
    this.hostAddress = new InetSocketAddress(InetAddress.getByName(addr), port);
    this.socket = AsynchronousSocketChannel.open();
    this.running = true;
    reconnect(ByteBuffer.allocate(0));
  }
  
  public void close()
    throws Exception
  {
    this.running = false;
    if (this.socket != null) {
      this.socket.close();
    }
    super.close();
  }
  
  private void doOutput(String s)
  {
    this.queue.offer(s);
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    String val = eventToString(event);
    this.queue.offer(val);
  }
}
