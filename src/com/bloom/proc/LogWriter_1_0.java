package com.bloom.proc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.event.ObjectMapperFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import org.apache.log4j.Logger;

@PropertyTemplate(name="LogWriter", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="name", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="filename", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="")}, outputType=Event.class)
public class LogWriter_1_0
  extends BaseProcess
{
  private static Logger logger = Logger.getLogger(LogWriter_1_0.class);
  ObjectMapper mapper = ObjectMapperFactory.newInstance();
  String outputName = "Default";
  BufferedWriter bw = null;
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    String name = (String)properties.get("name");
    String filename = (String)properties.get("filename");
    String charset = (String)properties.get("charset");
    OutputStreamWriter osw = null;
    if (name != null) {
      this.outputName = name;
    }
    if (filename != null)
    {
      if (charset != null) {
        osw = new OutputStreamWriter(new FileOutputStream(new File(filename)), charset);
      } else {
        osw = new FileWriter(filename);
      }
      this.bw = new BufferedWriter(osw);
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    try
    {
      Field[] fs = event.getClass().getDeclaredFields();
      String value = this.outputName + ": " + event.getClass().getSimpleName() + "{\n";
      for (Field f : fs) {
        if ((Modifier.isPublic(f.getModifiers())) && 
          (!"mapper".equals(f.getName()))) {
          value = value + "  " + f.getName() + ": " + this.mapper.writeValueAsString(f.get(event)) + "\n";
        }
      }
      value = value + "};";
      if (logger.isTraceEnabled()) {
        logger.trace(value);
      }
      synchronized (this.bw)
      {
        this.bw.write(value);
        this.bw.write("\n");
        this.bw.flush();
      }
    }
    catch (Throwable e)
    {
      logger.error("Error writing log file", e);
      throw e;
    }
  }
  
  public void stopWorker()
  {
    try
    {
      if (this.bw != null) {
        this.bw.close();
      }
    }
    catch (IOException e)
    {
      logger.error(e);
    }
    super.stopWorker();
  }
  
  public void close()
    throws Exception
  {
    stopWorker();
    super.close();
  }
}

