package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import org.apache.log4j.Logger;

@PropertyTemplate(name="CSVWriter", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="fileName", type=String.class, required=true, defaultValue="")}, outputType=WAEvent.class)
public class CSVWriter_1_0
  extends BaseProcess
{
  private static Logger logger = Logger.getLogger(CSVWriter_1_0.class);
  String fileName = null;
  BufferedWriter bw = null;
  boolean first = true;
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    this.fileName = ((String)properties.get("fileName"));
    if (this.fileName != null)
    {
      FileWriter fw = new FileWriter(this.fileName);
      this.bw = new BufferedWriter(fw);
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    try
    {
      synchronized (this.bw)
      {
        Object[] dataArray;
        if ((event instanceof WAEvent))
        {
          WAEvent waEvent = (WAEvent)event;
          dataArray = waEvent.data;
        }
        else
        {
          dataArray = event.getPayload();
        }
        if (dataArray != null)
        {
          StringBuilder outputStringBuilder = new StringBuilder();
          for (int i = 0; i < dataArray.length; i++) {
            if (i < dataArray.length - 1) {
              outputStringBuilder.append(dataArray[i] + ",");
            } else {
              outputStringBuilder.append(dataArray[i]);
            }
          }
          String outputString = outputStringBuilder.toString();
          synchronized (this.bw)
          {
            this.bw.write(outputString);
            this.bw.write(10);
            this.bw.flush();
          }
        }
      }
    }
    catch (Throwable e)
    {
      logger.error("Error writing to CSVWriter", e);
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

