package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.proc.events.WAEvent;
import com.bloom.source.lib.io.common.HDFSCommon;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.utils.DirectoryTree;
import com.bloom.source.lib.utils.HDFSDirectoryTree;
import com.bloom.uuid.UUID;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

@PropertyTemplate(name="HDFSWriter", type=AdapterType.target, properties={@com.bloom.anno.PropertyTemplateProperty(name="filename", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="hadoopurl", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="directory", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="rolloverpolicy", type=String.class, required=false, defaultValue="DefaultRollingPolicy"), @com.bloom.anno.PropertyTemplateProperty(name="flushpolicy", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="authenticationpolicy", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="hadoopconfigurationpath", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue="")}, outputType=WAEvent.class, requiresFormatter=true)
public class HDFSWriter_1_0
  extends FileWriter_1_0
{
  private FileSystem hadoopFileSystem;
  private Logger logger = Logger.getLogger(HDFSWriter_1_0.class);
  private FSDataOutputStream fsDataOutputStream;
  private HDFSCommon hdfsCommon;
  
  public void init(Map<String, Object> writerProperties, Map<String, Object> formatterProperties, UUID inputStream, String distributionID)
    throws Exception
  {
    super.init(writerProperties, formatterProperties, inputStream, distributionID);
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("HDFSWriter is initialized with following properties\nHadoopURL " + this.hdfsCommon.getHadoopUrl());
    }
  }
  
  protected OutputStream getOutputStream(String filename)
    throws IOException
  {
    String url = this.hdfsCommon.getHadoopUrl() + filename;
    Path file = new Path(url);
    if (this.append)
    {
      if (this.hadoopFileSystem.exists(file)) {
        this.fsDataOutputStream = this.hadoopFileSystem.append(file);
      } else {
        this.fsDataOutputStream = this.hadoopFileSystem.create(file);
      }
    }
    else {
      this.fsDataOutputStream = this.hadoopFileSystem.create(file);
    }
    return this.fsDataOutputStream;
  }
  
  public void close()
    throws AdapterException
  {
    try
    {
      super.close();
      this.hadoopFileSystem.close();
    }
    catch (IOException|AdapterException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      throw se;
    }
  }
  
  protected void sync()
    throws IOException
  {
    this.fsDataOutputStream.flush();
    this.fsDataOutputStream.hsync();
  }
  
  protected String getWorkingDirectoryName()
  {
    return "/";
  }
  
  protected void identifyTokens(Property prop, Field[] fields, Map<String, Object> properties)
    throws AdapterException
  {
    this.hdfsCommon = new HDFSCommon(prop);
    this.hadoopFileSystem = this.hdfsCommon.getHDFSInstance();
    this.createLocalDirectory = false;
    super.identifyTokens(prop, fields, properties);
  }
  
  protected DirectoryTree createDirectoryTree(String tokenName, Field field, Map<String, Object> properties)
  {
    return new HDFSDirectoryTree(tokenName, field, properties, this.hadoopFileSystem);
  }
}

