package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.Event;
import com.bloom.intf.Formatter;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.Acknowledgeable;
import com.bloom.recovery.Position;
import com.bloom.runtime.components.ReceiptCallback;
import com.bloom.source.lib.intf.OutputStreamProvider;
import com.bloom.source.lib.intf.RollOverObserver;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.rollingpolicy.outputstream.RollOverOutputStream;
import com.bloom.source.lib.rollingpolicy.outputstream.RollOverOutputStream.OutputStreamBuilder;
import com.bloom.source.lib.rollingpolicy.property.RollOverProperty;
import com.bloom.source.lib.rollingpolicy.util.DynamicRollOverFilenameFormat;
import com.bloom.source.lib.rollingpolicy.util.RollOverOutputStreamFactory;
import com.bloom.source.lib.rollingpolicy.util.RollingPolicyUtil;
import com.bloom.source.lib.rollingpolicy.util.RolloverFilenameFormat;
import com.bloom.source.lib.utils.DirectoryTree;
import com.bloom.uuid.UUID;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;
import org.apache.log4j.Logger;

@PropertyTemplate(name="FileWriter", type=AdapterType.target, properties={@com.bloom.anno.PropertyTemplateProperty(name="filename", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="directory", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="rolloverpolicy", type=String.class, required=false, defaultValue="DefaultRollingPolicy"), @com.bloom.anno.PropertyTemplateProperty(name="flushpolicy", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue="")}, outputType=WAEvent.class, requiresFormatter=true)
public class FileWriter_1_0
  extends BaseWriter
  implements RollOverObserver, Acknowledgeable
{
  private static final String ROLL_OVER_POLICY = "rolloverpolicy";
  protected boolean append;
  protected boolean createLocalDirectory;
  private String fileName;
  private String directory;
  private FileOutputStream fileOutputStream;
  private OutputStream outputStream;
  private Logger logger;
  private int flushInterval;
  private int flushCount;
  private int flushCounter;
  private boolean dynamicDirectoryEnabled;
  private DirectoryTree directoryTree;
  private Timer flushTimer;
  private boolean flushIntervalEnabled;
  private boolean flushCountEnabled;
  private RolloverFilenameFormat filenameFormat;
  private RollOverProperty rollOverProperty;
  private String currentWorkingDirectoryName;
  private Map<String, OutputStream> directoryOutputStreamCache;
  private RollOverOutputStream.OutputStreamBuilder outputStreamBuilder;
  private boolean writerClosed;
  private Position currentPosition;
  private boolean acknowledgePosition;
  private boolean compressionEnabled;
  
  public FileWriter_1_0()
  {
    this.append = false;this.createLocalDirectory = true;
    
    this.logger = Logger.getLogger(FileWriter_1_0.class);
    this.flushCounter = 0;
    this.dynamicDirectoryEnabled = false;
    
    this.flushIntervalEnabled = false;this.flushCountEnabled = false;
    
    this.writerClosed = false;
    
    this.acknowledgePosition = false;
    this.compressionEnabled = false;
  }
  
  public void init(Map<String, Object> writerProperties, Map<String, Object> formatterProperties, UUID inputStream, String distributionID)
    throws Exception
  {
    super.init(writerProperties, formatterProperties, inputStream, distributionID);
    
    Map<String, Object> localPropertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    localPropertyMap.putAll(writerProperties);
    localPropertyMap.putAll(formatterProperties);
    
    Property prop = new Property(localPropertyMap);
    String rollingPolicyName = prop.getString("rolloverpolicy", null);
    if (rollingPolicyName != null)
    {
      localPropertyMap.putAll(RollingPolicyUtil.mapPolicyValueToRollOverPolicyName(rollingPolicyName));
      rollingPolicyName = (String)localPropertyMap.get("rolloverpolicy");
    }
    this.rollOverProperty = new RollOverProperty(localPropertyMap);
    
    this.append = this.rollOverProperty.shouldAppend();
    
    this.fileName = prop.getString("filename", null);
    if (this.fileName != null)
    {
      if (this.fileName.trim().isEmpty()) {
        throw new AdapterException("File name cannot be empty, please specify a valid file name");
      }
    }
    else {
      throw new AdapterException("File name cannot be null, please specify a valid file name");
    }
    this.filenameFormat = new RolloverFilenameFormat(this.fileName, this.rollOverProperty.getFileLimit(), this.rollOverProperty.getSequenceStart(), this.rollOverProperty.getIncrementSequenceBy(), this.rollOverProperty.shouldAddDefaultSequence());
    
    identifyTokens(prop, this.fields, localPropertyMap);
    String flushPolicy = prop.getString("flushpolicy", null);
    if (flushPolicy != null)
    {
      Map<String, Object> flushProperties = extractFlushProperties(flushPolicy);
      Property flushProperty = new Property(flushProperties);
      this.flushInterval = flushProperty.getInt("flushinterval", 0);
      if (this.flushInterval < 0) {
        this.flushInterval = (-this.flushInterval);
      }
      this.flushInterval *= 1000;
      if (this.flushInterval != 0) {
        this.flushIntervalEnabled = true;
      }
      this.flushCount = flushProperty.getInt("flushcount", 0);
      if (this.flushCount < 0) {
        this.flushCount = (-this.flushCount);
      }
      if (this.flushCount != 0) {
        this.flushCountEnabled = true;
      }
    }
    String compressionType = prop.getString("compressiontype", null);
    if ((compressionType != null) && (!compressionType.trim().isEmpty()))
    {
      this.compressionEnabled = true;
      if (!compressionType.equalsIgnoreCase("GZIP")) {
        this.logger.warn("Unsupported compression type " + compressionType + " is specified. Defaulting to supported compression type : GZIP.");
      }
    }
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("FileWriter is initialized with following properties\nFileName - [" + this.fileName + "]\n" + "Directory - [" + this.directory + "]\n" + "RollingPolicy - [" + rollingPolicyName + "]\n" + "FlushPolicy - [" + flushPolicy + "]");
    }
    if (this.receiptCallback != null) {
      this.acknowledgePosition = true;
    }
    open();
  }
  
  private void open()
    throws AdapterException
  {
    try
    {
      this.outputStreamBuilder = new RollOverOutputStream.OutputStreamBuilder()
      {
        public OutputStream buildOutputStream(RolloverFilenameFormat filenameFormat)
          throws IOException
        {
          OutputStream outputStream = FileWriter_1_0.this.compressionEnabled ? new GZIPOutputStream(FileWriter_1_0.this.getOutputStream(FileWriter_1_0.this.directory + filenameFormat.getNextSequence())) : FileWriter_1_0.this.getOutputStream(FileWriter_1_0.this.directory + filenameFormat.getNextSequence());
          if ((FileWriter_1_0.this.formatter instanceof OutputStreamProvider)) {
            return ((OutputStreamProvider)FileWriter_1_0.this.formatter).provideOutputStream(outputStream);
          }
          return outputStream;
        }
      };
      if (this.dynamicDirectoryEnabled)
      {
        this.directory = "";
        this.directoryOutputStreamCache = new HashMap();
      }
      else
      {
        this.outputStream = RollOverOutputStreamFactory.getRollOverOutputStream(this.rollOverProperty, this.outputStreamBuilder, this.filenameFormat);
        if ((this.outputStream instanceof RollOverOutputStream)) {
          ((RollOverOutputStream)this.outputStream).registerObserver(this);
        }
        byte[] bytesToBeWrittenOnOpen = this.formatter.addHeader();
        if (bytesToBeWrittenOnOpen != null) {
          write(bytesToBeWrittenOnOpen);
        }
      }
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("File " + this.filenameFormat.getCurrentFileName() + " is opened in the directory " + new File(this.filenameFormat.getCurrentFileName()).getCanonicalPath());
      }
      if (this.flushIntervalEnabled) {
        initializeFlushTimer();
      }
    }
    catch (Exception e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_EXCEPTION, e);
      throw se;
    }
  }
  
  public void receive(int channel, Event event, Position pos)
    throws Exception
  {
    synchronized (this.outputStream)
    {
      this.currentPosition = pos;
      receiveImpl(channel, event);
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    try
    {
      if (this.dynamicDirectoryEnabled)
      {
        OutputStream outputStream = getDynamicOutputStream(event);
        if (outputStream != null) {
          this.outputStream = outputStream;
        } else {
          return;
        }
      }
      byte[] bytesToBeWritten = this.formatter.format(event);
      write(bytesToBeWritten);
    }
    catch (Exception e)
    {
      throw new AdapterException("Failure in writing event to the file. ", e);
    }
  }
  
  public void close()
    throws AdapterException
  {
    this.writerClosed = true;
    if ((this.flushCountEnabled) || (this.flushIntervalEnabled)) {
      synchronized (this.outputStream)
      {
        try
        {
          sync();
          if (this.acknowledgePosition) {
            this.receiptCallback.ack(this.currentPosition);
          }
        }
        catch (IOException e)
        {
          this.logger.error("Failure in synchronizing data to file. " + e.getMessage());
        }
      }
    }
    if (this.outputStream != null) {
      try
      {
        if (this.flushTimer != null) {
          stopFlushTimer();
        }
        this.outputStream.close();
        if (this.dynamicDirectoryEnabled)
        {
          this.directoryOutputStreamCache.clear();
          this.directoryOutputStreamCache = null;
        }
        if (this.logger.isTraceEnabled()) {
          this.logger.trace("File " + this.filenameFormat.getCurrentFileName() + " is closed");
        }
      }
      catch (Exception e)
      {
        AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
        throw se;
      }
    }
  }
  
  protected OutputStream getOutputStream(String filename)
    throws IOException
  {
    this.fileOutputStream = new FileOutputStream(new File(filename), this.append);
    return this.fileOutputStream;
  }
  
  private void write(byte[] bytesToBeWritten)
    throws Exception
  {
    try
    {
      this.outputStream.write(bytesToBeWritten);
      if (this.flushCountEnabled) {
        flush();
      }
    }
    catch (IOException e)
    {
      if (this.writerClosed) {
        this.logger.error("Failure in writing data to file. FileWriter is closed.");
      } else {
        throw e;
      }
    }
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("No of bytes written in the file " + this.filenameFormat.getCurrentFileName() + " is " + bytesToBeWritten.length);
    }
  }
  
  private void flush()
    throws IOException
  {
    this.flushCounter += 1;
    if (this.flushCount == this.flushCounter)
    {
      sync();
      if (this.acknowledgePosition) {
        this.receiptCallback.ack(this.currentPosition);
      }
      resetFlushCounter();
      if (this.flushIntervalEnabled)
      {
        stopFlushTimer();
        initializeFlushTimer();
      }
    }
  }
  
  private void resetFlushCounter()
  {
    this.flushCounter = 0;
  }
  
  protected void sync()
    throws IOException
  {
    this.outputStream.flush();
    this.fileOutputStream.getFD().sync();
  }
  
  protected void identifyTokens(Property prop, Field[] fields, Map<String, Object> properties)
    throws AdapterException
  {
    this.directory = prop.getString("directory", "");
    if ((this.directory != null) && 
      (!this.directory.trim().isEmpty()))
    {
      String separator = getSeparator();
      if (!this.directory.endsWith(separator)) {
        this.directory += separator;
      }
      if (this.directory.contains("%"))
      {
        this.dynamicDirectoryEnabled = true;
        validateTokenFromIdentifier(this.directory, fields, properties);
      }
      else if (this.createLocalDirectory)
      {
        File directoryToBeCreated = new File(this.directory);
        if ((!directoryToBeCreated.exists()) && 
          (!directoryToBeCreated.mkdirs())) {
          throw new AdapterException("Failure in writer initialization. Directory " + this.directory + " doesn't exist and unable to create it");
        }
      }
    }
  }
  
  private void validateTokenFromIdentifier(String identifier, Field[] fields, Map<String, Object> properties)
    throws AdapterException
  {
    String directory = identifier;
    int count = countOccurrencesOfToken(directory);
    if (count % 2 != 0) {
      throw new AdapterException("Failure in initializing writer. Directory name " + directory + " is invalid. Valid tokens are of the form %token1%/%token2%/...");
    }
    int[] tokenIndexArray = new int[count];
    
    int i = 0;
    for (int j = 0; i < directory.length(); i++) {
      if (directory.charAt(i) == '%')
      {
        tokenIndexArray[j] = i;
        j++;
      }
    }
    int startIndexOfToken = directory.indexOf('%');
    int endIndexOfToken = directory.lastIndexOf('%');
    String directoryPrefix = null;String directorySuffix = null;
    if (startIndexOfToken == 0)
    {
      directoryPrefix = getWorkingDirectoryName();
      if (endIndexOfToken + 1 == directory.length() - 1) {
        directorySuffix = directory.substring(endIndexOfToken + 1, directory.length());
      }
    }
    else if (endIndexOfToken + 1 == directory.length() - 1)
    {
      directoryPrefix = directory.substring(0, startIndexOfToken);
      directorySuffix = directory.substring(endIndexOfToken + 1, directory.length());
    }
    else
    {
      directoryPrefix = directory.substring(0, startIndexOfToken);
      directorySuffix = directory.substring(endIndexOfToken + 1, directory.length());
    }
    properties.put("directoryprefix", directoryPrefix);
    properties.put("directorysuffix", directorySuffix);
    
    boolean rootDirectory = false;
    for (int k = 0; k < count; k += 2)
    {
      String tokenName = null;
      tokenName = directory.substring(tokenIndexArray[k] + 1, tokenIndexArray[(k + 1)]);
      Field field = getAssociatedFieldOfToken(tokenName, fields);
      if (field != null)
      {
        if (!rootDirectory)
        {
          this.directoryTree = createDirectoryTree(tokenName, field, properties);
          rootDirectory = true;
        }
        else
        {
          this.directoryTree.addChildDirectory(tokenName, field);
        }
      }
      else {
        throw new AdapterException("Failure in initialzing writer. Specified token " + tokenName + " as part of directory property is not associated with the type of this input stream");
      }
    }
  }
  
  protected DirectoryTree createDirectoryTree(String tokenName, Field field, Map<String, Object> properties)
  {
    return new DirectoryTree(tokenName, field, properties);
  }
  
  private OutputStream getDynamicOutputStream(Event event)
    throws IOException
  {
    OutputStream dynamicOutputStream;
    if (this.directoryTree.createDirectory(event))
    {
      this.currentWorkingDirectoryName = this.directoryTree.getCurrentDirectoryName();
      DynamicRollOverFilenameFormat filenameFormat = new DynamicRollOverFilenameFormat(this.rollOverProperty.getFilename(), this.rollOverProperty.getFileLimit(), this.rollOverProperty.getSequenceStart(), this.rollOverProperty.getIncrementSequenceBy(), this.rollOverProperty.shouldAddDefaultSequence(), this.currentWorkingDirectoryName);
      
       dynamicOutputStream = RollOverOutputStreamFactory.getRollOverOutputStream(this.rollOverProperty, this.outputStreamBuilder, filenameFormat);
      if ((dynamicOutputStream instanceof RollOverOutputStream)) {
        postRollover(filenameFormat.getCurrentFileName());
      }
      this.directoryOutputStreamCache.put(this.currentWorkingDirectoryName, dynamicOutputStream);
    }
    else
    {
      if (!this.directoryTree.isDirectoryNameMalformed())
      {
        String currentDirectoryName = this.directoryTree.getCurrentDirectoryName();
        if (!this.currentWorkingDirectoryName.equalsIgnoreCase(currentDirectoryName))
        {
          this.currentWorkingDirectoryName = currentDirectoryName;
          dynamicOutputStream = (OutputStream)this.directoryOutputStreamCache.get(currentDirectoryName);
        }
        else
        {
          return this.outputStream;
        }
      }
      else
      {
        dynamicOutputStream = null;
      }
    }
    return dynamicOutputStream;
  }
  
  private Field getAssociatedFieldOfToken(String fieldName, Field[] fields)
  {
    for (Field field : fields) {
      if (field.getName().equalsIgnoreCase(fieldName)) {
        return field;
      }
    }
    return null;
  }
  
  private int countOccurrencesOfToken(String identifier)
  {
    int count = 0;
    for (int i = 0; i < identifier.length(); i++) {
      if (identifier.charAt(i) == '%') {
        count++;
      }
    }
    return count;
  }
  
  protected String getWorkingDirectoryName()
    throws AdapterException
  {
    try
    {
      String workingDirectoryName = new File(".").getCanonicalPath();
      String separator = FileSystems.getDefault().getSeparator();
      if (!workingDirectoryName.endsWith(separator)) {}
      return workingDirectoryName + separator;
    }
    catch (IOException e)
    {
      throw new AdapterException("Failure in initializing writer.", e);
    }
  }
  
  private void initializeFlushTimer()
  {
    this.flushTimer = new Timer();
    this.flushTimer.schedule(new FlushTask(), 0L, this.flushInterval);
  }
  
  private void stopFlushTimer()
  {
    this.flushTimer.cancel();
    this.flushTimer = null;
  }
  
  private boolean isFlushingCountEnabled()
  {
    return this.flushCountEnabled;
  }
  
  private Map<String, Object> extractFlushProperties(String flushPolicy)
    throws AdapterException
  {
    Map<String, Object> flushProperties = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    String[] flushPolicies = flushPolicy.split(",");
    for (String specifiedFlushPolicy : flushPolicies)
    {
      String[] policyProperties = specifiedFlushPolicy.split(":");
      if (policyProperties.length > 1)
      {
        if (!policyProperties[0].contains("flush")) {
          if (policyProperties[0].contains("event")) {
            policyProperties[0] = policyProperties[0].replace("event", "flush");
          } else {
            policyProperties[0] = ("flush" + policyProperties[0]);
          }
        }
        flushProperties.put(policyProperties[0], policyProperties[1]);
      }
      else if (!policyProperties[0].trim().isEmpty())
      {
        if (!policyProperties[0].contains("flush")) {
          if (policyProperties[0].contains("event")) {
            policyProperties[0] = policyProperties[0].replace("event", "flush");
          } else {
            policyProperties[0] = ("flush" + policyProperties[0]);
          }
        }
        flushProperties.put(policyProperties[0], null);
      }
    }
    return flushProperties;
  }
  
  protected String getSeparator()
  {
    return FileSystems.getDefault().getSeparator();
  }
  
  private class FlushTask
    extends TimerTask
  {
    private Logger logger = Logger.getLogger(FlushTask.class);
    
    private FlushTask() {}
    
    public void run()
    {
      try
      {
        synchronized (FileWriter_1_0.this.outputStream)
        {
          FileWriter_1_0.this.sync();
          if (FileWriter_1_0.this.acknowledgePosition) {
            FileWriter_1_0.this.receiptCallback.ack(FileWriter_1_0.this.currentPosition);
          }
        }
        if (FileWriter_1_0.this.isFlushingCountEnabled()) {
          FileWriter_1_0.this.resetFlushCounter();
        }
      }
      catch (IOException e)
      {
        this.logger.error("Failure in flushing data to disk", e);
      }
    }
  }
  
  public void preRollover()
    throws IOException
  {
    try
    {
      if ((this.flushCountEnabled) || (this.flushIntervalEnabled))
      {
        sync();
        if (this.acknowledgePosition) {
          this.receiptCallback.ack(this.currentPosition);
        }
      }
      byte[] bytesToBeAppended = this.formatter.addFooter();
      if (bytesToBeAppended != null) {
        write(bytesToBeAppended);
      }
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }
  
  public void postRollover(String lastRolledOverFilename)
    throws IOException
  {
    try
    {
      if (!this.writerClosed)
      {
        byte[] bytesToBeWrittenOnOpen = this.formatter.addHeader();
        if (bytesToBeWrittenOnOpen != null) {
          write(bytesToBeWrittenOnOpen);
        }
      }
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }
}

