package com.bloom.target.jms.message;

import com.bloom.common.exc.AdapterException;

public abstract interface JMSMessage
{
  public abstract void fillData(byte[] paramArrayOfByte)
    throws AdapterException;
  
  public abstract void send()
    throws AdapterException;
}

