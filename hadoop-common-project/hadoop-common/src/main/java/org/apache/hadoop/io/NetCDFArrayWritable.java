package org.apache.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

public class NetCDFArrayWritable extends ArrayWritable {
  public NetCDFArrayWritable() { 
    super(FloatWritable.class); 
  }	
}
