/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NetCDFArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.util.List;
import ucar.nc2.*;
import ucar.nc2.iosp.*;
import ucar.nc2.iosp.netcdf3.*;
import ucar.unidata.io.*;
import ucar.nc2.dataset.*;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import java.util.Arrays;
/**
 * Treats keys as offset in file and value as array. 
 */
public class NetCDFReaderWithCaching extends RecordReader<Text, NetCDFArrayWritable> {
  private static final Log LOG
    = LogFactory.getLog(LineRecordReader.class.getName());

  private long start;
  private long pos;	
  private long pos1D = 0; // 1D Index for dimensions [2:n]
  private long end;
  private int nDims;
  private NetcdfFile ncFile;
  private Variable v;
  private List<Dimension> dimensions;
  private long max1DSize = 1; // total number of elements could be stored in dimensions [2:n]	
  private Text key = new Text();
  private NetCDFArrayWritable value = new NetCDFArrayWritable();
  private String sectionLocator = null;
  private Array chunk = null;
  private float[] my;	
  private FloatWritable[] fw;
  private long previousTime = System.nanoTime();
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit fSplit = (FileSplit) genericSplit;
    start = fSplit.startChunk; //split.getStart();
    end = fSplit.endChunk; //start + split.getLength();
    final Path file = fSplit.getPath();
    
    ////LOG.info("Map is reading from input: " + file +" start chunk "+ start+" end chunk "+end);

    ncFile = NetcdfDataset.openFile(file.toString(), null);
    List<Variable> vs = ncFile.getVariables();
    v = vs.get(vs.size()-1);    
    LOG.info("Variable is "+ v.getFullName());
    dimensions = v.getDimensions();
    this.pos = start;
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<dimensions.size()-1; i++) {
        sb.append(",:");
    }
    sectionLocator =  sb.toString(); 
/*
    for ( int i=0; i < dimensions.size(); i++ ){
	max1DSize *= dimensions.get(i).getLength();
    }
*/
    //max1DSize = chunk.getSize();	
    //LOG.info( "max1DSize is " + max1DSize );	
  }

  private void convert1DtoND( long OneDIndex, long[] NDIndex ){
	for( int i = dimensions.size()-1; i >= 1; i-- ){
		int temp = 1;
		for( int j = dimensions.size()-1; j > i; j-- ){
			temp *= dimensions.get(j).getLength();
		}
		temp = (int)(OneDIndex / temp);
		temp = temp % dimensions.get(i).getLength();
		NDIndex[i-1] = temp;
	}
  }
  
  /** Read a line. */
  public boolean nextKeyValue()
    throws IOException {

    long time1, time2, time3, time4, time5, time6;
    long[] NDIndex = new long[dimensions.size()-1];
    time1 = System.nanoTime();
    convert1DtoND( pos1D, NDIndex );
    time2 = System.nanoTime();
    String keyword = new String();
    keyword += pos;
    for ( int i = 0; i < NDIndex.length; i++ )
	keyword = keyword + "-" +NDIndex[i];
    time3 = System.nanoTime();
    time4 = System.nanoTime();
    time5 = 0; 	
    if (pos < end) {
      key.set(keyword);
      time5=0;	
      time4 = System.nanoTime();
      if( pos1D == 0 ){	
      	try{
		////LOG.info( "[SAMAN] rsut("+pos+":"+pos+sectionLocator+")" );
        	chunk = ncFile.readSection("rsut("+pos+":"+pos+sectionLocator+")");
		time5 = System.nanoTime();
		if (chunk == null) {LOG.info("chunk is null");return false;}
		////LOG.info(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));
		max1DSize = chunk.getSize();
		my = (float[])chunk.get1DJavaArray(Float.class);
		fw = new FloatWritable[my.length];
		for (int i=0; i< fw.length; i++) {
			fw[i]=new FloatWritable(my[i]);
		}
      	} catch (ucar.ma2.InvalidRangeException e)
      	{
        	LOG.info("section error " + e);
      	}
      }
      ////LOG.info("[YIQI] "+(System.nanoTime()-previousTime));
      previousTime = System.nanoTime();
      FloatWritable[] fwResult = new FloatWritable[dimensions.size()+1];
      fwResult[0] = new FloatWritable((float)pos);
      for( int i = 1; i < dimensions.size(); i++ ){
	fwResult[i] = new FloatWritable((float)(NDIndex[i-1]));
      }	
      fwResult[dimensions.size()] = new FloatWritable( (float)my[(int)pos1D] );	
      // ADDED BY SAMAN
      //String floatNumbers = new String();	
      //for( int i = 0; i < fw.length; i++ ){
      //	floatNumbers = floatNumbers + fw[i] + " ";
      //}
      //LOG.info( "[SAMAN] " + floatNumbers );	
      String location = new String();	
      location = location + pos + ":" + pos;
      for( int i = 0; i < NDIndex.length; i++ ){
	location = location + "," + NDIndex[i] + ":" + NDIndex[i];
      }	
      ////LOG.info( "[SAMAN] location was (" + location + ")"  );	

      value.set(fwResult);
      if( pos1D == max1DSize-1 ){
	////LOG.info( "[SAMAN] pos1D " + pos1D + " reached " + max1DSize );
	pos++;
	pos1D=0;
      }else{
	////LOG.info( "[SAMAN] pos1D " + pos1D + " less than " + max1DSize );
	pos1D++;
      }	
      //pos ++;
      return true;

    }
    LOG.info("Reaching chunk end");

    return false;
  }


  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public NetCDFArrayWritable getCurrentValue() {
    return value;
  } 

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (ncFile != null) {
      ncFile.close(); 
    }
  }
}
