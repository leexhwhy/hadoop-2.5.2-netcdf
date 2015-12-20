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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.mapred.NetCDFInfo;
import java.util.List;
import ucar.nc2.*;
import ucar.nc2.iosp.*;
import ucar.nc2.iosp.netcdf3.*;
import ucar.unidata.io.*;
import ucar.nc2.dataset.*;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import java.util.Arrays;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;

//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * Treats keys as offset in file and value as line. 
 */
public class NetCDFInputFormatYiqi extends FileInputFormat<Text, NetCDFArrayWritable> {

  private static final Log LOG
    = LogFactory.getLog(NetCDFInputFormatYiqi.class.getName());


  private NetCDFInfo getNetCDFInfo(Path file, FileSystem fs)
  {
    
    //traverse header and return chunk start and size arrays
    NetCDFInfo result = new NetCDFInfo();//library call

    NetcdfFile ncFile;
    Variable v;
    ncFile = null;
    try {
    ncFile = NetcdfDataset.openFile(file.toString(), null);    

    v = ncFile.findVariable("rsut");
    //List<Variable> vs = ncFile.getVariables();
    //v = vs.get(vs.size()-1);

    //LOG.info("Variable is "+ v.getFullName());
    result.fileSize = ncFile.vfileSize;
    result.recStart = ncFile.vrecStart;
    Long[] metaArray = v.reallyReadMeta().toArray(new Long[(int)(ncFile.vnumRecs)]);
    result.chunkStarts =ArrayUtils.toPrimitive(metaArray);
    //result.chunkSizes = nc.chunkSizes;
    result.numRecs = ncFile.vnumRecs;
    result.recSize = ncFile.vrecSize;
    result.smallRecSize = ncFile.vsmallRecSize;
    //result.shape = v.shape;

    } catch (Exception e)
    {
	LOG.info("Bad... "+ e);
    }
    try{if (ncFile!=null)ncFile.close();}catch (Exception e) {LOG.info("Bad2... "+e);}

    return result;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job)
    throws IOException {
    List<FileStatus> files = listStatus(job);
    
    // Save the number of input files in the job-conf
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDir()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      //LOG.info ("[net] adding "+file.getPath());
      totalSize += file.getLen();
    }

    //long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
     //                       minSplitSize);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job.getConfiguration());
      long length = file.getLen();
      //LOG.info("get file len of "+file.getPath());
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      if ((length != 0) && isSplitable(job, path)) { 
        long blockSize = file.getBlockSize();
        NetCDFInfo netInfo = getNetCDFInfo(path, fs);
        long recStart      = netInfo.recStart;
        long[] chunkSizes = netInfo.chunkSizes;
        long[] chunkStarts = netInfo.chunkStarts;
        long smallSize = netInfo.smallRecSize;
        //LOG.info("smallsize is "+smallSize);
        long recSize = netInfo.recSize;
        long splitSize = 0;
        int chunkIndex = 0;        
        long bytesRemaining = chunkStarts[chunkStarts.length-1] + recSize - recStart - smallSize;
        long thisStart = recStart+smallSize;  //file position
        long thisChunk = 0;
        long blockNo = 1;
        while ( bytesRemaining > 0) {
          while ( chunkIndex < chunkStarts.length && chunkStarts[chunkIndex] < blockNo * blockSize ) {
            chunkIndex++;
          }
          long tempStart = thisStart;
          long endChunk;
          if (chunkIndex >= chunkStarts.length) {
            splitSize = chunkStarts[chunkStarts.length-1] + recSize - thisStart;
            
            //bytesRemaining should be 0 after this round
          }
          else {
            splitSize = chunkStarts[chunkIndex] - thisStart;
            thisStart = chunkStarts[chunkIndex];
          }
          endChunk = chunkIndex;
          //LOG.info("[net] split "+path+ " start "+tempStart+" size " +splitSize +" from chunk [" +thisChunk +" to "+ endChunk+")");
          blockNo++;          
          int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
          FileSplit split = new FileSplit(path, tempStart, splitSize, blkLocations[blkIndex].getHosts());
          split.startChunk = thisChunk;
          split.endChunk = endChunk;
          splits.add(split);
          bytesRemaining -= splitSize;
          //LOG.info("[net] split " +path+" remaining "+bytesRemaining);
          thisChunk = endChunk;
        }
        
      } else if (length != 0) {
        splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
      } else { 
        //Create empty hosts array for zero length files
        splits.add(new FileSplit(path, 0, length, new String[0]));
      }
    }
    //LOG.info("Total # of splits: " + splits.size());
    for (int i=0; i<splits.size(); i++)
    {
      FileSplit split = (FileSplit) (splits.get(i));
      //LOG.info("split "+(i+1)+" "+ split.getStart()+ " "+split.getLength() + " " + split.startChunk +" "+ split.endChunk);
    }
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

    return splits;

  }

 @Override
  public RecordReader<Text, NetCDFArrayWritable> createRecordReader(
                                          InputSplit genericSplit, TaskAttemptContext context)
    throws IOException {
    
    return new NetCDFReader();
  }


}
