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

package org.apache.hadoop.mapred;

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
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.NetCDFArrayWritable;
import java.util.List;
import ucar.nc2.*;
import ucar.nc2.iosp.*;
import ucar.nc2.iosp.netcdf3.*;
import ucar.unidata.io.*;
import ucar.nc2.dataset.*;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import java.util.Arrays;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.NetCDFReaderWithMeta;
import org.apache.hadoop.mapred.NetCDFReaderWithMetaShrinked;
/**
 * Treats keys as offset in fil`e and value as line. 
 */
public class NetCDFInputFormat extends FileInputFormat<Text, NetCDFArrayWritable> {

  private static final Log LOG
    = LogFactory.getLog(NetCDFInputFormat.class.getName());

  public static final String HIVE_QUERY = "hadoop.netcdf.hivequery.raw";


  private NetCDFInfo getNetCDFInfo(Path file, FileSystem fs, JobConf job)
  {
    //traverse header and return chunk start and size arrays
    NetCDFInfo result = new NetCDFInfo();//library call

    NetcdfFile ncFile;
    Variable v;
    ncFile = null;
    try {
    //if( file == null ){
	//System.out.println( "[SAMAN] NetCDFInputFormat.getNetCDFInfo  file is null" );
	//LOG.info( "[SAMAN] NetCDFInputFormat.getNetCDFInfo  file is null" );
    //}else{
	//System.out.println( "[SAMAN] NetCDFInputFormat.getNetCDFInfo file is " + file.toString() );		
	//LOG.info( "[SAMAN] NetCDFInputFormat.getNetCDFInfo  file is null" );
    //}	
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
	LOG.info( "Bad... "+ e );
	System.out.println("Bad... "+ e);
    }
    try{if (ncFile!=null)ncFile.close();}catch (Exception e) { LOG.info( "Bad2... "+e ); System.out.println("Bad2... "+e);}

    return result;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    FileStatus[] files = listStatus(job);

    //LOG.info("[SAMAN][NetCDFInputFormat][getSplits] hive query is: " + job.get(HIVE_QUERY, "Kossher"));
    //System.out.println("[SAMAN][NetCDFInputFormat][getSplits] hive query is: " + job.get(HIVE_QUERY, "Kossher"));
	
    //System.out.println( "[SAMAN] beginning of getSplits" );
    //LOG.info( "[SAMAN] beginning of getSplits" );
    //System.out.println( "[SAMAN] " + files.length );	
    //LOG.info( "[SAMAN] " + files.length ); 	
    // Save the number of input files in the job-conf
    job.setLong(NUM_INPUT_FILES, files.length);
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDir()) {
        throw new IOException("Not a file: " + file.getPath());
      }
      totalSize += file.getLen();
    }
    //long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
     //                       minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      //LOG.info("[SAMAN][NetCDFInputFormat][getSplits] File name is : " + path.getName());
      //System.out.println("[SAMAN][NetCDFInputFormat][getSplits] File name is : " + path.getName());
      FileSystem fs = path.getFileSystem(job);
      long length = file.getLen();
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      if ((length != 0) && isSplitable(fs, path)) { 
        long blockSize = file.getBlockSize();
        NetCDFInfo netInfo     = getNetCDFInfo(path, fs, job);
        long   recStart        = netInfo.recStart;
        long[] chunkStarts     = netInfo.chunkStarts;
        long   smallSize       = netInfo.smallRecSize;
        long   recSize         = netInfo.recSize;
        long   splitSize       = 0;
        int    chunkIndex      = 0;
        long   bytesRemaining  = chunkStarts[chunkStarts.length-1] + recSize - recStart - 2*smallSize;
        long   thisStart       = recStart;  //file position
        long   thisChunk       = 0;
        long   blockNo         = 1;
        LOG.info( "[SAMAN] NetCDFInputFormat.getSplits => recStart = " + recStart + ", chunkStarts = " + chunkStarts +
                ", smallSize = " + smallSize + ", recSize = " + recSize + ", bytesRemaining = " + bytesRemaining +
                ", thisStart = " + thisStart);
        System.out.println( "[SAMAN] NetCDFInputFormat.getSplits => recStart = " + recStart + ", chunkStarts = " + chunkStarts +
                ", smallSize = " + smallSize + ", recSize = " + recSize + ", bytesRemaining = " + bytesRemaining +
                ", thisStart = " + thisStart);
        while ( bytesRemaining > 0) {
          while ( chunkIndex < chunkStarts.length && chunkStarts[chunkIndex] < blockNo * blockSize ) {
            chunkIndex++;
          }
          long tempStart       = thisStart;
          long endChunk;
          if (chunkIndex >= chunkStarts.length) {
            splitSize = chunkStarts[chunkStarts.length-1] + recSize - thisStart - smallSize;
            //thisStart = chunkStarts[chunkIndex];

            //bytesRemaining should be 0 after this round
          }
          else {
            splitSize         = chunkStarts[chunkIndex] - thisStart - smallSize;
            thisStart         = chunkStarts[chunkIndex];
          }
          endChunk            = chunkIndex;
          //if( endChunk == thisChunk )
            //break;

          blockNo++;
          LOG.info( "[SAMAN] NetCDFInputFormat.getSplits => splitSize="+splitSize+", thisStart="+thisStart+
                    ", endChunk="+endChunk+", blockNo="+blockNo);
          System.out.println( "[SAMAN] NetCDFInputFormat.getSplits => splitSize="+splitSize+", thisStart="+thisStart+
                    ", endChunk="+endChunk+", blockNo="+blockNo);
          String[] splitHosts = getSplitHosts(blkLocations, tempStart, splitSize, clusterMap);
          FileSplit split     = new FileSplit(path, tempStart, splitSize, splitHosts);
          split.getFileSplit().startChunk = thisChunk;
          split.getFileSplit().endChunk = endChunk;
          splits.add(split);
          bytesRemaining -= splitSize;
          thisChunk = endChunk;
          //LOG.info( "[SAMAN] NetCDFInputFormat.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
          //System.out.println( "[SAMAN] NetCDFInputFormat.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
        }
      } else if (length != 0) {
        String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
        splits.add(new FileSplit(path, 0, length, splitHosts));
      } else { 
        //Create empty hosts array for zero length files
        splits.add(new FileSplit(path, 0, length, new String[0]));
      }
    }

    return splits.toArray(new FileSplit[splits.size()]);
  }

 @Override
  public RecordReader<Text, NetCDFArrayWritable> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
    throws IOException {
    
    reporter.setStatus(genericSplit.toString());
    //LOG.info( "[SAMAN] return getRecordReader" );
    //System.out.println( "[SAMAN] return getRecordReader" );	 	
    return new NetCDFReaderWithMeta(job, (FileSplit) genericSplit);
  }


}
