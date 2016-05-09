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
import org.apache.hadoop.mapreduce.lib.db.IntegerSplitter;
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
/**
 * Treats keys as offset in fil`e and value as line. 
 */
public class NetCDFInputFormatPrunerByFileIndex extends FileInputFormat<Text, NetCDFArrayWritable> {

    private static final Log LOG
            = LogFactory.getLog(NetCDFInputFormatPrunerByFileIndex.class.getName());

    public static final String HIVE_QUERY = "hadoop.netcdf.hivequery.raw";

    public enum QueryType { TIME, LAT, LON, NOLIMIT }

    private long blockSize = 128 * 1024 * 1024;

    private NetCDFInfo getNetCDFInfo(Path file, FileSystem fs, JobConf job)
    {
        //traverse header and return chunk start and size arrays
        NetCDFInfo result = new NetCDFInfo();//library call

        NetcdfFile ncFile;
        Variable v;
        Variable time;
        Variable lat;
        Variable lon;
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
            time = ncFile.findVariable("time");
            lat = ncFile.findVariable("lat");
            lon = ncFile.findVariable("lon");

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
            result.timeLength = (int)(time.getSize());
            result.latLength = (int)(lat.getSize());
            result.lonLength = (int)(lon.getSize());
            //result.shape = v.shape;

        } catch (Exception e)

        {
            LOG.info( "Bad... "+ e );
            e.printStackTrace();
            e.printStackTrace(System.out);
            System.out.println("Bad... \n" );
        }
        try{if (ncFile!=null)ncFile.close();}catch (Exception e) { LOG.info( "Bad2... "+e ); System.out.println("Bad2... "+e);}

        return result;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        FileStatus[] files = listStatus(job);

        LOG.info("[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] hive query is: " + job.get(HIVE_QUERY, "Kossher"));
        System.out.println("[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] hive query is: " + job.get(HIVE_QUERY, "Kossher"));

        /* Analyzing Query here */
        String hiveQuery = job.get(HIVE_QUERY, "Kossher");
        QueryType queryType = QueryType.NOLIMIT; // default mode
        if(hiveQuery.contains("where") || hiveQuery.contains("WHERE")) {
            if (hiveQuery.contains("time") || hiveQuery.contains("TIME")) {
                queryType = QueryType.TIME;
            } else if (hiveQuery.contains("lat") || hiveQuery.contains("LAT")) {
                queryType = QueryType.LAT;
            } else if (hiveQuery.contains("lon") || hiveQuery.contains("LON")) {
                queryType = QueryType.LON;
            }
        }

        float topLimit = -1;
        float bottomLimit = -1;

        if( queryType != QueryType.NOLIMIT ) {
            if (hiveQuery.contains("<")) {
                String[] querySplitted = hiveQuery.split(" ");
                int i = Arrays.asList(querySplitted).indexOf("<");
                topLimit = Float.valueOf(querySplitted[i+1]);
            }
            if (hiveQuery.contains(">")) {
                String[] querySplitted = hiveQuery.split(" ");
                int i = Arrays.asList(querySplitted).indexOf(">");
                bottomLimit = Float.valueOf(querySplitted[i+1]);
            }
        }

        System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndex] QueryType = " + queryType.toString()
                +", topLimit = " + topLimit + ", bottomLimit = " + bottomLimit );
        LOG.info("[SAMAN][NetCDFInputFormatPrunerByFileIndex] QueryType = " + queryType.toString()
                + ", topLimit = " + topLimit + ", bottomLimit = " + bottomLimit);
        /* End Analyzing Query here */

        System.out.println( "[SAMANPruner] beginning of getSplits" );
        LOG.info( "[SAMANPruner] beginning of getSplits" );
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
            int fileIndex = 0;
            int dimIndex = 0;
            if( queryType == QueryType.TIME || queryType == QueryType.NOLIMIT){
                if( path.getName().contains("lat") || path.getName().contains("lon") )
                    continue;
            }else if( queryType == QueryType.LAT ){
                //if( !path.getName().contains("lat") )
                //    continue;
            }else if( queryType == QueryType.LON ){
                //if( !path.getName().contains("lon") )
                //    continue;
            }
            if( queryType == QueryType.TIME ){
                String[] parts = path.getName().split("-");
                fileIndex = Integer.valueOf(parts[1]);
            }
            else if( queryType == QueryType.LAT || queryType == QueryType.LON ){
                if( path.getName().contains("_") ){
                    String[] parts = path.getName().split("_");
                    fileIndex = Integer.valueOf(parts[2]);
                    dimIndex = Integer.valueOf(parts[0].substring(7));
                }else{
                    //dimIndex = Integer.valueOf(path.getName().substring(7));
                    String[] parts = path.getName().split("-");
                    dimIndex = Integer.valueOf(parts[1]);
                }
            }

            LOG.info("[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] File name is : " + path.getName());
            System.out.println("[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] File name is : " + path.getName());
            FileSystem  fs = path.getFileSystem(job);
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            String[] hosts = new String[3];
            for( int i = 0; i < blkLocations.length; i++ ){
                for( int j = 0; j < blkLocations[i].getHosts().length; j++ ) {
                    System.out.println("[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] block location " + blkLocations[i].getHosts()[j]);
                    hosts[j] = blkLocations[i].getHosts()[j];
                }
            }
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
                long   thisStart       = recStart; // file position
                long   thisChunk       = 0;
                long   blockNo         = 1;
                long numChunksPerKey = 0;
                if( queryType == QueryType.LAT ){
                    long chunkSize = netInfo.timeLength * netInfo.lonLength * 4;
                    numChunksPerKey = blockSize / chunkSize;
                }else if( queryType == QueryType.LON ){
                    long chunkSize = netInfo.timeLength * netInfo.latLength * 4;
                    numChunksPerKey = blockSize / chunkSize;
                }

                System.out.println( "[SAMAN][NetCDFInputFormat][getSplits] numChunksPerKey = " + numChunksPerKey );

                //LOG.info( "[SAMAN] NetCDFInputFormatPruner.getSplits => recStart = " + recStart + ", chunkStarts = " + chunkStarts +
                //        ", smallSize = " + smallSize + ", recSize = " + recSize + ", bytesRemaining = " + bytesRemaining +
                //        ", thisStart = " + thisStart);
                //System.out.println( "[SAMAN] NetCDFInputFormatPruner.getSplits => recStart = " + recStart + ", chunkStarts = " + chunkStarts +
                //        ", smallSize = " + smallSize + ", recSize = " + recSize + ", bytesRemaining = " + bytesRemaining +
                //        ", thisStart = " + thisStart);
                while ( bytesRemaining > 0) {
                    while ( chunkIndex < chunkStarts.length && chunkStarts[chunkIndex] < blockNo * blockSize ) {
                        chunkIndex++;
                    }
                    long tempStart       = thisStart;
                    long endChunk;
                    if (chunkIndex >= chunkStarts.length) {
                        splitSize = chunkStarts[chunkStarts.length-1] + recSize - thisStart - smallSize;

                        //bytesRemaining should be 0 after this round
                    }
                    else {
                        splitSize         = chunkStarts[chunkIndex] - thisStart - smallSize;
                        thisStart         = chunkStarts[chunkIndex];
                    }
                    endChunk            = chunkIndex;
                    blockNo++;
                    //LOG.info( "[SAMAN] NetCDFInputFormatPruner.getSplits => splitSize="+splitSize+", thisStart="+thisStart+
                    //        ", endChunk="+endChunk+", blockNo="+blockNo);
                    //System.out.println( "[SAMAN] NetCDFInputFormatPruner.getSplits => splitSize="+splitSize+", thisStart="+thisStart+
                    //        ", endChunk="+endChunk+", blockNo="+blockNo);
                    String[] splitHosts = getSplitHosts(blkLocations, tempStart, splitSize, clusterMap);
                    //String[] splitHosts = hosts;
                    FileSplit split     = new FileSplit(path, tempStart, splitSize, splitHosts);
                    split.getFileSplit().startChunk = thisChunk;
                    split.getFileSplit().endChunk = endChunk;
                    if( queryType == QueryType.TIME ) {
                        if ((topLimit < thisChunk + (fileIndex*netInfo.timeLength)) && (topLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                        if ((bottomLimit > endChunk + (fileIndex*netInfo.timeLength)) && (bottomLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                    } else if( queryType == QueryType.LAT || queryType == QueryType.LON ){
                        System.out.println( "[SAMAN][NetCDFInputFormat][getSplits] file = "
                                + path.getName() + ", topLimit = " + topLimit + ", bottomLimit = " + bottomLimit + ", dimIndex = " + dimIndex );
                        /*
                        if( topLimit < dimIndex*numChunksPerKey && (topLimit != -1) ){
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                        if( bottomLimit > dimIndex*numChunksPerKey && (bottomLimit != -1) ){
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }*/
                        if (topLimit < thisChunk && (topLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                        if (bottomLimit > endChunk && (bottomLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                        /*
                        if ((topLimit < thisChunk) && (topLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                        if ((bottomLimit > endChunk) && (bottomLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                        */

                        if( bottomLimit > thisChunk && (bottomLimit != -1) ){
                            System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] startChunk = "
                                    + bottomLimit );
                            split.getFileSplit().startChunk = (long)bottomLimit;
                        }
                        if( topLimit < endChunk && (topLimit != -1) ){
                            System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] endChunk = "
                                    + endChunk );
                            split.getFileSplit().endChunk = (long)topLimit;
                        }

                        //split.getFileSplit().endChunk = (long)topLimit;
                    } else {
                        if ((topLimit < thisChunk) && (topLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                        if ((bottomLimit > endChunk) && (bottomLimit != -1)) {
                            bytesRemaining -= splitSize;
                            thisChunk = endChunk;
                            continue;
                        }
                    }

                    splits.add(split);

                    bytesRemaining -= splitSize;
                    thisChunk = endChunk;
                    //LOG.info( "[SAMAN] NetCDFInputFormatPruner.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
                    //System.out.println( "[SAMAN] NetCDFInputFormatPruner.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
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
