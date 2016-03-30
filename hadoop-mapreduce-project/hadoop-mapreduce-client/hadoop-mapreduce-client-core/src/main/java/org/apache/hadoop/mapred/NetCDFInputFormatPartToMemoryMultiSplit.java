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
import java.util.*;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
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
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.NetCDFReaderWithMeta;
/**
 * Treats keys as offset in fil`e and value as line. 
 */
public class NetCDFInputFormatPartToMemoryMultiSplit extends FileInputFormat<Text, NetCDFArrayWritable> {

    private static final Log LOG
            = LogFactory.getLog(NetCDFInputFormatPartToMemoryMultiSplit.class.getName());

    public static final String HIVE_QUERY = "hadoop.netcdf.hivequery.raw";

    public enum QueryType { TIME, LAT, LON, NOLIMIT }

    private long blockSize = 128 * 1024 * 1024;

    // mapping from a block to the nodes on which it has replicas
    HashMap<NetCDFFileSplit, String[]> blockToNodes =
            new HashMap<NetCDFFileSplit, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, Set<NetCDFFileSplit>> nodeToBlocks =
            new HashMap<String, Set<NetCDFFileSplit>>();

    NetCDFInfo netInfo = null;


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
            System.out.println("Bad... "+ e);
        }
        try{if (ncFile!=null)ncFile.close();}catch (Exception e) { LOG.info( "Bad2... "+e ); System.out.println("Bad2... "+e);}

        return result;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        FileStatus[] files = listStatus(job);

        LOG.info("[SAMAN][NetCDFInputFormatPartToMemoryMultiSplit][getSplits] hive query is: " + job.get(HIVE_QUERY, "Kossher"));
        System.out.println("[SAMAN][NetCDFInputFormatPartToMemoryMultiSplit][getSplits] hive query is: " + job.get(HIVE_QUERY, "Kossher"));

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

        //System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndex] QueryType = " + queryType.toString()
        //        +", topLimit = " + topLimit + ", bottomLimit = " + bottomLimit );
        //LOG.info("[SAMAN][NetCDFInputFormatPrunerByFileIndex] QueryType = " + queryType.toString()
        //        + ", topLimit = " + topLimit + ", bottomLimit = " + bottomLimit);
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
        ArrayList<NetCDFFileSplit> splits = new ArrayList<NetCDFFileSplit>(numSplits);
        ArrayList<NetCDFFileSplit> finalSplits = new ArrayList<NetCDFFileSplit>();
        NetworkTopology clusterMap = new NetworkTopology();
        for (FileStatus file: files) {
            Path path = file.getPath();
            int fileIndex = 0;
            int dimIndex = 0;

            //LOG.info("[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] File name is : " + path.getName());
            System.out.println("[SAMAN][NetCDFInputFormatPartToMemoryMultiSplit][getSplits] File name is : " + path.getName());
            FileSystem fs = path.getFileSystem(job);
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(fs, path)) {
                long blockSize = file.getBlockSize();
                netInfo     = getNetCDFInfo(path, fs, job);
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
                    System.out.println( "[SAMAN] NetCDFInputFormatPruner.getSplits => splitSize="+splitSize+", thisStart="+thisStart+
                            ", endChunk="+endChunk+", blockNo="+blockNo);
                    String[] splitHosts = getSplitHosts(blkLocations, tempStart, splitSize, clusterMap);
                    NetCDFFileSplit split     = new NetCDFFileSplit(path, tempStart, splitSize, splitHosts);

                    split.getFileSplit().startChunk.add(thisChunk);
                    split.getFileSplit().endChunk.add(endChunk);

                    if( queryType == QueryType.TIME ){
                        split.getFileSplit().timeStartLimit.add((long)bottomLimit);
                        split.getFileSplit().timeEndLimit.add((long)topLimit);
                        split.getFileSplit().latStartLimit.add((long)-1);
                        split.getFileSplit().latEndLimit.add((long)-1);
                        split.getFileSplit().lonStartLimit.add((long)-1);
                        split.getFileSplit().lonEndLimit.add((long)-1);
                    }else if( queryType == QueryType.LAT ){
                        split.getFileSplit().timeStartLimit.add((long)-1);
                        split.getFileSplit().timeEndLimit.add((long)-1);
                        split.getFileSplit().latStartLimit.add((long)bottomLimit);
                        split.getFileSplit().latEndLimit.add((long)topLimit);
                        split.getFileSplit().lonStartLimit.add((long)-1);
                        split.getFileSplit().lonEndLimit.add((long)-1);
                    }else if( queryType == QueryType.LON ){
                        split.getFileSplit().timeStartLimit.add((long)-1);
                        split.getFileSplit().timeEndLimit.add((long)-1);
                        split.getFileSplit().latStartLimit.add((long)-1);
                        split.getFileSplit().latEndLimit.add((long)-1);
                        split.getFileSplit().lonStartLimit.add((long)bottomLimit);
                        split.getFileSplit().lonEndLimit.add((long)topLimit);
                    }

                    blockToNodes.put( split, splitHosts );

                    System.out.println( "[SAMAN][NetCDFInputFormat][getSplits] Put the nodes with the specified split into the node to block set" );
                    for( int i = 0; i < splitHosts.length; i++ ){
                        Set<NetCDFFileSplit> splitList = nodeToBlocks.get(splitHosts[i]);
                        if( splitList == null ){
                            splitList = new LinkedHashSet<NetCDFFileSplit>();
                            nodeToBlocks.put( splitHosts[i], splitList );
                        }
                        splitList.add( split );
                    }

                    /*
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

                        blockToNodes.put( split, splitHosts );

                        // Put the nodes with the specified split into the node to block set
                        System.out.println( "[SAMAN][NetCDFInputFormat][getSplits] Put the nodes with the specified split into the node to block set" );
                        for( int i = 0; i < splitHosts.length; i++ ){
                            Set<NetCDFFileSplit> splitList = nodeToBlocks.get(splitHosts[i]);
                            if( splitList == null ){
                                splitList = new LinkedHashSet<NetCDFFileSplit>();
                                nodeToBlocks.put( splitHosts[i], splitList );
                            }
                            splitList.add( split );
                        }

                        System.out.println("[SAMAN][NetCDFInputFormat][getSplits] set start and end!" );

                        split.getFileSplit().startChunk.add(thisChunk);
                        split.getFileSplit().endChunk.add(endChunk);
                    } else if( queryType == QueryType.LAT || queryType == QueryType.LON ){
                        //System.out.println( "[SAMAN][NetCDFInputFormat][getSplits] file = "
                        //        + path.getName() + ", topLimit = " + topLimit + ", bottomLimit = " + bottomLimit + ", dimIndex = " + dimIndex );
                        */
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
                        /*
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
                        */
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
                        //split.getNetCDFFileSplit().endChunk = (long)topLimit;
                        /*
                        split.getFileSplit().startChunk.add(thisChunk);
                        split.getFileSplit().endChunk.add(endChunk);
                        */
                        // Put the block into the block to node set
                    /*

                        blockToNodes.put( split, splitHosts );

                        // Put the nodes with the specified split into the node to block set
                        for( int i = 0; i < splitHosts.length; i++ ){
                            Set<NetCDFFileSplit> splitList = nodeToBlocks.get(splitHosts[i]);
                            if( splitList == null ){
                                splitList = new LinkedHashSet<NetCDFFileSplit>();
                                nodeToBlocks.put( splitHosts[i], splitList );
                            }
                            splitList.add( split );
                        }

                        // For the test, we would assign everything statically.
                        if( bottomLimit > thisChunk && (bottomLimit != -1) ){
                            System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] startChunk = "
                                    + bottomLimit );
                            split.getFileSplit().startChunk.add((long)bottomLimit);
                        }else{
                            split.getFileSplit().startChunk.add(thisChunk);
                        }
                        if( topLimit < endChunk && (topLimit != -1) ){
                            System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] endChunk = "
                                    + endChunk );
                            split.getFileSplit().endChunk.add((long)topLimit);
                        }else{
                            split.getFileSplit().endChunk.add(endChunk);
                        }
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

                        blockToNodes.put( split, splitHosts );

                        // Put the nodes with the specified split into the node to block set
                        for( int i = 0; i < splitHosts.length; i++ ){
                            Set<NetCDFFileSplit> splitList = nodeToBlocks.get(splitHosts[i]);
                            if( splitList == null ){
                                splitList = new LinkedHashSet<NetCDFFileSplit>();
                                nodeToBlocks.put( splitHosts[i], splitList );
                            }
                            splitList.add( split );
                        }

                        split.getFileSplit().startChunk.add(thisChunk);
                        split.getFileSplit().endChunk.add(endChunk);
                    }
                    */

                    splits.add(split);

                    bytesRemaining -= splitSize;
                    thisChunk = endChunk;
                    //LOG.info( "[SAMAN] NetCDFInputFormatPruner.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
                    //System.out.println( "[SAMAN] NetCDFInputFormatPruner.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
                }

            } else if (length != 0) {
                String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
                //splits.add(new FileSplit(path, 0, length, splitHosts));
            } else {
                //Create empty hosts array for zero length files
                //splits.add(new FileSplit(path, 0, length, new String[0]));
            }
        }

        // Now it's time to merge non-complete splits.
        // Check if each split has enough space to include another split too

        Set<String> completedNodes = new HashSet<String>();
        ArrayList<NetCDFFileSplit> validBlocks = new ArrayList<NetCDFFileSplit>();
        long curSplitSize = 0;
        Multiset<String> splitsPerNode = HashMultiset.create();

        for (Iterator<Map.Entry<String, Set<NetCDFFileSplit>>> iter = nodeToBlocks
                .entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String, Set<NetCDFFileSplit>> one = iter.next();
            String node = one.getKey();

            System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] node is = " + node );

            // Skip the node if it has previously been marked as completed.
            if (completedNodes.contains(node)) {
                continue;
            }

            Set<NetCDFFileSplit> blocksInCurrentNode = one.getValue();

            // for each block, copy it into validBlocks. Delete it from
            // blockToNodes so that the same block does not appear in
            // two different splits.
            Iterator<NetCDFFileSplit> oneBlockIter = blocksInCurrentNode.iterator();
            while (oneBlockIter.hasNext()) {
                NetCDFFileSplit oneblock = oneBlockIter.next();

                System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] " +
                        "split is: " + oneblock.getFileSplit().getPath());

                // Remove all blocks which may already have been assigned to other
                // splits.
                if(!blockToNodes.containsKey(oneblock)) {
                    oneBlockIter.remove();
                    continue;
                }

                validBlocks.add(oneblock);
                if( queryType == QueryType.LAT ){
                    curSplitSize += (oneblock.getFileSplit().latEndLimit.get(0) - oneblock.getFileSplit().latStartLimit.get(0)) * 4 * netInfo.lonLength * netInfo.timeLength;
                }else if( queryType == QueryType.LON ){
                    curSplitSize += (oneblock.getFileSplit().lonEndLimit.get(0) - oneblock.getFileSplit().lonStartLimit.get(0)) * 4 * netInfo.latLength * netInfo.timeLength;
                }else if( queryType == QueryType.TIME ){
                    curSplitSize += (oneblock.getFileSplit().timeEndLimit.get(0) - oneblock.getFileSplit().timeStartLimit.get(0)) * 4 * netInfo.latLength * netInfo.lonLength;
                }else{
                    curSplitSize += (oneblock.getFileSplit().endChunk.get(0) - oneblock.getFileSplit().startChunk.get(0)) * 4 * netInfo.latLength * netInfo.lonLength;
                }
                blockToNodes.remove(oneblock);
                System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] curSplitSize = " + curSplitSize );

                //curSplitSize += singleSplitSize;

                //System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] " +
                //        "Added to valid blocks!" );

                // if the accumulated split size exceeds the maximum, then
                // create this split.
                if (blockSize != 0 && curSplitSize >= blockSize) {
                    // create an input split and add it to the splits array
                    addCreatedSplit(finalSplits, Collections.singleton(node), validBlocks);
                    //totalLength -= curSplitSize;

                    //System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] " +
                    //        "addCreatedSplit called!" );

                    curSplitSize = 0;
                    splitsPerNode.add(node);

                    // Remove entries from blocksInNode so that we don't walk these
                    // again.
                    //blocksInCurrentNode.removeAll(validBlocks);
                    validBlocks.clear();

                    // Done creating a single split for this node. Move on to the next
                    // node so that splits are distributed across nodes.
                    //break;
                }

            }
            if( !validBlocks.isEmpty() ){
                //System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] validBlocks not empty!" );
                addCreatedSplit(finalSplits, Collections.singleton(node), validBlocks);
                curSplitSize = 0;
                splitsPerNode.add(node);
                blocksInCurrentNode.removeAll(validBlocks);
                validBlocks.clear();
            }
        }

        Set<NetCDFFileSplit> singleSplitsSet =  blockToNodes.keySet();
        Iterator itrSingle = singleSplitsSet.iterator();
        while( itrSingle.hasNext() ){
            NetCDFFileSplit temp = (NetCDFFileSplit)itrSingle.next();
            addCreatedSingleSplit( finalSplits, temp.getLocations() , temp );
        }


        Iterator itr = finalSplits.iterator();
        while( itr.hasNext() ){

            NetCDFFileSplit temp = (NetCDFFileSplit)itr.next();

            String[] locations = temp.getFileSplit().getLocations();
            String locationsString = "";
            for( int i = 0; i < locations.length; i++ )
                locationsString += locations[i];

            String pathsString = "";
            List<Path> paths = temp.getFileSplit().getPaths();
            for( Path path : paths )
                pathsString += path.getName()+",";

            String startsString = "";
            List<Long> starts = temp.getFileSplit().startChunk;
            for( Long start : starts )
                startsString += (start+",");


            String endsString = "";
            List<Long> ends = temp.getFileSplit().endChunk;
            for( Long end : ends )
                endsString += (end+",");

            System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] " +
                    "locations="+locationsString+","+
                    "paths="+pathsString+","+
                    "starts="+startsString+","+
                    "ends="+endsString+",");
        }


        return finalSplits.toArray(new NetCDFFileSplit[finalSplits.size()]);


    }

    /**
     * Create a single split from the list of blocks specified in validBlocks
     * Add this new split into splitList.
     */
    private void addCreatedSplit(List<NetCDFFileSplit> splitList,
                                 Collection<String> locations,
                                 ArrayList<NetCDFFileSplit> validBlocks) {
        // create an input split
        List<Path> fl = new LinkedList<Path>();
        List<Long> offset = new LinkedList<Long>();
        List<Long> length = new LinkedList<Long>();
        List<Long> startChunk = new LinkedList<Long>();
        List<Long> endChunk = new LinkedList<Long>();
        List<Long> timeStartLimit = new LinkedList<Long>();
        List<Long> timeEndLimit = new LinkedList<Long>();
        List<Long> latStartLimit = new LinkedList<Long>();
        List<Long> latEndLimit = new LinkedList<Long>();
        List<Long> lonStartLimit = new LinkedList<Long>();
        List<Long> lonEndLimit = new LinkedList<Long>();

        for (int i = 0; i < validBlocks.size(); i++) {
            fl.add(validBlocks.get(i).getFileSplit().getPaths().get(0));
            offset.add(validBlocks.get(i).getFileSplit().getStart());
            length.add(validBlocks.get(i).getFileSplit().getLength());
            startChunk.add(validBlocks.get(i).getFileSplit().startChunk.get(0));
            endChunk.add(validBlocks.get(i).getFileSplit().endChunk.get(0));
            timeStartLimit.add(validBlocks.get(i).getFileSplit().timeStartLimit.get(0));
            timeEndLimit.add(validBlocks.get(i).getFileSplit().timeEndLimit.get(0));
            latStartLimit.add(validBlocks.get(i).getFileSplit().latStartLimit.get(0));
            latEndLimit.add(validBlocks.get(i).getFileSplit().latEndLimit.get(0));
            lonStartLimit.add(validBlocks.get(i).getFileSplit().lonStartLimit.get(0));
            lonEndLimit.add(validBlocks.get(i).getFileSplit().lonEndLimit.get(0));
        }

        // add this split to the list that is returned
        NetCDFFileSplit thissplit = new NetCDFFileSplit(fl, offset,
                length, locations.toArray(new String[0]), startChunk, endChunk,
                timeStartLimit, timeEndLimit, latStartLimit, latEndLimit, lonStartLimit,
                lonEndLimit);
        splitList.add(thissplit);
    }

    private void addCreatedSingleSplit( List<NetCDFFileSplit> splitList,
                                        String[] locations,
                                        NetCDFFileSplit validBlock){
        List<Path> fl = new LinkedList<Path>();
        List<Long> offset = new LinkedList<Long>();
        List<Long> length = new LinkedList<Long>();
        List<Long> startChunk = new LinkedList<Long>();
        List<Long> endChunk = new LinkedList<Long>();
        List<Long> timeStartLimit = new LinkedList<Long>();
        List<Long> timeEndLimit = new LinkedList<Long>();
        List<Long> latStartLimit = new LinkedList<Long>();
        List<Long> latEndLimit = new LinkedList<Long>();
        List<Long> lonStartLimit = new LinkedList<Long>();
        List<Long> lonEndLimit = new LinkedList<Long>();

        fl.add( validBlock.getFileSplit().getPaths().get(0) );
        offset.add( validBlock.getFileSplit().getStart() );
        length.add( validBlock.getFileSplit().getLength() );
        startChunk.add( validBlock.getFileSplit().startChunk.get(0) );
        endChunk.add( validBlock.getFileSplit().endChunk.get(0) );
        timeStartLimit.add(validBlock.getFileSplit().startChunk.get(0));
        timeEndLimit.add(validBlock.getFileSplit().endChunk.get(0));
        latStartLimit.add(validBlock.getFileSplit().startChunk.get(0));
        latEndLimit.add(validBlock.getFileSplit().endChunk.get(0));
        lonStartLimit.add(validBlock.getFileSplit().startChunk.get(0));
        lonEndLimit.add(validBlock.getFileSplit().endChunk.get(0));

        NetCDFFileSplit thissplit = new NetCDFFileSplit( fl, offset,
                length, locations, startChunk, endChunk, timeStartLimit, timeEndLimit,
                latStartLimit, latEndLimit, lonStartLimit, lonEndLimit);
        splitList.add( thissplit );
    }



    @Override
    public RecordReader<Text, NetCDFArrayWritable> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        //LOG.info( "[SAMAN] return getRecordReader" );
        //System.out.println( "[SAMAN] return getRecordReader" );
        return new NetCDFReaderWithMetaPartToMemoryMultiSplit(job, (NetCDFFileSplit) genericSplit);
    }


}
