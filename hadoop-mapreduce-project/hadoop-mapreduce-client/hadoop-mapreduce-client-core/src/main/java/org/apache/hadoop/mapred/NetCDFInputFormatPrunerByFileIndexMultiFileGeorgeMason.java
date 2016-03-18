package org.apache.hadoop.mapred;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NetCDFArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetworkTopology;

import java.io.IOException;
import java.util.*;

/**
 * Created by saman on 3/17/16.
 */
public class NetCDFInputFormatPrunerByFileIndexMultiFileGeorgeMason extends FileInputFormat<Text, NetCDFArrayWritable>{

    private static final Log LOG
            = LogFactory.getLog(NetCDFInputFormatPrunerByFileIndex.class.getName());

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
            ncFile = NetcdfDataset.openFile(file.toString(), null);

            v = ncFile.findVariable("rsut");
            time = ncFile.findVariable("time");
            lat = ncFile.findVariable("lat");
            lon = ncFile.findVariable("lon");

            result.fileSize = ncFile.vfileSize;
            result.recStart = ncFile.vrecStart;
            Long[] metaArray = v.reallyReadMeta().toArray(new Long[(int)(ncFile.vnumRecs)]);
            result.chunkStarts = ArrayUtils.toPrimitive(metaArray);
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

        System.out.println( "[SAMANPruner] beginning of getSplits" );
        job.setLong(NUM_INPUT_FILES, files.length);
        long totalSize = 0;                           // compute total size
        for (FileStatus file: files) {                // check we have valid files
            if (file.isDir()) {
                throw new IOException("Not a file: " + file.getPath());
            }
            totalSize += file.getLen();
        }

        // generate splits
        ArrayList<NetCDFFileSplit> splits = new ArrayList<NetCDFFileSplit>(numSplits);
        ArrayList<NetCDFFileSplit> finalSplits = new ArrayList<NetCDFFileSplit>();
        NetworkTopology clusterMap = new NetworkTopology();
        for (FileStatus file: files) {
            Path path = file.getPath();
            int fileIndex = 0;
            int dimIndex = 0;
            if( queryType == QueryType.TIME || queryType == QueryType.NOLIMIT){
                if( path.getName().contains("lat") || path.getName().contains("lon") )
                    continue;
            }else if( queryType == QueryType.LAT ){
                if( !path.getName().contains("lat") )
                    continue;
            }else if( queryType == QueryType.LON ){
                if( !path.getName().contains("lon") )
                    continue;
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

            System.out.println("[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] File name is : " + path.getName());
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

                while ( bytesRemaining > 0) {
                    while ( chunkIndex < chunkStarts.length && chunkStarts[chunkIndex] < blockNo * blockSize ) {
                        chunkIndex++;
                        // This would break here to return only one time variable
                        break;
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

                    String[] splitHosts = getSplitHosts(blkLocations, tempStart, splitSize, clusterMap);
                    NetCDFFileSplit split     = new NetCDFFileSplit(path, tempStart, splitSize, splitHosts);

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
                    } else if( queryType == QueryType.LAT || queryType == QueryType.LON ){
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

                        // Put the block into the block to node set
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
                            split.getFileSplit().startChunk.add((long)bottomLimit);
                        }else{
                            split.getFileSplit().startChunk.add(thisChunk);
                        }
                        if( topLimit < endChunk && (topLimit != -1) ){
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

                    splits.add(split);

                    bytesRemaining -= splitSize;
                    thisChunk = endChunk;
                }

            } else if (length != 0) {
                String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
            } else {
            }
        }

        System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndex][getSplits] total number of splits are: " + splits.size() );

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

                // Remove all blocks which may already have been assigned to other
                // splits.
                if(!blockToNodes.containsKey(oneblock)) {
                    oneBlockIter.remove();
                    continue;
                }

                validBlocks.add(oneblock);
                if( queryType == QueryType.LAT ){
                    curSplitSize += (oneblock.getFileSplit().endChunk.get(0) - oneblock.getFileSplit().startChunk.get(0)) * 4 * netInfo.lonLength * netInfo.timeLength;
                }else if( queryType == QueryType.LON ){
                    curSplitSize += (oneblock.getFileSplit().endChunk.get(0) - oneblock.getFileSplit().startChunk.get(0)) * 4 * netInfo.latLength * netInfo.timeLength;
                }else if( queryType == QueryType.TIME ){
                    curSplitSize += (oneblock.getFileSplit().endChunk.get(0) - oneblock.getFileSplit().startChunk.get(0)) * 4 * netInfo.latLength * netInfo.lonLength;
                }else{
                    curSplitSize += (oneblock.getFileSplit().endChunk.get(0) - oneblock.getFileSplit().startChunk.get(0)) * 4 * netInfo.latLength * netInfo.lonLength;
                }
                blockToNodes.remove(oneblock);

                // if the accumulated split size exceeds the maximum, then
                // create this split.
                if (blockSize != 0 && curSplitSize >= blockSize) {
                    // create an input split and add it to the splits array
                    addCreatedSplit(finalSplits, Collections.singleton(node), validBlocks);

                    curSplitSize = 0;
                    splitsPerNode.add(node);

                    validBlocks.clear();

                }

            }
            if( !validBlocks.isEmpty() ){
//                System.out.println( "[SAMAN][NetCDFInputFormatPrunerByFileIndexMultiFile][getSplits] validBlocks not empty!" );
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

        for (int i = 0; i < validBlocks.size(); i++) {
            fl.add(validBlocks.get(i).getFileSplit().getPaths().get(0));
            offset.add(validBlocks.get(i).getFileSplit().getStart());
            length.add(validBlocks.get(i).getFileSplit().getLength());
            startChunk.add(validBlocks.get(i).getFileSplit().startChunk.get(0));
            endChunk.add(validBlocks.get(i).getFileSplit().endChunk.get(0));
        }

        // add this split to the list that is returned
        NetCDFFileSplit thissplit = new NetCDFFileSplit(fl, offset,
                length, locations.toArray(new String[0]), startChunk, endChunk);
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

        fl.add( validBlock.getFileSplit().getPaths().get(0) );
        offset.add( validBlock.getFileSplit().getStart() );
        length.add( validBlock.getFileSplit().getLength() );
        startChunk.add( validBlock.getFileSplit().startChunk.get(0) );
        endChunk.add( validBlock.getFileSplit().endChunk.get(0) );

        NetCDFFileSplit thissplit = new NetCDFFileSplit( fl, offset,
                length, locations, startChunk, endChunk );
        splitList.add( thissplit );
    }



    @Override
    public RecordReader<Text, NetCDFArrayWritable> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        return new NetCDFReaderWithMetaMultiFile(job, (NetCDFFileSplit) genericSplit);
    }


}
