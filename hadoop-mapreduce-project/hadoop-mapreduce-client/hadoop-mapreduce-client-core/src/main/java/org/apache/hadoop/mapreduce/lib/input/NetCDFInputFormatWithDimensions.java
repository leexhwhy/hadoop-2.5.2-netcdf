package org.apache.hadoop.mapreduce.lib.input;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NetCDFArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.NetCDFInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.NetworkTopology;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * Treats keys as offset in file and value as line.
 */
public class NetCDFInputFormatWithDimensions extends FileInputFormat<Text, NetCDFArrayWritable> {

    private static final Log LOG
            = LogFactory.getLog(NetCDFInputFormat.class.getName());


    private NetCDFInfo getNetCDFInfo(Path file, FileSystem fs)
    {

        //traverse header and return chunk start and size arrays
        NetCDFInfo result = new NetCDFInfo();//library call
        System.out.println( "[SAMAN][NetCDFInputFormat][getNetCDFInfo] after NetCDFInfo!" );

        NetcdfFile ncFile;
        Variable v;
        ncFile = null;
        try {
            ncFile = NetcdfDataset.openFile(file.toString(), null);
            System.out.println("[SAMAN][NetCDFInputFormat][getNetCDFInfo] after opening the file!");

            v = ncFile.findVariable("rsut");
            //List<Variable> vs = ncFile.getVariables();
            //v = vs.get(vs.size()-1);
            System.out.println( "[SAMAN][NetCDFInputFormat][getNetCDFInfo] after finding a variable!" );

            //LOG.info("Variable is "+ v.getFullName());
            result.fileSize = ncFile.vfileSize;
            result.recStart = ncFile.vrecStart;
            Long[] metaArray = v.reallyReadMeta().toArray(new Long[(int)(ncFile.vnumRecs)]);
            System.out.println( "[SAMAN][NetCDFInputFormat][getNetCDFInfo] reading meta!" );
            result.chunkStarts =ArrayUtils.toPrimitive(metaArray);
            System.out.println( "[SAMAN][NetCDFInputFormat][getNetCDFInfo] after reading meta!" );
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

        System.out.println( "[SAMAN][NetCDFInputFormat][getNetCDFInfo] getNetCDFInfo done!" );

        return result;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job)
            throws IOException {
        List<FileStatus> files = listStatus(job);

        System.out.println( "[SAMAN][NetCDFInputFormatWithDimensions] Beginning of getSplits!" );

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

            System.out.println( "[SAMAN][NetCDFInputFormat][GetSplits]  path is = " + file.getPath().getName() );

            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            long length = file.getLen();
            //LOG.info("get file len of "+file.getPath());
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(job, path)) {
                long blockSize = file.getBlockSize();
                System.out.println( "[SAMAN][NetCDFInputFormat][GetSplits] NetCDFInfo!" );
                NetCDFInfo netInfo = getNetCDFInfo(path, fs);
                System.out.println( "[SAMAN][NetCDFInputFormat][GetSplits] After NetCDFInfo!" );
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
                // Added by Saman
                System.out.println( "[SAMAN][NetCDFInputFormat][GetSplits] before prune blocks!" );
                int pruneBlocks = job.getConfiguration().getInt("hadoop.netcdf.pruneblocks", 0);
                //System.out.println( "[SAMAN] NetCDFInputFormat.getSplits, prubeBlocks="+pruneBlocks );
                //LOG.info( "[SAMAN] NetCDFInputFormat.getSplits => recStart = " + recStart + ", chunkStarts = " + chunkStarts +
                //        ", smallSize = " + smallSize + ", recSize = " + recSize + ", bytesRemaining = " + bytesRemaining +
                //        ", thisStart = " + thisStart);
                //System.out.println( "[SAMAN] NetCDFInputFormat.getSplits => recStart = " + recStart + ", chunkStarts = " + chunkStarts +
                //        ", smallSize = " + smallSize + ", recSize = " + recSize + ", bytesRemaining = " + bytesRemaining +
                //        ", thisStart = " + thisStart);
                int count = 0;
                while ( bytesRemaining > 0) {
                    System.out.println( "[SAMAN][NetCDFInputFormatWithDimensions][getSplits] in while!" );
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
                    //LOG.info( "[SAMAN] NetCDFInputFormat.getSplits => splitSize="+splitSize+", thisStart="+thisStart+
                    //        ", endChunk="+endChunk+", blockNo="+blockNo);
                    //System.out.println( "[SAMAN] NetCDFInputFormat.getSplits => splitSize="+splitSize+", thisStart="+thisStart+
                    //        ", endChunk="+endChunk+", blockNo="+blockNo);

                    int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                    FileSplit split = new FileSplit(path, tempStart, splitSize, blkLocations[blkIndex].getHosts());
                    split.startChunk = thisChunk;
                    split.endChunk = endChunk;
                    System.out.println( "[SAMAN] NetCDFInputFormat.getSplits => split.startChunk="+thisChunk+", split.endChunk="+endChunk );
                    if( pruneBlocks == 1 ) {
                        if (count == 0)
                            splits.add(split);
                    }else{
                        splits.add(split);
                    }
                    bytesRemaining -= splitSize;
                    //LOG.info("[net] split " +path+" remaining "+bytesRemaining);
                    thisChunk = endChunk;
                    //LOG.info( "[SAMAN] NetCDFInputFormat.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
                    //System.out.println( "[SAMAN] NetCDFInputFormat.getSplits => bytesRemaining="+bytesRemaining+", thisChunk="+thisChunk );
                    count++;
                }

            } else if (length != 0) {
                splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
            } else {
                //Create empty hosts array for zero length files
                splits.add(new FileSplit(path, 0, length, new String[0]));
            }
        }


        System.out.println("Total # of splits: " + splits.size());
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

        return splits;

    }

    @Override
    public RecordReader<Text, NetCDFArrayWritable> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {

        return new NetCDFReaderWithDimensions();
    }


}
