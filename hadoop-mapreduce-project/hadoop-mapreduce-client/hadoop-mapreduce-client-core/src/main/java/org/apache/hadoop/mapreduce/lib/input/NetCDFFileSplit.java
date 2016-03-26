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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NetCDFFileSplit extends FileSplit implements Writable {
    private List<Path> file = new LinkedList<Path>();
    private List<Long> start = new LinkedList<Long>();
    private List<Long> length = new LinkedList<Long>();
    private String[] hosts;
    private SplitLocationInfo[] hostInfos;
    public List<Long> startChunk = new LinkedList<Long>();
    public List<Long> endChunk = new LinkedList<Long>();

    public List<Long> secondDimStartChunk = new LinkedList<Long>();
    public List<Long> secondDimEndChunk = new LinkedList<Long>();

    public List<Long> timeStartLimit = new LinkedList<Long>();
    public List<Long> timeEndLimit = new LinkedList<Long>();
    public List<Long> latStartLimit = new LinkedList<Long>();
    public List<Long> latEndLimit = new LinkedList<Long>();
    public List<Long> lonStartLimit = new LinkedList<Long>();
    public List<Long> lonEndLimit = new LinkedList<Long>();

    public NetCDFFileSplit() {}

    /** Constructs a split with host information
     *
     * @param file the file name
     * @param start the position of the first byte in the file to process
     * @param length the number of bytes in the file to process
     * @param hosts the list of hosts containing the block, possibly null
     */
    public NetCDFFileSplit(Path file, long start, long length, String[] hosts) {
        this.file.add(file);
        this.start.add(start);
        this.length.add(length);
        this.hosts = hosts;
    }

    public NetCDFFileSplit( List<Path> paths, List<Long> starts, List<Long> lengths, String[] hosts ){
        this.file = paths;
        this.start = starts;
        this.length = lengths;
        this.hosts = hosts;
    }

    public NetCDFFileSplit( List<Path> paths, List<Long> starts, List<Long> lengths, String[] hosts,
                            List<Long> startChunks, List<Long> endChunks){
        this.file = paths;
        this.start = starts;
        this.length = lengths;
        this.hosts = hosts;
        this.startChunk = startChunks;
        this.endChunk = endChunks;
    }

    public NetCDFFileSplit( List<Path> paths, List<Long> starts, List<Long> lengths, String[] hosts,
                            List<Long> startChunks, List<Long> endChunks, List<Long> timeStartLimit,
                            List<Long> timeEndLimit, List<Long> latStartLimit, List<Long> latEndLimit,
                            List<Long> lonStartLimit, List<Long> lonEndLimit){

        this.file = paths;
        this.start = starts;
        this.length = lengths;
        this.hosts = hosts;
        this.startChunk = startChunks;
        this.endChunk = endChunks;
        this.timeStartLimit = timeStartLimit;
        this.timeEndLimit = timeEndLimit;
        this.latStartLimit = latStartLimit;
        this.latEndLimit = latEndLimit;
        this.lonStartLimit = lonStartLimit;
        this.lonEndLimit = lonEndLimit;

    }

    public NetCDFFileSplit( List<Path> paths, List<Long> starts, List<Long> lengths, String[] hosts,
                            List<Long> startChunks, List<Long> endChunks,
                            List<Long> secondDimStartChunks, List<Long> secondDimEndChunks){
        this.file = paths;
        this.start = starts;
        this.length = lengths;
        this.hosts = hosts;
        this.startChunk = startChunks;
        this.endChunk = endChunks;
        this.secondDimEndChunk = secondDimEndChunks;
        this.secondDimStartChunk = secondDimStartChunks;
    }

    /** Constructs a split with host and cached-blocks information
     *
     * @param file the file name
     * @param start the position of the first byte in the file to process
     * @param length the number of bytes in the file to process
     * @param hosts the list of hosts containing the block
     * @param inMemoryHosts the list of hosts containing the block in memory
     */
    public NetCDFFileSplit(Path file, long start, long length, String[] hosts,
                     String[] inMemoryHosts) {
        this(file, start, length, hosts);
        hostInfos = new SplitLocationInfo[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            // because N will be tiny, scanning is probably faster than a HashSet
            boolean inMemory = false;
            for (String inMemoryHost : inMemoryHosts) {
                if (inMemoryHost.equals(hosts[i])) {
                    inMemory = true;
                    break;
                }
            }
            hostInfos[i] = new SplitLocationInfo(hosts[i], inMemory);
        }
    }

    /** files containing this split's data. */
    public Path getPath() { return file.get(0); }

    public List<Path> getPaths() { return file; }

    /** The position of the first byte in the file to process. */
    public long getStart() { return start.get(0); }

    /** The number of bytes in the file to process. */
    @Override
    public long getLength() {
        long totalLength = 0;
        Iterator itr = length.iterator();
        while( itr.hasNext() ){
            totalLength += (Long)itr.next();
        }

        return totalLength;
    }

    @Override
    public String toString() { return file + ":" + start + "+" + length; }

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////

    @Override
    public void write(DataOutput out) throws IOException {

        //System.out.println( "[SAMAN][NetCDFFileSplit][write] calling write!" );

        int numberOfPaths = file.size();
        out.writeInt( numberOfPaths );
        for( int i = 0; i < numberOfPaths; i++ )
            Text.writeString(out, file.get(i).toString());

        int numberOfStarts = start.size();
        out.writeInt( numberOfStarts );
        for( int i = 0; i < numberOfStarts; i++ )
            out.writeLong( start.get(i) );

        int numberOfLengths = length.size();
        out.writeInt( numberOfLengths );
        for( int i = 0; i < numberOfLengths; i++ )
            out.writeLong( length.get(i) );

        int numberOfStartChunks = startChunk.size();
        out.writeInt( numberOfStartChunks );
        for( int i = 0; i < numberOfStartChunks; i++ )
            out.writeLong( startChunk.get(i) );

        int numberOfEndChunks = endChunk.size();
        out.writeInt( numberOfEndChunks );
        for( int i = 0; i < numberOfEndChunks; i++ )
            out.writeLong( endChunk.get(i) );

        int numberOfSecondDimStartChunks = secondDimStartChunk.size();
        out.writeInt( numberOfSecondDimStartChunks );
        for( int i = 0; i < numberOfSecondDimStartChunks; i++ ){
            out.writeLong( secondDimStartChunk.get(i) );
        }

        int numberOfSecondDimEndChunks = secondDimEndChunk.size();
        out.writeInt( numberOfSecondDimEndChunks );
        for( int i = 0; i < numberOfSecondDimEndChunks; i++ ){
            out.writeLong( secondDimEndChunk.get(i) );
        }

        int numberOfTimeStartLimit = timeStartLimit.size();
        out.writeInt( numberOfTimeStartLimit );
        for( int i = 0; i < numberOfTimeStartLimit; i++ ){
            out.writeLong( timeStartLimit.get(i) );
        }

        int numberOfTimeEndLimit = timeEndLimit.size();
        out.writeInt(numberOfTimeEndLimit);
        for( int i = 0; i < numberOfTimeEndLimit; i++ ){
            out.writeLong(timeEndLimit.get(i));
        }

        int numberOfLatStartLimit = latStartLimit.size();
        out.writeInt(numberOfLatStartLimit);
        for( int i = 0; i < numberOfLatStartLimit; i++ ){
            out.writeLong(latStartLimit.get(i));
        }

        int numberOfLatEndLimit = latEndLimit.size();
        out.writeInt(numberOfLatEndLimit);
        for( int i = 0; i < numberOfLatEndLimit; i++ ){
            out.writeLong(latEndLimit.get(i));
        }

        int numberOfLonStartLimit = lonStartLimit.size();
        out.writeInt(numberOfLonStartLimit);
        for( int i = 0; i < numberOfLonStartLimit; i++ ){
            out.writeLong(lonStartLimit.get(i));
        }

        int numberOfLonEndLimit = lonEndLimit.size();
        out.writeInt( numberOfLonEndLimit );
        for( int i = 0; i < numberOfLonEndLimit; i++ ){
            out.writeLong(lonEndLimit.get(i));
        }

        //Text.writeString(out, file.toString());
        //out.writeLong(start.get(0));
        //out.writeLong(length.get(0));
        //out.writeLong(startChunk);
        //out.writeLong(endChunk);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //file = new Path(Text.readString(in));
        //start = in.readLong();
        //length = in.readLong();
        //startChunk = in.readLong();
        //endChunk = in.readLong();

        //System.out.println( "[SAMAN][NetCDFFileSplit][readFields] calling readFields!" );

        int numberOfPaths = in.readInt();
        file = new LinkedList<Path>();
        for( int i = 0; i < numberOfPaths; i++ )
            file.add( new Path(Text.readString(in)) );

        int numberOfStarts = in.readInt();
        start = new LinkedList<Long>();
        for( int i = 0; i < numberOfStarts; i++ )
            start.add( in.readLong() );

        int numberOfLengths = in.readInt();
        length = new LinkedList<Long>();
        for( int i = 0; i < numberOfLengths; i++ )
            length.add( in.readLong() );

        int numberOfStartChunks = in.readInt();
        startChunk = new LinkedList<Long>();
        for( int i = 0; i < numberOfStartChunks; i++ )
            startChunk.add( in.readLong() );

        int numberOfEndChunks = in.readInt();
        endChunk = new LinkedList<Long>();
        for( int i = 0; i < numberOfEndChunks; i++ )
            endChunk.add( in.readLong() );

        int numberOfSecondDimStartChunks = in.readInt();
        secondDimStartChunk = new LinkedList<Long>();
        for( int i = 0; i < numberOfSecondDimStartChunks; i++ )
            secondDimStartChunk.add( in.readLong() );

        int numberOfSecondDimEndChunks = in.readInt();
        for( int i = 0; i < numberOfSecondDimEndChunks; i++ )
            secondDimEndChunk.add( in.readLong() );

        int numberOfTimeStartLimit = in.readInt();
        for( int i = 0; i < numberOfTimeStartLimit; i++ ){
            timeStartLimit.add( in.readLong() );
        }

        int numberOfTimeEndLimit = in.readInt();
        for( int i = 0; i < numberOfTimeEndLimit; i++ ){
            timeEndLimit.add( in.readLong() );
        }

        int numberOfLatStartLimit = in.readInt();
        for( int i = 0; i < numberOfLatStartLimit; i++ ){
            latStartLimit.add( in.readLong() );
        }

        int numberOfLatEndLimit = in.readInt();
        for( int i = 0; i < numberOfLatEndLimit; i++ ){
            latEndLimit.add( in.readLong() );
        }

        int numberOfLonStartLimit = in.readInt();
        for( int i = 0; i < numberOfLonStartLimit; i++ ){
            lonStartLimit.add( in.readLong() );
        }

        int numberOfLonEndLimit = in.readInt();
        for( int i = 0; i < numberOfLonEndLimit; i++ ){
            lonEndLimit.add(in.readLong());
        }

        hosts = null;
    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[]{};
        } else {
            return this.hosts;
        }
    }

    @Override
    @Evolving
    public SplitLocationInfo[] getLocationInfo() throws IOException {
        return hostInfos;
    }
}
