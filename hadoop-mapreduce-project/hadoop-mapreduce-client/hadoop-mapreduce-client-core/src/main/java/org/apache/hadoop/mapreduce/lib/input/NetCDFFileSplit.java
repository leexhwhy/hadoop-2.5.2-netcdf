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
public class NetCDFFileSplit extends InputSplit implements Writable {
    private List<Path> file = new LinkedList<Path>();
    private List<Long> start = new LinkedList<Long>();
    private List<Long> length = new LinkedList<Long>();
    private String[] hosts;
    private SplitLocationInfo[] hostInfos;
    public List<Long> startChunk = new LinkedList<Long>();
    public List<Long> endChunk = new LinkedList<Long>();
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
    public List<Path> getPath() { return file; }

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
        Text.writeString(out, file.toString());
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