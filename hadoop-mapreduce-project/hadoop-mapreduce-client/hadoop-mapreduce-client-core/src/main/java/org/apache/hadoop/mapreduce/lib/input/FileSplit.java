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
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileSplit extends InputSplit implements Writable {
  private Path file;
  private long start;
  private long length;
  private String[] hosts;
  private SplitLocationInfo[] hostInfos;
  public long startChunk;
  public long endChunk;
  public long timeStartLimit;
  public long timeEndLimit;
  public long latStartLimit;
  public long latEndLimit;
  public long lonStartLimit;
  public long lonEndLimit;
  //public long secondStartChunk;
  //public long secondEndChunk;
  //public long thirdStartChunk;
  //public long thirdEndChunk;
  public FileSplit() {}

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
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
 public FileSplit(Path file, long start, long length, String[] hosts,
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
 
  /** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }
  
  /** The number of bytes in the file to process. */
  @Override
  public long getLength() { return length; }

  @Override
  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
    out.writeLong(startChunk);
    out.writeLong(endChunk);
    out.writeLong(timeStartLimit);
    out.writeLong(timeEndLimit);
    out.writeLong(latStartLimit);
    out.writeLong(latEndLimit);
    out.writeLong(lonStartLimit);
    out.writeLong(lonEndLimit);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in));
    start = in.readLong();
    length = in.readLong();
    startChunk = in.readLong();
    endChunk = in.readLong();
    timeStartLimit = in.readLong();
    timeEndLimit = in.readLong();
    latStartLimit = in.readLong();
    latEndLimit = in.readLong();
    lonStartLimit = in.readLong();
    lonEndLimit = in.readLong();
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
