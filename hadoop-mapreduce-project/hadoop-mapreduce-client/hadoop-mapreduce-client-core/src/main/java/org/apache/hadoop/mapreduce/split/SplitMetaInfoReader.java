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

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * A utility that reads the split meta info and creates
 * split meta info objects
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SplitMetaInfoReader {
  
  public static JobSplit.TaskSplitMetaInfo[] readSplitMetaInfo(
      JobID jobId, FileSystem fs, Configuration conf, Path jobSubmitDir) 
  throws IOException {
    boolean isNetCDF = conf.getBoolean(MRJobConfig.MR_NETCDF_ISNETCDF,
            MRJobConfig.MR_NETCDF_ISNETCDF_VALUE);
    boolean bestLayoutEnabled = conf.getBoolean(MRJobConfig.MR_NETCDF_BEST_LAYOUT_ENABLED,
            MRJobConfig.MR_NETCDF_BEST_LAYOUT_ENABLED_VALUE);
    boolean bestFetchLayoutEnabled = conf.getBoolean(MRJobConfig.MR_NETCDF_BESTFETCH_LAYOUT_ENABLED,
            MRJobConfig.MR_NETCDF_BESTFETCH_LAYOUT_ENABLED_VALUE);

    long maxMetaInfoSize = conf.getLong(MRJobConfig.SPLIT_METAINFO_MAXSIZE,
        MRJobConfig.DEFAULT_SPLIT_METAINFO_MAXSIZE);
    Path metaSplitFile = JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir);
    String jobSplitFile = JobSubmissionFiles.getJobSplitFile(jobSubmitDir).toString();
    FileStatus fStatus = fs.getFileStatus(metaSplitFile);
    if (maxMetaInfoSize > 0 && fStatus.getLen() > maxMetaInfoSize) {
      throw new IOException("Split metadata size exceeded " +
          maxMetaInfoSize +". Aborting job " + jobId);
    }
    FSDataInputStream in = fs.open(metaSplitFile);
    byte[] header = new byte[JobSplit.META_SPLIT_FILE_HEADER.length];
    in.readFully(header);
    if (!Arrays.equals(JobSplit.META_SPLIT_FILE_HEADER, header)) {
      throw new IOException("Invalid header on split file");
    }
    int vers = WritableUtils.readVInt(in);
    if (vers != JobSplit.META_SPLIT_VERSION) {
      in.close();
      throw new IOException("Unsupported split version " + vers);
    }
    int numSplits = WritableUtils.readVInt(in); //TODO: check for insane values
    JobSplit.TaskSplitMetaInfo[] allSplitMetaInfo = 
      new JobSplit.TaskSplitMetaInfo[numSplits];

    for (int i = 0; i < numSplits; i++) {
      JobSplit.SplitMetaInfo splitMetaInfo = new JobSplit.SplitMetaInfo();
      splitMetaInfo.readFields(in);
      JobSplit.TaskSplitIndex splitIndex = new JobSplit.TaskSplitIndex(
          jobSplitFile, 
          splitMetaInfo.getStartOffset());

      // Here we would modify the target nodes
      // We would force the task to be deployed on
      // a remote read and would see the result
      // We would consider how many tasks to be
      // read forcefully from a remote node
      // We would consider values 1/4, 1/2, and full

      if( bestLayoutEnabled && isNetCDF ){
        System.out.println( "[SAMAN][SplitMetaInfoReader][readSplitMetaInfo] bestLayoutEnabled && isNetCDF" );
        String[] locations = new String[1];
        //locations[0] = getFakeNode(splitMetaInfo.getLocations(), i);
        locations[0] = splitMetaInfo.getLocations()[0];
        allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex,
                locations, splitMetaInfo.getInputDataLength());
      }else if( bestFetchLayoutEnabled && isNetCDF ){
        System.out.println( "[SAMAN][SplitMetaInfoReader][readSplitMetaInfo] bestFetchLayoutEnabled && isNetCDF" );
        String[] locations = new String[1];
        //locations[0] = getFakeNode(splitMetaInfo.getLocations(), i);
        locations[0] = splitMetaInfo.getLocations()[0];
        allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex,
                locations, splitMetaInfo.getInputDataLength());
      }else {
        System.out.println( "[SAMAN][SplitMetaInfoReader][readSplitMetaInfo] else!" );
        allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex,
                splitMetaInfo.getLocations(),
                splitMetaInfo.getInputDataLength());
      }
    }
    in.close();
    return allSplitMetaInfo;
  }

  private static String getFakeNode( String[] trueLocations, int splitNumber ){

    String trueLocationsString = new String();
    for( int i = 0; i < trueLocations.length; i++ ){
      trueLocationsString += trueLocations[i];
    }
    System.out.println( "[SAMAN][SplitMetaInfoReader][getFakeNodes] trueLocationsString = " + trueLocationsString );
    int divider = 1;
    if( splitNumber%divider != 0 )
      return trueLocations[0];

    // We consider trueLocations to have 3 nodes
    String[] allNodes = new String[8];
    allNodes[0] = "c3n3"; allNodes[1] = "c3n4"; allNodes[2] = "c3n5"; allNodes[3] = "c3n6";
    allNodes[4] = "c3n7"; allNodes[5] = "c3n8"; allNodes[6] = "c3n9"; allNodes[7] = "c3n10";

    String[] remoteNodes = new String[8-trueLocations.length];
    for( int i = 0; i < allNodes.length; i++ ) {
      for (int j = 0; j < trueLocations.length; j++) {
        if (allNodes[i].equals(trueLocations[j])) {
          allNodes[i] = "";
          break;
        }
      }
    }

    int counter = 0;
    for( int i = 0; i < allNodes.length; i++ ){
      if( !allNodes[i].equals("") ){
        remoteNodes[counter] = allNodes[i];
        counter++;
      }
    }

    int random = ThreadLocalRandom.current().nextInt(0, 8-trueLocations.length);
    System.out.println( "[SAMAN][SplitMetaInfoReader][getFakeNode] random remote node = " + remoteNodes[random] );
    return remoteNodes[random];

  }

}
