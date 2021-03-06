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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NetCDFArrayWritable;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import ucar.ma2.*;
import ucar.nc2.*;

import com.google.common.annotations.VisibleForTesting;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class BlockReceiver implements Closeable {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  @VisibleForTesting
  static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;
  private final long datanodeSlowLogThresholdMs;
  private DataInputStream in = null; // from where data are read
  private DataChecksum clientChecksum; // checksum used by client
  private DataChecksum diskChecksum; // checksum we write to disk
  
  /**
   * In the case that the client is writing with a different
   * checksum polynomial than the block is stored with on disk,
   * the DataNode needs to recalculate checksums before writing.
   */
  private boolean needsChecksumTranslation;
  private OutputStream out = null; // to block file at local disk
  private FileDescriptor outFd;
  private DataOutputStream checksumOut = null; // to crc file at local disk
  private int bytesPerChecksum;
  private int checksumSize;
  
  private final PacketReceiver packetReceiver = new PacketReceiver(false);
  
  protected final String inAddr;
  protected final String myAddr;
  private String mirrorAddr;
  private DataOutputStream mirrorOut;
  private Daemon responder = null;
  private DataTransferThrottler throttler;
  private ReplicaOutputStreams streams;
  private DatanodeInfo srcDataNode = null;
  private Checksum partialCrc = null;
  private final DataNode datanode;
  volatile private boolean mirrorError;

  // Cache management state
  private boolean dropCacheBehindWrites;
  private long lastCacheManagementOffset = 0;
  private boolean syncBehindWrites;
  private boolean syncBehindWritesInBackground;

  /** The client name.  It is empty if a datanode is the client */
  private final String clientname;
  private final boolean isClient; 
  private final boolean isDatanode;

  /** the block to receive */
  private final ExtendedBlock block; 
  /** the replica to write */
  private final ReplicaInPipelineInterface replicaInfo;
  /** pipeline stage */
  private final BlockConstructionStage stage;
  private final boolean isTransfer;

  private boolean syncOnClose;
  private long restartBudget;

  BlockReceiver(final ExtendedBlock block, final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage, 
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd, 
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode datanode, DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy) throws IOException {
    try{
      this.block = block;
      this.in = in;
      this.inAddr = inAddr;
      this.myAddr = myAddr;
      this.srcDataNode = srcDataNode;
      this.datanode = datanode;

      this.clientname = clientname;
      this.isDatanode = clientname.length() == 0;
      this.isClient = !this.isDatanode;
      this.restartBudget = datanode.getDnConf().restartReplicaExpiry;
      this.datanodeSlowLogThresholdMs = datanode.getDnConf().datanodeSlowIoWarningThresholdMs;
      //for datanode, we have
      //1: clientName.length() == 0, and
      //2: stage == null or PIPELINE_SETUP_CREATE
      this.stage = stage;
      this.isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED;

      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + ": " + block
            + "\n  isClient  =" + isClient + ", clientname=" + clientname
            + "\n  isDatanode=" + isDatanode + ", srcDataNode=" + srcDataNode
            + "\n  inAddr=" + inAddr + ", myAddr=" + myAddr
            + "\n  cachingStrategy = " + cachingStrategy
            );
      }

      //
      // Open local disk out
      //
      if (isDatanode) { //replication or move
        replicaInfo = datanode.data.createTemporary(block);
      } else {
        switch (stage) {
        case PIPELINE_SETUP_CREATE:
          replicaInfo = datanode.data.createRbw(block);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaInfo.getStorageUuid());
          break;
        case PIPELINE_SETUP_STREAMING_RECOVERY:
          replicaInfo = datanode.data.recoverRbw(
              block, newGs, minBytesRcvd, maxBytesRcvd);
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND:
          replicaInfo = datanode.data.append(block, newGs, minBytesRcvd);
          if (datanode.blockScanner != null) { // remove from block scanner
            datanode.blockScanner.deleteBlock(block.getBlockPoolId(),
                block.getLocalBlock());
          }
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaInfo.getStorageUuid());
          break;
        case PIPELINE_SETUP_APPEND_RECOVERY:
          replicaInfo = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
          if (datanode.blockScanner != null) { // remove from block scanner
            datanode.blockScanner.deleteBlock(block.getBlockPoolId(),
                block.getLocalBlock());
          }
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaInfo.getStorageUuid());
          break;
        case TRANSFER_RBW:
        case TRANSFER_FINALIZED:
          // this is a transfer destination
          replicaInfo = datanode.data.createTemporary(block);
          break;
        default: throw new IOException("Unsupported stage " + stage + 
              " while receiving block " + block + " from " + inAddr);
        }
      }
      this.dropCacheBehindWrites = (cachingStrategy.getDropBehind() == null) ?
        datanode.getDnConf().dropCacheBehindWrites :
          cachingStrategy.getDropBehind();
      this.syncBehindWrites = datanode.getDnConf().syncBehindWrites;
      this.syncBehindWritesInBackground = datanode.getDnConf().
          syncBehindWritesInBackground;
      
      final boolean isCreate = isDatanode || isTransfer 
          || stage == BlockConstructionStage.PIPELINE_SETUP_CREATE;
      streams = replicaInfo.createStreams(isCreate, requestedChecksum);
      assert streams != null : "null streams!";

      // read checksum meta information
      this.clientChecksum = requestedChecksum;
      this.diskChecksum = streams.getChecksum();
      this.needsChecksumTranslation = !clientChecksum.equals(diskChecksum);
      this.bytesPerChecksum = diskChecksum.getBytesPerChecksum();
      this.checksumSize = diskChecksum.getChecksumSize();

      this.out = streams.getDataOut();
      if (out instanceof FileOutputStream) {
        this.outFd = ((FileOutputStream)out).getFD();
      } else {
        LOG.warn("Could not get file descriptor for outputstream of class " +
            out.getClass());
      }
      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), HdfsConstants.SMALL_BUFFER_SIZE));
      // write data chunk header if creating a new replica
      if (isCreate) {
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);
      } 
    } catch (ReplicaAlreadyExistsException bae) {
      throw bae;
    } catch (ReplicaNotFoundException bne) {
      throw bne;
    } catch(IOException ioe) {
      IOUtils.closeStream(this);
      cleanupBlock();
      
      // check if there is a disk error
      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
      DataNode.LOG.warn("IOException in BlockReceiver constructor. Cause is ",
          cause);
      
      if (cause != null) { // possible disk error
        ioe = cause;
        datanode.checkDiskError();
      }
      
      throw ioe;
    }
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}

  String getStorageUuid() {
    return replicaInfo.getStorageUuid();
  }

  /**
   * close files.
   */
  @Override
  public void close() throws IOException {
    packetReceiver.close();

    IOException ioe = null;
    if (syncOnClose && (out != null || checksumOut != null)) {
      datanode.metrics.incrFsyncCount();      
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;
    // close checksum file
    try {
      if (checksumOut != null) {
        long flushStartNanos = System.nanoTime();
        checksumOut.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncChecksumOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        checksumOut.close();
        checksumOut = null;
      }
    } catch(IOException e) {
      ioe = e;
    }
    finally {
      IOUtils.closeStream(checksumOut);
    }
    // close block file
    try {
      if (out != null) {
        long flushStartNanos = System.nanoTime();
        out.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncDataOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        out.close();
        out = null;
      }
    } catch (IOException e) {
      ioe = e;
    }
    finally{
      IOUtils.closeStream(out);
    }
    if (measuredFlushTime) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
    }
    // disk check
    if(ioe != null) {
      datanode.checkDiskError();
      throw ioe;
    }
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flushOrSync(boolean isSync) throws IOException {
    long flushTotalNanos = 0;
    long begin = Time.monotonicNow();
    if (checksumOut != null) {
      long flushStartNanos = System.nanoTime();
      checksumOut.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncChecksumOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (out != null) {
      long flushStartNanos = System.nanoTime();
      out.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncDataOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (checksumOut != null || out != null) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
      if (isSync) {
    	  datanode.metrics.incrFsyncCount();      
      }
    }
    long duration = Time.monotonicNow() - begin;
    if (duration > datanodeSlowLogThresholdMs) {
      LOG.warn("Slow flushOrSync took " + duration + "ms (threshold="
          + datanodeSlowLogThresholdMs + "ms), isSync:" + isSync + ", flushTotalNanos="
          + flushTotalNanos + "ns");
    }
  }

  /**
   * While writing to mirrorOut, failure to write to mirror should not
   * affect this datanode unless it is caused by interruption.
   */
  private void handleMirrorOutError(IOException ioe) throws IOException {
    String bpid = block.getBlockPoolId();
    LOG.info(datanode.getDNRegistrationForBP(bpid)
        + ":Exception writing " + block + " to mirror " + mirrorAddr, ioe);
    if (Thread.interrupted()) { // shut down if the thread is interrupted
      throw ioe;
    } else { // encounter an error while writing to mirror
      // continue to run even if can not write to mirror
      // notify client of the error
      // and wait for the client to shut down the pipeline
      mirrorError = true;
    }
  }
  
  /**
   * Verify multiple CRC chunks. 
   */
  private void verifyChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
      throws IOException {
    try {
      clientChecksum.verifyChunkedSums(dataBuf, checksumBuf, clientname, 0);
    } catch (ChecksumException ce) {
      LOG.warn("Checksum error in block " + block + " from " + inAddr, ce);
      // No need to report to namenode when client is writing.
      if (srcDataNode != null && isDatanode) {
        try {
          LOG.info("report corrupt " + block + " from datanode " +
                    srcDataNode + " to namenode");
          datanode.reportRemoteBadBlock(srcDataNode, block);
        } catch (IOException e) {
          LOG.warn("Failed to report bad " + block + 
                    " from datanode " + srcDataNode + " to namenode");
        }
      }
      throw new IOException("Unexpected checksum mismatch while writing "
          + block + " from " + inAddr);
    }
  }
  
    
  /**
   * Translate CRC chunks from the client's checksum implementation
   * to the disk checksum implementation.
   * 
   * This does not verify the original checksums, under the assumption
   * that they have already been validated.
   */
  private void translateChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf) {
    diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
  }

  /** 
   * Check whether checksum needs to be verified.
   * Skip verifying checksum iff this is not the last one in the 
   * pipeline and clientName is non-null. i.e. Checksum is verified
   * on all the datanodes when the data is being written by a 
   * datanode rather than a client. Whe client is writing the data, 
   * protocol includes acks and only the last datanode needs to verify 
   * checksum.
   * @return true if checksum verification is needed, otherwise false.
   */
  private boolean shouldVerifyChecksum() {
    return (mirrorOut == null || isDatanode || needsChecksumTranslation);
  }

  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
  private int receivePacket() throws IOException {
    // read the next packet
    packetReceiver.receiveNextPacket(in);

    PacketHeader header = packetReceiver.getHeader();
    if (LOG.isDebugEnabled()){
      LOG.debug("Receiving one packet for block " + block +
                ": " + header);
    }

    // Sanity check the header
    if (header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
      throw new IOException("Received an out-of-sequence packet for " + block + 
          "from " + inAddr + " at offset " + header.getOffsetInBlock() +
          ". Expecting packet starting at " + replicaInfo.getNumBytes());
    }
    if (header.getDataLen() < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block + 
                            ") from " + inAddr + " at offset " + 
                            header.getOffsetInBlock() + ": " +
                            header.getDataLen()); 
    }

    long offsetInBlock = header.getOffsetInBlock();
    long seqno = header.getSeqno();
    boolean lastPacketInBlock = header.isLastPacketInBlock();
    int len = header.getDataLen();
    boolean syncBlock = header.getSyncBlock();

    // avoid double sync'ing on close
    if (syncBlock && lastPacketInBlock) {
      this.syncOnClose = false;
    }

    // update received bytes
    long firstByteInBlock = offsetInBlock;
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock);
    }
    
    // put in queue for pending acks, unless sync was requested
    if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    //First write the packet to the mirror:
    if (mirrorOut != null && !mirrorError) {
      try {
        long begin = Time.monotonicNow();
        packetReceiver.mirrorPacketTo(mirrorOut);
        mirrorOut.flush();
        long duration = Time.monotonicNow() - begin;
        if (duration > datanodeSlowLogThresholdMs) {
          LOG.warn("Slow BlockReceiver write packet to mirror took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
        }
      } catch (IOException e) {
        handleMirrorOutError(e);
      }
    }
    
    ByteBuffer dataBuf = packetReceiver.getDataSlice();
    ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();
    
    if (lastPacketInBlock || len == 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Receiving an empty packet or the end of the block " + block);
      }
      // sync block if requested
      if (syncBlock) {
        flushOrSync(true);
      }
    } else {
      int checksumLen = ((len + bytesPerChecksum - 1)/bytesPerChecksum)*
                                                            checksumSize;

      if ( checksumBuf.capacity() != checksumLen) {
        throw new IOException("Length of checksums in packet " +
            checksumBuf.capacity() + " does not match calculated checksum " +
            "length " + checksumLen);
      }

      if (shouldVerifyChecksum()) {
        try {
          verifyChunks(dataBuf, checksumBuf);
        } catch (IOException ioe) {
          // checksum error detected locally. there is no reason to continue.
          if (responder != null) {
            try {
              ((PacketResponder) responder.getRunnable()).enqueue(seqno,
                  lastPacketInBlock, offsetInBlock,
                  Status.ERROR_CHECKSUM);
              // Wait until the responder sends back the response
              // and interrupt this thread.
              Thread.sleep(3000);
            } catch (InterruptedException e) { }
          }
          throw new IOException("Terminating due to a checksum error." + ioe);
        }
 
        if (needsChecksumTranslation) {
          // overwrite the checksums in the packet buffer with the
          // appropriate polynomial for the disk storage.
          translateChunks(dataBuf, checksumBuf);
        }
      }
      
      // by this point, the data in the buffer uses the disk checksum

      byte[] lastChunkChecksum;
      
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk();
        if (onDiskLen<offsetInBlock) {
          //finally write to the disk :
          
          if (onDiskLen % bytesPerChecksum != 0) { 
            // prepare to overwrite last checksum
            adjustCrcFilePosition();
          }
          
          // If this is a partial chunk, then read in pre-existing checksum
          if (firstByteInBlock % bytesPerChecksum != 0) {
            LOG.info("Packet starts at " + firstByteInBlock +
                     " for " + block +
                     " which is not a multiple of bytesPerChecksum " +
                     bytesPerChecksum);
            long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                onDiskLen / bytesPerChecksum * checksumSize;
            computePartialChunkCrc(onDiskLen, offsetInChecksum, bytesPerChecksum);
          }

          int startByteToDisk = (int)(onDiskLen-firstByteInBlock) 
              + dataBuf.arrayOffset() + dataBuf.position();

          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
          
          // Write data to disk.
          long begin = Time.monotonicNow();
          System.out.println( "[SAMAN][BlockReceiver][receivePacket] out.write, " +
                  "startByteToDisk="+startByteToDisk+
                  ",numBytesToDisk="+numBytesToDisk );
          //System.out.println( "[SAMAN][BlockReceiver][receivePacket] out = " + out.toString() );
          out.write(dataBuf.array(), startByteToDisk, numBytesToDisk);
          long duration = Time.monotonicNow() - begin;
          if (duration > datanodeSlowLogThresholdMs) {
            LOG.warn("Slow BlockReceiver write data to disk cost:" + duration
                + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
          }

          // If this is a partial chunk, then verify that this is the only
          // chunk in the packet. Calculate new crc for this chunk.
          if (partialCrc != null) {
            if (len > bytesPerChecksum) {
              throw new IOException("Got wrong length during writeBlock(" + 
                                    block + ") from " + inAddr + " " +
                                    "A packet can have only one partial chunk."+
                                    " len = " + len + 
                                    " bytesPerChecksum " + bytesPerChecksum);
            }
            partialCrc.update(dataBuf.array(), startByteToDisk, numBytesToDisk);
            byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
            lastChunkChecksum = Arrays.copyOfRange(
              buf, buf.length - checksumSize, buf.length
            );
            checksumOut.write(buf);
            if(LOG.isDebugEnabled()) {
              LOG.debug("Writing out partial crc for data len " + len);
            }
            partialCrc = null;
          } else {
            lastChunkChecksum = Arrays.copyOfRange(
                checksumBuf.array(),
                checksumBuf.arrayOffset() + checksumBuf.position() + checksumLen - checksumSize,
                checksumBuf.arrayOffset() + checksumBuf.position() + checksumLen);
            if( datanode.getDnConf().verifyChecksum == true ) {
                System.out.println( "[SAMAN][BlockReceiver][receiveBlock] verify checksum is true!" );
                checksumOut.write(checksumBuf.array(),
                        checksumBuf.arrayOffset() + checksumBuf.position(),
                        checksumLen);
            }
          }
          /// flush entire packet, sync if requested
          flushOrSync(syncBlock);
          
          replicaInfo.setLastChecksumAndDataLen(
            offsetInBlock, lastChunkChecksum
          );

          datanode.metrics.incrBytesWritten(len);

          manageWriterOsCache(offsetInBlock);
        }
      } catch (IOException iex) {
        datanode.checkDiskError();
        throw iex;
      }
    }

    // if sync was requested, put in queue for pending acks here
    // (after the fsync finished)
    if (responder != null && (syncBlock || shouldVerifyChecksum())) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    if (throttler != null) { // throttle I/O
      throttler.throttle(len);
    }
    
    return lastPacketInBlock?-1:len;
  }

  private void manageWriterOsCache(long offsetInBlock) {
    try {
      if (outFd != null &&
          offsetInBlock > lastCacheManagementOffset + CACHE_DROP_LAG_BYTES) {
        long begin = Time.monotonicNow();
        //
        // For SYNC_FILE_RANGE_WRITE, we want to sync from
        // lastCacheManagementOffset to a position "two windows ago"
        //
        //                         <========= sync ===========>
        // +-----------------------O--------------------------X
        // start                  last                      curPos
        // of file                 
        //
        if (syncBehindWrites) {
          if (syncBehindWritesInBackground) {
            this.datanode.getFSDataset().submitBackgroundSyncFileRangeRequest(
                block, outFd, lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
          } else {
            NativeIO.POSIX.syncFileRangeIfPossible(outFd,
                lastCacheManagementOffset, offsetInBlock
                    - lastCacheManagementOffset,
                NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
          }
        }
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning 
        // of the file to a position prior to the current position.
        //
        // <=== drop =====> 
        //                 <---W--->
        // +--------------+--------O--------------------------X
        // start        dropPos   last                      curPos
        // of file             
        //                     
        long dropPos = lastCacheManagementOffset - CACHE_DROP_LAG_BYTES;
        if (dropPos > 0 && dropCacheBehindWrites) {
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
              block.getBlockName(), outFd, 0, dropPos,
              NativeIO.POSIX.POSIX_FADV_DONTNEED);
        }
        lastCacheManagementOffset = offsetInBlock;
        long duration = Time.monotonicNow() - begin;
        if (duration > datanodeSlowLogThresholdMs) {
          LOG.warn("Slow manageWriterOsCache took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
        }
      }
    } catch (Throwable t) {
      LOG.warn("Error managing cache for writer of block " + block, t);
    }
  }

  void receiveBlock(
      DataOutputStream mirrOut, // output to next datanode
      DataInputStream mirrIn,   // input from next datanode
      DataOutputStream replyOut,  // output to previous datanode
      String mirrAddr, DataTransferThrottler throttlerArg,
      DatanodeInfo[] downstreams) throws IOException {

      syncOnClose = datanode.getDnConf().syncOnClose;
      boolean responderClosed = false;
      mirrorOut = mirrOut;
      mirrorAddr = mirrAddr;
      throttler = throttlerArg;

    String downstreamNodes = new String();

    for( int i = 0; i < downstreams.length; i++ ){
        downstreamNodes += downstreams[i].getName() + ",";
    }
    System.out.println( "[SAMAN][BlockReceiver][receiveBlock] downStreamNodes are: " + downstreamNodes );


    try {
      if (isClient && !isTransfer) {
        responder = new Daemon(datanode.threadGroup, 
            new PacketResponder(replyOut, mirrIn, downstreams));
        responder.start(); // start thread to processes responses
      }

      System.out.println( "[SAMAN][BlockReceiver][receiveBlock] start receivePacket()!" );

      if(((ReplicaInPipeline)replicaInfo).getBlockFile().exists()){
        System.out.println( "[SAMAN][BlockReceiver][receiveBlock] file exist before receivePacket()"  );
      }else{
        System.out.println( "[SAMAN][BlockReceiver][receiveBlock] file not exist before receivePacket()"  );
      }

      while (receivePacket() >= 0) { /* Receive until the last packet */ }

      if(((ReplicaInPipeline)replicaInfo).getBlockFile().exists()){
        System.out.println( "[SAMAN][BlockReceiver][receiveBlock] file exist after receivePacket()"  );
      }else{
        System.out.println( "[SAMAN][BlockReceiver][receiveBlock] file not exist after receivePacket()"  );
      }

      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // Here we need to do the transformation.
      // This part has beed added by me(SAMAN), for the netcdf project. By default, it's not working.
      // So you don't need to be worried about it.
      if( datanode.getDnConf().isnetcdf == true ){
        if(((ReplicaInPipeline)replicaInfo).getBlockFile().exists()){
          System.out.println( "[SAMAN][BlockReceiver][receiveBlock] file size:" + ((ReplicaInPipeline) replicaInfo).getBlockFile().length() );
        }
        if( downstreams.length == 2 ){
          System.out.println( "[SAMAN][BlockReceiver][receiveBlock] length == 2" );
          // In this case we would keep the file in it's original version.
        }else if( downstreams.length == 1 ){
          System.out.println( "[SAMAN][BlockReceiver][receiveBlock] length == 1" );
          // In this case we would transform it into sorted by latitude.
          boolean tmp = transformNetCDFSortedByLatitude();
          if( tmp )
            ((ReplicaInPipeline)replicaInfo).getNetCDFBlockFile().renameTo( ((ReplicaInPipeline)replicaInfo).getBlockFile() );
        }else{
          System.out.println( "[SAMAN][BlockReceiver][receiveBlock] length == 0" );
          // In this case we would transform it into sorted by longitude
          boolean tmp = transformNetCDFSortedByLongitude();
          if( tmp )
            ((ReplicaInPipeline)replicaInfo).getNetCDFBlockFile().renameTo( ((ReplicaInPipeline)replicaInfo).getBlockFile() );
        }
      }else{
        System.out.println( "[SAMAN][BlockReceiver][receiveBlock] isnetcdf == false" );
      }

      // If this write is for a replication or transfer-RBW/Finalized,
      // then finalize block or convert temporary to RBW.
      // For client-writes, the block is finalized in the PacketResponder.
      if (isDatanode || isTransfer) {

        System.out.println( "[SAMAN][BlockReceiver][receiveBlock] isDatanode || isTransfer" );

        // close the block/crc files
        close();
        block.setNumBytes(replicaInfo.getNumBytes());

        if (stage == BlockConstructionStage.TRANSFER_RBW) {
          System.out.println( "[SAMAN][BlockReceiver][receiveBlock] TRANSFER_RBW" );
          // for TRANSFER_RBW, convert temporary to RBW
          datanode.data.convertTemporaryToRbw(block);
        } else {
          System.out.println( "[SAMAN][BlockReceiver][receiveBlock] TRANSFER_RBW else!" );
          // for isDatnode or TRANSFER_FINALIZED
          // Finalize the block.
          datanode.data.finalizeBlock(block);
        }
        datanode.metrics.incrBlocksWritten();
      }

    } catch (IOException ioe) {
      if (datanode.isRestarting()) {
        // Do not throw if shutting down for restart. Otherwise, it will cause
        // premature termination of responder.
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.info("Exception for " + block, ioe);
        throw ioe;
      }
    } finally {
      // Clear the previous interrupt state of this thread.
      Thread.interrupted();

      // If a shutdown for restart was initiated, upstream needs to be notified.
      // There is no need to do anything special if the responder was closed
      // normally.
      if (!responderClosed) { // Data transfer was not complete.
        if (responder != null) {
          // In case this datanode is shutting down for quick restart,
          // send a special ack upstream.
          if (datanode.isRestarting() && isClient && !isTransfer) {
            File blockFile = ((ReplicaInPipeline)replicaInfo).getBlockFile();
            File restartMeta = new File(blockFile.getParent()  + 
                File.pathSeparator + "." + blockFile.getName() + ".restart");
            if (restartMeta.exists() && !restartMeta.delete()) {
              LOG.warn("Failed to delete restart meta file: " +
                  restartMeta.getPath());
            }
            try {
              FileWriter out = new FileWriter(restartMeta);
              // write out the current time.
              out.write(Long.toString(Time.now() + restartBudget));
              out.flush();
              out.close();
            } catch (IOException ioe) {
              // The worst case is not recovering this RBW replica. 
              // Client will fall back to regular pipeline recovery.
            }
            try {
              ((PacketResponder) responder.getRunnable()).
                  sendOOBResponse(PipelineAck.getRestartOOBStatus());
              // Even if the connection is closed after the ack packet is
              // flushed, the client can react to the connection closure 
              // first. Insert a delay to lower the chance of client 
              // missing the OOB ack.
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // It is already going down. Ignore this.
            } catch (IOException ioe) {
              LOG.info("Error sending OOB Ack.", ioe);
            }
          }
          responder.interrupt();
        }
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.interrupt();
          // join() on the responder should timeout a bit earlier than the
          // configured deadline. Otherwise, the join() on this thread will
          // likely timeout as well.
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder
                + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // do not throw if shutting down for restart.
          if (!datanode.isRestarting()) {
            throw new IOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }

  /** Cleanup a partial block 
   * if this write is for a replication request (and not from a client)
   */
  private void cleanupBlock() throws IOException {
    if (isDatanode) {
      datanode.data.unfinalizeBlock(block);
    }
  }

  /**
   * Adjust the file pointer in the local meta file so that the last checksum
   * will be overwritten.
   */
  private void adjustCrcFilePosition() throws IOException {
    if (out != null) {
     out.flush();
    }
    if (checksumOut != null) {
      checksumOut.flush();
    }

    // rollback the position of the meta file
    datanode.data.adjustCrcChannelPosition(block, streams, checksumSize);
  }

  /**
   * Convert a checksum byte array to a long
   */
  static private long checksum2long(byte[] checksum) {
    long crc = 0L;
    for(int i=0; i<checksum.length; i++) {
      crc |= (0xffL&checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  /**
   * reads in the partial crc chunk and computes checksum
   * of pre-existing data in partial chunk.
   */
  private void computePartialChunkCrc(long blkoff, long ckoff, 
                                      int bytesPerChecksum) throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    int checksumSize = diskChecksum.getChecksumSize();
    blkoff = blkoff - sizePartialChunk;
    LOG.info("computePartialChunkCrc sizePartialChunk " + 
              sizePartialChunk + " " + block +
              " block offset " + blkoff +
              " metafile offset " + ckoff);

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    ReplicaInputStreams instr = null;
    try { 
      instr = datanode.data.getTmpInputStreams(block, blkoff, ckoff);
      IOUtils.readFully(instr.getDataIn(), buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      IOUtils.readFully(instr.getChecksumIn(), crcbuf, 0, crcbuf.length);
    } finally {
      IOUtils.closeStream(instr);
    }

    // compute crc of partial chunk from data read in the block file.
    partialCrc = DataChecksum.newDataChecksum(
        diskChecksum.getChecksumType(), diskChecksum.getBytesPerChecksum());
    partialCrc.update(buf, 0, sizePartialChunk);
    LOG.info("Read in partial CRC chunk from disk for " + block);

    // paranoia! verify that the pre-computed crc matches what we
    // recalculated just now
    if (partialCrc.getValue() != checksum2long(crcbuf)) {
      String msg = "Partial CRC " + partialCrc.getValue() +
                   " does not match value computed the " +
                   " last time file was closed " +
                   checksum2long(crcbuf);
      throw new IOException(msg);
    }
  }
  
  private static enum PacketResponderType {
    NON_PIPELINE, LAST_IN_PIPELINE, HAS_DOWNSTREAM_IN_PIPELINE
  }
  
  private static final Status[] MIRROR_ERROR_STATUS = {Status.SUCCESS, Status.ERROR};
  
  /**
   * Processes responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable, Closeable {   
    /** queue for packets waiting for ack - synchronization using monitor lock */
    private final LinkedList<Packet> ackQueue = new LinkedList<Packet>(); 
    /** the thread that spawns this responder */
    private final Thread receiverThread = Thread.currentThread();
    /** is this responder running? - synchronization using monitor lock */
    private volatile boolean running = true;
    /** input from the next downstream datanode */
    private final DataInputStream downstreamIn;
    /** output to upstream datanode/client */
    private final DataOutputStream upstreamOut;
    /** The type of this responder */
    private final PacketResponderType type;
    /** for log and error messages */
    private final String myString; 
    private boolean sending = false;

    @Override
    public String toString() {
      return myString;
    }

    PacketResponder(final DataOutputStream upstreamOut,
        final DataInputStream downstreamIn, final DatanodeInfo[] downstreams) {
      this.downstreamIn = downstreamIn;
      this.upstreamOut = upstreamOut;

      this.type = downstreams == null? PacketResponderType.NON_PIPELINE
          : downstreams.length == 0? PacketResponderType.LAST_IN_PIPELINE
              : PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE;

      final StringBuilder b = new StringBuilder(getClass().getSimpleName())
          .append(": ").append(block).append(", type=").append(type);
      if (type != PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
        b.append(", downstreams=").append(downstreams.length)
            .append(":").append(Arrays.asList(downstreams));
      }
      this.myString = b.toString();
    }

    private boolean isRunning() {
      // When preparing for a restart, it should continue to run until
      // interrupted by the receiver thread.
      return running && (datanode.shouldRun || datanode.isRestarting());
    }
    
    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno sequence number of the packet
     * @param lastPacketInBlock if true, this is the last packet in block
     * @param offsetInBlock offset of this packet in block
     */
    void enqueue(final long seqno, final boolean lastPacketInBlock,
        final long offsetInBlock, final Status ackStatus) {
      final Packet p = new Packet(seqno, lastPacketInBlock, offsetInBlock,
          System.nanoTime(), ackStatus);
      if(LOG.isDebugEnabled()) {
        LOG.debug(myString + ": enqueue " + p);
      }
      synchronized(ackQueue) {
        if (running) {
          ackQueue.addLast(p);
          ackQueue.notifyAll();
        }
      }
    }

    /**
     * Send an OOB response. If all acks have been sent already for the block
     * and the responder is about to close, the delivery is not guaranteed.
     * This is because the other end can close the connection independently.
     * An OOB coming from downstream will be automatically relayed upstream
     * by the responder. This method is used only by originating datanode.
     *
     * @param ackStatus the type of ack to be sent
     */
    void sendOOBResponse(final Status ackStatus) throws IOException,
        InterruptedException {
      if (!running) {
        LOG.info("Cannot send OOB response " + ackStatus + 
            ". Responder not running.");
        return;
      }

      synchronized(this) {
        if (sending) {
          wait(PipelineAck.getOOBTimeout(ackStatus));
          // Didn't get my turn in time. Give up.
          if (sending) {
            throw new IOException("Could not send OOB reponse in time: "
                + ackStatus);
          }
        }
        sending = true;
      }

      LOG.info("Sending an out of band ack of type " + ackStatus);
      try {
        sendAckUpstreamUnprotected(null, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
            ackStatus);
      } finally {
        // Let others send ack. Unless there are miltiple OOB send
        // calls, there can be only one waiter, the responder thread.
        // In any case, only one needs to be notified.
        synchronized(this) {
          sending = false;
          notify();
        }
      }
    }
    
    /** Wait for a packet with given {@code seqno} to be enqueued to ackQueue */
    Packet waitForAckHead(long seqno) throws InterruptedException {
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(myString + ": seqno=" + seqno +
                      " waiting for local datanode to finish write.");
          }
          ackQueue.wait();
        }
        return isRunning() ? ackQueue.getFirst() : null;
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    @Override
    public void close() {
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() != 0) {
          try {
            ackQueue.wait();
          } catch (InterruptedException e) {
            running = false;
            Thread.currentThread().interrupt();
          }
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug(myString + ": closing");
        }
        running = false;
        ackQueue.notifyAll();
      }

      synchronized(this) {
        running = false;
        notifyAll();
      }
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      boolean lastPacketInBlock = false;
      final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
      while (isRunning() && !lastPacketInBlock) {
        long totalAckTimeNanos = 0;
        boolean isInterrupted = false;
        try {
          Packet pkt = null;
          long expected = -2;
          PipelineAck ack = new PipelineAck();
          long seqno = PipelineAck.UNKOWN_SEQNO;
          long ackRecvNanoTime = 0;
          try {
            if (type != PacketResponderType.LAST_IN_PIPELINE && !mirrorError) {
              // read an ack from downstream datanode
              ack.readFields(downstreamIn);
              ackRecvNanoTime = System.nanoTime();
              if (LOG.isDebugEnabled()) {
                LOG.debug(myString + " got " + ack);
              }
              // Process an OOB ACK.
              Status oobStatus = ack.getOOBStatus();
              if (oobStatus != null) {
                LOG.info("Relaying an out of band ack of type " + oobStatus);
                sendAckUpstream(ack, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
                    Status.SUCCESS);
                continue;
              }
              seqno = ack.getSeqno();
            }
            if (seqno != PipelineAck.UNKOWN_SEQNO
                || type == PacketResponderType.LAST_IN_PIPELINE) {
              pkt = waitForAckHead(seqno);
              if (!isRunning()) {
                break;
              }
              expected = pkt.seqno;
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE
                  && seqno != expected) {
                throw new IOException(myString + "seqno: expected=" + expected
                    + ", received=" + seqno);
              }
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
                // The total ack time includes the ack times of downstream
                // nodes.
                // The value is 0 if this responder doesn't have a downstream
                // DN in the pipeline.
                totalAckTimeNanos = ackRecvNanoTime - pkt.ackEnqueueNanoTime;
                // Report the elapsed time from ack send to ack receive minus
                // the downstream ack time.
                long ackTimeNanos = totalAckTimeNanos
                    - ack.getDownstreamAckTimeNanos();
                if (ackTimeNanos < 0) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Calculated invalid ack time: " + ackTimeNanos
                        + "ns.");
                  }
                } else {
                  datanode.metrics.addPacketAckRoundTripTimeNanos(ackTimeNanos);
                }
              }
              lastPacketInBlock = pkt.lastPacketInBlock;
            }
          } catch (InterruptedException ine) {
            isInterrupted = true;
          } catch (IOException ioe) {
            if (Thread.interrupted()) {
              isInterrupted = true;
            } else {
              // continue to run even if can not read from mirror
              // notify client of the error
              // and wait for the client to shut down the pipeline
              mirrorError = true;
              LOG.info(myString, ioe);
            }
          }

          if (Thread.interrupted() || isInterrupted) {
            /*
             * The receiver thread cancelled this thread. We could also check
             * any other status updates from the receiver thread (e.g. if it is
             * ok to write to replyOut). It is prudent to not send any more
             * status back to the client because this datanode has a problem.
             * The upstream datanode will detect that this datanode is bad, and
             * rightly so.
             *
             * The receiver thread can also interrupt this thread for sending
             * an out-of-band response upstream.
             */
            LOG.info(myString + ": Thread is interrupted.");
            running = false;
            continue;
          }

          if (lastPacketInBlock) {
            // Finalize the block and close the block file
            finalizeBlock(startTime);
          }

          sendAckUpstream(ack, expected, totalAckTimeNanos,
              (pkt != null ? pkt.offsetInBlock : 0), 
              (pkt != null ? pkt.ackStatus : Status.SUCCESS));
          if (pkt != null) {
            // remove the packet from the ack queue
            removeAckHead();
          }
        } catch (IOException e) {
          LOG.warn("IOException in BlockReceiver.run(): ", e);
          if (running) {
            datanode.checkDiskError();
            LOG.info(myString, e);
            running = false;
            if (!Thread.interrupted()) { // failure not caused by interruption
              receiverThread.interrupt();
            }
          }
        } catch (Throwable e) {
          if (running) {
            LOG.info(myString, e);
            running = false;
            receiverThread.interrupt();
          }
        }
      }
      LOG.info(myString + " terminating");
    }
    
    /**
     * Finalize the block and close the block file
     * @param startTime time when BlockReceiver started receiving the block
     */
    private void finalizeBlock(long startTime) throws IOException {
      BlockReceiver.this.close();
      final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime()
          : 0;
      block.setNumBytes(replicaInfo.getNumBytes());
      datanode.data.finalizeBlock(block);
      datanode.closeBlock(
          block, DataNode.EMPTY_DEL_HINT, replicaInfo.getStorageUuid());
      if (ClientTraceLog.isInfoEnabled() && isClient) {
        long offset = 0;
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block
            .getBlockPoolId());
        ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT, inAddr,
            myAddr, block.getNumBytes(), "HDFS_WRITE", clientname, offset,
            dnR.getDatanodeUuid(), block, endTime - startTime));
      } else {
        LOG.info("Received " + block + " size " + block.getNumBytes()
            + " from " + inAddr);
      }
    }
    
    /**
     * The wrapper for the unprotected version. This is only called by
     * the responder's run() method.
     *
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myStatus the local ack status
     */
    private void sendAckUpstream(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock,
        Status myStatus) throws IOException {
      try {
        // Wait for other sender to finish. Unless there is an OOB being sent,
        // the responder won't have to wait.
        synchronized(this) {
          while(sending) {
            wait();
          }
          sending = true;
        }

        try {
          if (!running) return;
          sendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos,
              offsetInBlock, myStatus);
        } finally {
          synchronized(this) {
            sending = false;
            notify();
          }
        }
      } catch (InterruptedException ie) {
        // The responder was interrupted. Make it go down without
        // interrupting the receiver(writer) thread.  
        running = false;
      }
    }

    /**
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myStatus the local ack status
     */
    private void sendAckUpstreamUnprotected(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock, Status myStatus)
        throws IOException {
      Status[] replies = null;
      if (ack == null) {
        // A new OOB response is being sent from this node. Regardless of
        // downstream nodes, reply should contain one reply.
        replies = new Status[1];
        replies[0] = myStatus;
      } else if (mirrorError) { // ack read error
        replies = MIRROR_ERROR_STATUS;
      } else {
        short ackLen = type == PacketResponderType.LAST_IN_PIPELINE ? 0 : ack
            .getNumOfReplies();
        replies = new Status[1 + ackLen];
        replies[0] = myStatus;
        for (int i = 0; i < ackLen; i++) {
          replies[i + 1] = ack.getReply(i);
        }
        // If the mirror has reported that it received a corrupt packet,
        // do self-destruct to mark myself bad, instead of making the 
        // mirror node bad. The mirror is guaranteed to be good without
        // corrupt data on disk.
        if (ackLen > 0 && replies[1] == Status.ERROR_CHECKSUM) {
          throw new IOException("Shutting down writer and responder "
              + "since the down streams reported the data sent by this "
              + "thread is corrupt");
        }
      }
      PipelineAck replyAck = new PipelineAck(seqno, replies,
          totalAckTimeNanos);
      if (replyAck.isSuccess()
          && offsetInBlock > replicaInfo.getBytesAcked()) {
        replicaInfo.setBytesAcked(offsetInBlock);
      }
      // send my ack back to upstream datanode
      long begin = Time.monotonicNow();
      replyAck.write(upstreamOut);
      upstreamOut.flush();
      long duration = Time.monotonicNow() - begin;
      if (duration > datanodeSlowLogThresholdMs) {
        LOG.warn("Slow PacketResponder send ack to upstream took " + duration
            + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), " + myString
            + ", replyAck=" + replyAck);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(myString + ", replyAck=" + replyAck);
      }

      // If a corruption was detected in the received data, terminate after
      // sending ERROR_CHECKSUM back. 
      if (myStatus == Status.ERROR_CHECKSUM) {
        throw new IOException("Shutting down writer and responder "
            + "due to a checksum error in received data. The error "
            + "response has been sent upstream.");
      }
    }
    
    /**
     * Remove a packet from the head of the ack queue
     * 
     * This should be called only when the ack queue is not empty
     */
    private void removeAckHead() {
      synchronized(ackQueue) {
        ackQueue.removeFirst();
        ackQueue.notifyAll();
      }
    }
  }
  
  /**
   * This information is cached by the Datanode in the ackQueue.
   */
  private static class Packet {
    final long seqno;
    final boolean lastPacketInBlock;
    final long offsetInBlock;
    final long ackEnqueueNanoTime;
    final Status ackStatus;

    Packet(long seqno, boolean lastPacketInBlock, long offsetInBlock,
        long ackEnqueueNanoTime, Status ackStatus) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.offsetInBlock = offsetInBlock;
      this.ackEnqueueNanoTime = ackEnqueueNanoTime;
      this.ackStatus = ackStatus;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(seqno=" + seqno
        + ", lastPacketInBlock=" + lastPacketInBlock
        + ", offsetInBlock=" + offsetInBlock
        + ", ackEnqueueNanoTime=" + ackEnqueueNanoTime
        + ", ackStatus=" + ackStatus
        + ")";
    }
  }

  public boolean transformNetCDFSortedByLatitude() {
    NetcdfFile inputFile = null;
    NetcdfFileWriter outputFile = null;

    boolean returnValue = false;

    try{
      inputFile = NetcdfFile.open(((ReplicaInPipeline)replicaInfo).getBlockFile().getAbsolutePath(), null);
      outputFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, ((ReplicaInPipeline)replicaInfo).getNetCDFBlockFile().getAbsolutePath());

      if( outputFile == null )
        System.out.println( "[SAMAN][BlockReceiver][transformNetCDFSortedByLatitude] outputFile is null!" );

      Variable vtime = inputFile.findVariable("time");
      Variable vtime_bnds = inputFile.findVariable("time_bnds");
      Variable vlat = inputFile.findVariable("lat");
      Variable vlat_bnds = inputFile.findVariable("lat_bnds");
      Variable vlon = inputFile.findVariable("lon");
      Variable vlon_bnds = inputFile.findVariable("lon_bnds");
      Variable vrsut = inputFile.findVariable("rsut");

      Dimension latDim = outputFile.addDimension(null, vlat.getDimensionsString(), (int) (vlat.getSize()));
      latDim.setUnlimited(true);
      Dimension timeDim = outputFile.addDimension(null, vtime.getDimensionsString(), (int) (vtime.getSize()));
      Dimension lonDim = outputFile.addDimension(null, vlon.getDimensionsString(), (int) (vlon.getSize()));
      Dimension bndDim = outputFile.addDimension(null, "bnds", 2);

      java.util.List<Dimension> time_bnds_dim = new ArrayList<Dimension>();
      java.util.List<Dimension> lat_bnds_dim = new ArrayList<Dimension>();
      java.util.List<Dimension> lon_bnds_dim = new ArrayList<Dimension>();
      java.util.List<Dimension> rsut_dim = new ArrayList<Dimension>();

      time_bnds_dim.add(timeDim);
      time_bnds_dim.add(bndDim);
      lat_bnds_dim.add(latDim);
      lat_bnds_dim.add(bndDim);
      lon_bnds_dim.add(lonDim);
      lon_bnds_dim.add(bndDim);
      rsut_dim.add(latDim);
      rsut_dim.add(timeDim);
      rsut_dim.add(lonDim);

      Variable vlatNew = outputFile.addVariable(null, vlat.getShortName(), vlat.getDataType(), vlat.getDimensionsString());
      Variable vlatbndsNew = outputFile.addVariable(null, vlat_bnds.getShortName(), vlat_bnds.getDataType(), lat_bnds_dim);
      Variable vtimeNew = outputFile.addVariable(null, vtime.getShortName(), vtime.getDataType(), vtime.getDimensionsString());
      Variable vtimebndsNew = outputFile.addVariable(null, vtime_bnds.getShortName(), vtime_bnds.getDataType(), time_bnds_dim);
      Variable vlonNew = outputFile.addVariable(null, vlon.getShortName(), vlon.getDataType(), vlon.getDimensionsString());
      Variable vlonbndsNew = outputFile.addVariable(null, vlon_bnds.getShortName(), vlon_bnds.getDataType(), lon_bnds_dim);
      Variable vrsutNew = outputFile.addVariable(null, vrsut.getShortName(), vrsut.getDataType(), rsut_dim);

      java.util.List<Attribute> attributes = vtime.getAttributes();
      Iterator itr = attributes.iterator();

      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vtimeNew.addAttribute(attribute);
      }

      attributes = vlat.getAttributes();
      itr = attributes.iterator();
      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vlatNew.addAttribute(attribute);
      }

      attributes = vlon.getAttributes();
      itr = attributes.iterator();
      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vlonNew.addAttribute(attribute);
      }

      attributes = vrsut.getAttributes();
      itr = attributes.iterator();
      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vrsutNew.addAttribute(attribute);
      }

      outputFile.addGroupAttribute(null, new Attribute("institution", "European Centre for Medium-Range Weather Forecasts"));
      outputFile.addGroupAttribute(null, new Attribute("institute_id", "ECMWF"));
      outputFile.addGroupAttribute(null, new Attribute("experiment_id", "ERA-Interim"));
      outputFile.addGroupAttribute(null, new Attribute("source", "ERA Interim, Synoptic Monthly Means, Full Resolution"));
      outputFile.addGroupAttribute(null, new Attribute("model_id", "IFS-Cy31r2"));
      outputFile.addGroupAttribute(null, new Attribute("contact", "ECMWF, Dick Dee (dick.dee@ecmwf.int)"));
      outputFile.addGroupAttribute(null, new Attribute("references", "http://www.ecmwf.int"));
      outputFile.addGroupAttribute(null, new Attribute("tracking_id", "df4494d9-1d4b-4156-8804-ce238542a777"));
      outputFile.addGroupAttribute(null, new Attribute("mip_specs", "CMIP5"));
      outputFile.addGroupAttribute(null, new Attribute("source_id", "ERA-Interim"));
      outputFile.addGroupAttribute(null, new Attribute("product", "reanalysis"));
      outputFile.addGroupAttribute(null, new Attribute("frequency", "mon"));
      outputFile.addGroupAttribute(null, new Attribute("creation_date", "2014-04-28T21:55:14Z"));
      outputFile.addGroupAttribute(null, new Attribute("history", "2014-04-28T21:54:28Z CMOR rewrote data to comply with CF standards and ana4MIPs requirements."));
      outputFile.addGroupAttribute(null, new Attribute("Conventions", "CF-1.4"));
      outputFile.addGroupAttribute(null, new Attribute("project_id", "ana4MIPs"));
      outputFile.addGroupAttribute(null, new Attribute("table_id", "Table Amon_ana (10 March 2011) fb925e593e0cbb86dd6e96fbbcb352e0"));
      outputFile.addGroupAttribute(null, new Attribute("title", "Reanalysis output prepared for ana4MIPs "));
      outputFile.addGroupAttribute(null, new Attribute("modeling_realm", "atmos"));
      outputFile.addGroupAttribute(null, new Attribute("cmor_version", "2.8.3"));

      ArrayDouble.D1 latArray = (ArrayDouble.D1) vlat.read();
      Array dataLat = Array.factory(DataType.DOUBLE, new int[]{(int)(vlat.getSize())});
      int[] shape = latArray.getShape();
      for( int i = 0; i < shape[0]; i++ ){
        dataLat.setDouble(i, latArray.get(i));
      }

      ArrayDouble.D2 latBndsArray = (ArrayDouble.D2) vlat_bnds.read();
      shape = latBndsArray.getShape();
      Array dataLatBnds = Array.factory(DataType.DOUBLE, new int[]{shape[0], shape[1]});
      Index2D idx = new Index2D(new int[]{shape[0], shape[1]});
      for (int i = 0; i < shape[0]; i++) {
        for (int j = 0; j < shape[1]; j++) {
          idx.set(i, j);
          dataLatBnds.setDouble(idx, latBndsArray.get(i,j));
        }
      }

      ArrayDouble.D1 timeArray = (ArrayDouble.D1) vtime.read();
      Array dataTime = Array.factory(DataType.DOUBLE, new int[]{(int) (vtime.getSize())});
      shape = timeArray.getShape();
      for (int i = 0; i < shape[0]; i++) {
        dataTime.setDouble(i, timeArray.get(i));
      }

      ArrayDouble.D2 timeBndsArray = (ArrayDouble.D2) vtime_bnds.read();
      shape = timeBndsArray.getShape();
      Array dataTimeBnds = Array.factory(DataType.DOUBLE, new int[]{shape[0], shape[1]});
      idx = new Index2D(new int[]{shape[0], shape[1]});
      for (int i = 0; i < shape[0]; i++) {
        for (int j = 0; j < shape[1]; j++) {
          idx.set(i, j);
          dataTimeBnds.setDouble(idx, timeBndsArray.get(i, j));
        }
      }

      ArrayDouble.D1 lonArray = (ArrayDouble.D1) vlon.read();
      Array dataLon = Array.factory(DataType.DOUBLE, new int[]{(int) (vlon.getSize())});
      shape = lonArray.getShape();
      for (int i = 0; i < shape[0]; i++) {
        dataLon.setDouble(i, lonArray.get(i));
      }

      ArrayDouble.D2 lonBndsArray = (ArrayDouble.D2) vlon_bnds.read();
      shape = lonBndsArray.getShape();
      Array dataLonBnds = Array.factory(DataType.DOUBLE, new int[]{shape[0], shape[1]});
      idx = new Index2D(new int[]{shape[0], shape[1]});
      for (int i = 0; i < shape[0]; i++) {
        for (int j = 0; j < shape[1]; j++) {
          idx.set(i, j);
          dataLonBnds.setDouble(idx, lonBndsArray.get(i, j));
        }
      }

      ArrayFloat.D3 dataRsut = (ArrayFloat.D3)(Array.factory(DataType.FLOAT, new int[]{(int) (vlat.getSize()), (int) (vtime.getSize()), (int) (vlon.getSize())}));
      ArrayFloat.D3 rsutArray = (ArrayFloat.D3) vrsut.read();
      for( int i = 0; i < vlat.getSize(); i++ ) {
        for (int j = 0; j < vtime.getSize(); j++) {
          for (int k = 0; k < vlon.getSize(); k++) {
            try {
              dataRsut.set(i, j, k, rsutArray.get(j, i, k));
            } catch (Exception e) {
              System.out.println("[SAMAN][NetCDFOutputFormat][Write] Exception in rsut = " + e.getMessage());
              throw e;
            }
          }
        }
      }

      outputFile.create();
      outputFile.write(vlatNew, dataLat);
      outputFile.write(vlatbndsNew, dataLatBnds);
      outputFile.write(vtimeNew, dataTime);
      outputFile.write(vtimebndsNew, dataTimeBnds);
      outputFile.write(vlonNew, dataLon);
      outputFile.write(vlonbndsNew, dataLonBnds);
      outputFile.write(vrsutNew, dataRsut);

      returnValue = true;

    }catch (Exception e){
      e.printStackTrace(System.out);
    } finally {
      if( outputFile != null ){
        try{
          outputFile.close();
        }catch (IOException e){
          e.printStackTrace();
        }
      }
    }

    return returnValue;
  }

  public boolean transformNetCDFSortedByLongitude() {
    NetcdfFile inputFile = null;
    NetcdfFileWriter outputFile = null;

    boolean returnValue = false;

    try{
      inputFile = NetcdfFile.open(((ReplicaInPipeline)replicaInfo).getBlockFile().getAbsolutePath(), null);
      outputFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, ((ReplicaInPipeline)replicaInfo).getNetCDFBlockFile().getAbsolutePath());

      if( outputFile == null )
        System.out.println( "[SAMAN][BlockReceiver][transformNetCDFSortedByLongitude] outputFile is null!" );


      Variable vtime = inputFile.findVariable("time");
      Variable vtime_bnds = inputFile.findVariable("time_bnds");
      Variable vlat = inputFile.findVariable("lat");
      Variable vlat_bnds = inputFile.findVariable("lat_bnds");
      Variable vlon = inputFile.findVariable("lon");
      Variable vlon_bnds = inputFile.findVariable("lon_bnds");
      Variable vrsut = inputFile.findVariable("rsut");

      Dimension lonDim = outputFile.addDimension(null, vlon.getDimensionsString(), (int) (vlon.getSize()));
      lonDim.setUnlimited(true);
      Dimension timeDim = outputFile.addDimension(null, vtime.getDimensionsString(), (int) (vtime.getSize()));
      Dimension latDim = outputFile.addDimension(null, vlat.getDimensionsString(), (int) (vlat.getSize()));
      Dimension bndDim = outputFile.addDimension(null, "bnds", 2);

      java.util.List<Dimension> time_bnds_dim = new ArrayList<Dimension>();
      java.util.List<Dimension> lat_bnds_dim = new ArrayList<Dimension>();
      java.util.List<Dimension> lon_bnds_dim = new ArrayList<Dimension>();
      java.util.List<Dimension> rsut_dim = new ArrayList<Dimension>();

      time_bnds_dim.add(timeDim);
      time_bnds_dim.add(bndDim);
      lat_bnds_dim.add(latDim);
      lat_bnds_dim.add(bndDim);
      lon_bnds_dim.add(lonDim);
      lon_bnds_dim.add(bndDim);
      rsut_dim.add(lonDim);
      rsut_dim.add(timeDim);
      rsut_dim.add(latDim);

      Variable vlonNew = outputFile.addVariable(null, vlon.getShortName(), vlon.getDataType(), vlon.getDimensionsString());
      Variable vlonbndsNew = outputFile.addVariable(null, vlon_bnds.getShortName(), vlon_bnds.getDataType(), lon_bnds_dim);
      Variable vtimeNew = outputFile.addVariable(null, vtime.getShortName(), vtime.getDataType(), vtime.getDimensionsString());
      Variable vtimebndsNew = outputFile.addVariable(null, vtime_bnds.getShortName(), vtime_bnds.getDataType(), time_bnds_dim);
      Variable vlatNew = outputFile.addVariable(null, vlat.getShortName(), vlat.getDataType(), vlat.getDimensionsString());
      Variable vlatbndsNew = outputFile.addVariable(null, vlat_bnds.getShortName(), vlat_bnds.getDataType(), lat_bnds_dim);
      Variable vrsutNew = outputFile.addVariable(null, vrsut.getShortName(), vrsut.getDataType(), rsut_dim);

      java.util.List<Attribute> attributes = vtime.getAttributes();
      Iterator itr = attributes.iterator();

      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vtimeNew.addAttribute(attribute);
      }

      attributes = vlat.getAttributes();
      itr = attributes.iterator();
      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vlatNew.addAttribute(attribute);
      }

      attributes = vlon.getAttributes();
      itr = attributes.iterator();
      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vlonNew.addAttribute(attribute);
      }

      attributes = vrsut.getAttributes();
      itr = attributes.iterator();
      while (itr.hasNext()) {
        Attribute attribute = (Attribute) itr.next();
        vrsutNew.addAttribute(attribute);
      }

      outputFile.addGroupAttribute(null, new Attribute("institution", "European Centre for Medium-Range Weather Forecasts"));
      outputFile.addGroupAttribute(null, new Attribute("institute_id", "ECMWF"));
      outputFile.addGroupAttribute(null, new Attribute("experiment_id", "ERA-Interim"));
      outputFile.addGroupAttribute(null, new Attribute("source", "ERA Interim, Synoptic Monthly Means, Full Resolution"));
      outputFile.addGroupAttribute(null, new Attribute("model_id", "IFS-Cy31r2"));
      outputFile.addGroupAttribute(null, new Attribute("contact", "ECMWF, Dick Dee (dick.dee@ecmwf.int)"));
      outputFile.addGroupAttribute(null, new Attribute("references", "http://www.ecmwf.int"));
      outputFile.addGroupAttribute(null, new Attribute("tracking_id", "df4494d9-1d4b-4156-8804-ce238542a777"));
      outputFile.addGroupAttribute(null, new Attribute("mip_specs", "CMIP5"));
      outputFile.addGroupAttribute(null, new Attribute("source_id", "ERA-Interim"));
      outputFile.addGroupAttribute(null, new Attribute("product", "reanalysis"));
      outputFile.addGroupAttribute(null, new Attribute("frequency", "mon"));
      outputFile.addGroupAttribute(null, new Attribute("creation_date", "2014-04-28T21:55:14Z"));
      outputFile.addGroupAttribute(null, new Attribute("history", "2014-04-28T21:54:28Z CMOR rewrote data to comply with CF standards and ana4MIPs requirements."));
      outputFile.addGroupAttribute(null, new Attribute("Conventions", "CF-1.4"));
      outputFile.addGroupAttribute(null, new Attribute("project_id", "ana4MIPs"));
      outputFile.addGroupAttribute(null, new Attribute("table_id", "Table Amon_ana (10 March 2011) fb925e593e0cbb86dd6e96fbbcb352e0"));
      outputFile.addGroupAttribute(null, new Attribute("title", "Reanalysis output prepared for ana4MIPs "));
      outputFile.addGroupAttribute(null, new Attribute("modeling_realm", "atmos"));
      outputFile.addGroupAttribute(null, new Attribute("cmor_version", "2.8.3"));

      ArrayDouble.D1 lonArray = (ArrayDouble.D1) vlon.read();
      Array dataLon = Array.factory(DataType.DOUBLE, new int[]{(int) (vlon.getSize())});
      int[] shape = lonArray.getShape();
      for (int i = 0; i < shape[0]; i++) {
        dataLon.setDouble(i, lonArray.get(i));
      }

      ArrayDouble.D2 lonBndsArray = (ArrayDouble.D2) vlon_bnds.read();
      shape = lonBndsArray.getShape();
      Array dataLonBnds = Array.factory(DataType.DOUBLE, new int[]{shape[0], shape[1]});
      Index2D idx = new Index2D(new int[]{shape[0], shape[1]});
      for (int i = 0; i < shape[0]; i++) {
        for (int j = 0; j < shape[1]; j++) {
          idx.set(i, j);
          dataLonBnds.setDouble(idx, lonBndsArray.get(i, j));
        }
      }

      ArrayDouble.D1 timeArray = (ArrayDouble.D1) vtime.read();
      Array dataTime = Array.factory(DataType.DOUBLE, new int[]{(int) (vtime.getSize())});
      shape = timeArray.getShape();
      for (int i = 0; i < shape[0]; i++) {
        dataTime.setDouble(i, timeArray.get(i));
      }

      ArrayDouble.D2 timeBndsArray = (ArrayDouble.D2) vtime_bnds.read();
      shape = timeBndsArray.getShape();
      Array dataTimeBnds = Array.factory(DataType.DOUBLE, new int[]{shape[0], shape[1]});
      idx = new Index2D(new int[]{shape[0], shape[1]});
      for (int i = 0; i < shape[0]; i++) {
        for (int j = 0; j < shape[1]; j++) {
          idx.set(i, j);
          dataTimeBnds.setDouble(idx, timeBndsArray.get(i, j));
        }
      }

      ArrayDouble.D1 latArray = (ArrayDouble.D1) vlat.read();
      Array dataLat = Array.factory(DataType.DOUBLE, new int[]{(int)(vlat.getSize())});
      shape = latArray.getShape();
      for( int i = 0; i < shape[0]; i++ ){
        dataLat.setDouble(i, latArray.get(i));
      }

      ArrayDouble.D2 latBndsArray = (ArrayDouble.D2) vlat_bnds.read();
      shape = latBndsArray.getShape();
      Array dataLatBnds = Array.factory(DataType.DOUBLE, new int[]{shape[0], shape[1]});
      idx = new Index2D(new int[]{shape[0], shape[1]});
      for (int i = 0; i < shape[0]; i++) {
        for (int j = 0; j < shape[1]; j++) {
          idx.set(i, j);
          dataLatBnds.setDouble(idx, latBndsArray.get(i,j));
        }
      }

      ArrayFloat.D3 dataRsut = (ArrayFloat.D3)(Array.factory(DataType.FLOAT, new int[]{(int) (vlon.getSize()), (int) (vtime.getSize()), (int) (vlat.getSize())}));
      ArrayFloat.D3 rsutArray = (ArrayFloat.D3) vrsut.read();
      for( int i = 0; i < vlon.getSize(); i++ ) {
        for (int j = 0; j < vtime.getSize(); j++) {
          for (int k = 0; k < vlat.getSize(); k++) {
            try {
              dataRsut.set(i, j, k, rsutArray.get(j,k,i));
            } catch (Exception e) {
              System.out.println("[SAMAN][NetCDFOutputFormat][Write] Exception in rsut = " + e.getMessage());
              throw e;
            }
          }
        }
      }
      outputFile.create();
      outputFile.write(vlonNew, dataLon);
      outputFile.write(vlonbndsNew, dataLonBnds);
      outputFile.write(vtimeNew, dataTime);
      outputFile.write(vtimebndsNew, dataTimeBnds);
      outputFile.write(vlatNew, dataLat);
      outputFile.write(vlatbndsNew, dataLatBnds);
      outputFile.write(vrsutNew, dataRsut);
      returnValue = true;

    }catch (Exception e){
      e.printStackTrace();
    } finally {
      if( outputFile != null ){
        try{
          outputFile.close();
        }catch (IOException e){
          e.printStackTrace();
        }
      }
    }
    return returnValue;
  }

}
