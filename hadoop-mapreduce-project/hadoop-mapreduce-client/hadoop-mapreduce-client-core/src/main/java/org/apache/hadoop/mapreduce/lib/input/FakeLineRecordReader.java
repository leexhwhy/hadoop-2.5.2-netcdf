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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Treats keys as offset in file and value as line. 
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class FakeLineRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
    public static final String MAX_LINE_LENGTH =
            "mapreduce.input.linerecordreader.line.maxlength";

    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;
    int counter = 0;

    public FakeLineRecordReader() {
    }

    public FakeLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {

        FileSplit split = (FileSplit) genericSplit;
        LOG.info("[SAMAN] LineRecordReader.initialize => split.Locations="+split.getLocations()+", split.startChunk="+split.getStart()+", split.endChunk="
                +split.getStart()+split.getLength()
                +", split.length="+split.getLength()+", split.path="+split.getPath());
        System.out.println("[SAMAN] LineRecordReader.initialize => split.Locations="+split.getLocations()+", split.startChunk="+split.getStart()+", split.endChunk="
                +split.getStart()+split.getLength()
                +", split.length="+split.getLength()+", split.path="+split.getPath());
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);

        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
        if (null!=codec) {
            isCompressedInput = true;
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream cIn =
                        ((SplittableCompressionCodec)codec).createInputStream(
                                fileIn, decompressor, start, end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                in = new CompressedSplitLineReader(cIn, job,
                        this.recordDelimiterBytes);
                start = cIn.getAdjustedStart();
                end = cIn.getAdjustedEnd();
                filePosition = cIn;
            } else {
                in = new SplitLineReader(codec.createInputStream(fileIn,
                        decompressor), job, this.recordDelimiterBytes);
                filePosition = fileIn;
            }
        } else {
            fileIn.seek(start);
            in = new SplitLineReader(fileIn, job, this.recordDelimiterBytes);
            filePosition = fileIn;
        }
        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.pos = start;
    }


    private int maxBytesToConsume(long pos) {
        return isCompressedInput
                ? Integer.MAX_VALUE
                : (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

    private int skipUtfByteOrderMark() throws IOException {
        // Strip BOM(Byte Order Mark)
        // Text only support UTF-8, we only need to check UTF-8 BOM
        // (0xEF,0xBB,0xBF) at the start of the text stream.
        int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
                Integer.MAX_VALUE);
        int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
        // Even we read 3 extra bytes for the first line,
        // we won't alter existing behavior (no backwards incompat issue).
        // Because the newSize is less than maxLineLength and
        // the number of bytes copied to Text is always no more than newSize.
        // If the return size from readLine is not less than maxLineLength,
        // we will discard the current line and read the next line.
        pos += newSize;
        int textLength = value.getLength();
        byte[] textBytes = value.getBytes();
        if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
                (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
            // find UTF-8 BOM, strip it.
            LOG.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                // It may work to use the same buffer and not do the copyBytes
                textBytes = value.copyBytes();
                value.set(textBytes, 3, textLength);
            } else {
                value.clear();
            }
        }
        return newSize;
    }

    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)

        String text  = "Hello my name is saman welcome to my fake program and enjoy them ";
        text += text;
        text += text;
        text += text;
        text += text;
        text += text;
        text += text;
        text += text;
        text += text;
        text += text;
        if( counter < 128*1024*1024 ){
            value = new Text(text);
            pos += 64*512;
            counter += 64*512;
            newSize = 64*512;
        }
        if( counter >= 128*1024*1024 ){
            newSize = 0;
        }
        /*
        while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
            if (pos == 0) {
                newSize = skipUtfByteOrderMark();
            } else {
                try {
                    newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
                }catch ( IOException e ){
                    //LOG.info( "[SAMAN] LineRecordReader.nextKeyValue(), exception for value="+value );
                    //System.out.println( "[SAMAN] LineRecordReader.nextKeyValue(), exception for value="+value );
                    throw e;
                }
                pos += newSize;
            }

            if ((newSize == 0) || (newSize < maxLineLength)) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " +
                    (pos - newSize));
        }
        */
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            try {
                Thread.sleep(1);
            }catch (Exception e){
                return true;
            }
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
        }
    }

    public synchronized void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }
}