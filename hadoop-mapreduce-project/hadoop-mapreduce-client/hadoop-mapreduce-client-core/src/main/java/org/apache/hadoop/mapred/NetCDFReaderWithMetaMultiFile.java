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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NetCDFArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.util.List;
import ucar.nc2.*;
import ucar.nc2.iosp.*;
import ucar.nc2.iosp.netcdf3.*;
import ucar.unidata.io.*;
import ucar.nc2.dataset.*;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import java.util.Arrays;
/**
 * Treats keys as offset in file and value as array.
 */
public class NetCDFReaderWithMetaMultiFile implements RecordReader<Text, NetCDFArrayWritable> {
    private static final Log LOG
            = LogFactory.getLog(LineRecordReader.class.getName());

    private List<Long> start;

    private List<Long> end;
    private List<NetcdfFile> ncFile;
    private List<Variable> v;
    private long pos;
    private int numberOfElements;

    int currChunk = 0;


    public NetCDFReaderWithMetaMultiFile(Configuration job,
                                NetCDFFileSplit split) throws IOException {


        numberOfElements = split.getFileSplit().getPaths().size();
        for( int i = 0; i < numberOfElements; i++ ) {

            start.add(split.getFileSplit().startChunk.get(i)); //split.getStart();
            end.add(split.getFileSplit().endChunk.get(0)); //start + split.getLength();
            final Path file = split.getFileSplit().getPaths().get(i);

            LOG.info("Map is reading from input: " + file + " start chunk " + start + " end chunk " + end);

            ncFile.add(NetcdfDataset.openFile(file.toString(), null));
            List<Variable> vs = ncFile.get(i).getVariables();
            v.add(vs.get(vs.size() - 1));
            //LOG.info("Variable is "+ v.getFullName());

        }
        this.pos = start.get(0);
    }


    public Text createKey() {
        return new Text();
    }

    public NetCDFArrayWritable createValue() {
        return new NetCDFArrayWritable();
    }

    /** Read a line. */
    public synchronized boolean next(Text key, NetCDFArrayWritable value)
            throws IOException {


        if (pos < end.get(currChunk)) {
            key.set(String.valueOf(pos));
            Array chunk = null;
            try{

                long first = System.currentTimeMillis();

                chunk = ncFile.get(currChunk).readSection("rsut(" + pos + ":" + pos + ",:,:)");

                long second = System.currentTimeMillis();
                LOG.info( "[SAMAN][NetCDFReaderWithMeta][Next] read time = " + (second - first) );

            } catch (ucar.ma2.InvalidRangeException e)
            {
                LOG.info("section error " + e);
            }
            if (chunk == null) {LOG.info("chunk is null");return false;}
            LOG.info(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));
            System.out.println(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));
            int dimensionsSize = v.get(currChunk).getDimensions().size();

            long third = System.currentTimeMillis();

            float[] my = (float[])chunk.get1DJavaArray(Float.class);

            long fourth = System.currentTimeMillis();

            System.out.println( "[SAMAN][NetCDFReaderWithMeta][Next] chunk array time = " + (fourth-third) );

            FloatWritable[] fw = new FloatWritable[my.length + dimensionsSize + 1];
            fw[0] = new FloatWritable( dimensionsSize );
            fw[1] = new FloatWritable( pos );
            for (int i = 2; i < dimensionsSize+1; i++ ){
                fw[i] = new FloatWritable(v.get(currChunk).getDimensions().get(i-1).getLength());
            }
            for (int i=dimensionsSize+1; i< my.length+dimensionsSize+1; i++) {
                fw[i]=new FloatWritable(my[i-dimensionsSize-1]);
            }

            //System.out.println( "[SAMAN][NetCDFReaderWithMeta][Next] fw[0]="+fw[0].toString() );

            value.set(fw);
            pos ++;

            if( pos >= end.get(currChunk) ){
                if( currChunk < numberOfElements-1 ){
                    currChunk++;
                    pos = start.get(currChunk);
                }
            }

            return true;

        }
        //LOG.info("Reaching chunk end");

        return false;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() {
        return 0.0f;
        /*
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
        */
    }

    public  synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {

        for( int i = 0; i < numberOfElements; i++ ) {
            if (ncFile.get(i) != null) {
                ncFile.get(i).close();
            }
        }
    }
}
