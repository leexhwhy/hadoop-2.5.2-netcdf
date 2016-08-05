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
import java.util.Random;

/**
 * Treats keys as offset in file and value as array.
 */
public class NetCDFReaderWithMetaRandomSecond implements RecordReader<Text, NetCDFArrayWritable> {
    private static final Log LOG
            = LogFactory.getLog(LineRecordReader.class.getName());

    private long start;
    private long pos;
    private long end;
    private NetcdfFile ncFile;
    private Variable v;
    private int timeLength = 280;
    private int latLength = 241;
    private int lonLenth = 480;
    private int rand;

    public NetCDFReaderWithMetaRandomSecond(Configuration job,
                                FileSplit split) throws IOException {
        start = split.getFileSplit().startChunk; //split.getStart();
        end = split.getFileSplit().endChunk; //start + split.getLength();
        final Path file = split.getPath();

        LOG.info("Map is reading from input: " + file +" start chunk "+ start+" end chunk "+end);

        ncFile = NetcdfDataset.openFile(file.toString(), null);
        List<Variable> vs = ncFile.getVariables();
        for( int i = 0; i < vs.size(); i++ ){
            System.out.println( "[SAMAN][NetCDFReaderWithMeta]variable is: " + vs.get(i).getName() );
        }
        v = vs.get(vs.size()-1);
        //LOG.info("Variable is "+ v.getFullName());

        this.pos = start;

        Random nr = new Random();
        int i = nr.nextInt() % 3;
        if ( i == 0 || i == 1 ){
            rand = 1;
            this.pos = 0;
            this.end = 280;
        }else{
            i = nr.nextInt() % 2;
            if ( i == 1 ){
                this.pos = 0;
                this.end = 241;
                rand = 1;
            }else{
                this.pos = 0;
                this.end = 480;
                rand = 2;
            }
        }

        System.out.println ( "[SAMAN][NetCDFReaderWithMeta] rand is " + rand );

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

        if (pos < end) {
            key.set(String.valueOf(pos));
            Array chunk = null;
            try{

                long first = System.currentTimeMillis();
                if (rand == 1) {
                    chunk = ncFile.readSection("rsut(" + pos + ":" + pos + ",:,:)");
                }
                else if (rand == 2) {
                    chunk = ncFile.readSection("rsut(:," + pos + ":" + pos + ",:)");
                }
                else if (rand == 3){
                    chunk = ncFile.readSection("rsut(:,:," + pos + ":" + pos + ")");
                }

                long second = System.currentTimeMillis();
                //LOG.info( "[SAMAN][NetCDFReaderWithMeta][Next] read time = " + (second - first) );

            } catch (ucar.ma2.InvalidRangeException e)
            {
                LOG.info("section error " + e);
            }
            if (chunk == null) {LOG.info("chunk is null");return false;}
            //LOG.info(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));
            System.out.println(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));
            int dimensionsSize = v.getDimensions().size();

            //long third = System.currentTimeMillis();

            float[] my = (float[])chunk.get1DJavaArray(Float.class);

            // long fourth = System.currentTimeMillis();

            //System.out.println( "[SAMAN][NetCDFReaderWithMeta][Next] chunk array time = " + (fourth-third) );

            FloatWritable[] fw = new FloatWritable[my.length + dimensionsSize + 1];
            fw[0] = new FloatWritable( dimensionsSize );
            fw[1] = new FloatWritable( pos );
            for (int i = 2; i < dimensionsSize+1; i++ ){
                fw[i] = new FloatWritable(v.getDimensions().get(i-1).getLength());
            }
            for (int i=dimensionsSize+1; i< my.length+dimensionsSize+1; i++) {
                fw[i]=new FloatWritable(my[i-dimensionsSize-1]);
            }

            //System.out.println( "[SAMAN][NetCDFReaderWithMeta][Next] fw[0]="+fw[0].toString() );

            value.set(fw);
            pos ++;
            return true;

        }
        //LOG.info("Reaching chunk end");

        return false;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    public  synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        if (ncFile != null) {
            ncFile.close();
        }
    }
}
