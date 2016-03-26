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

import java.util.Iterator;
import java.util.LinkedList;
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
public class NetCDFReaderWithMetaPartToMemoryMultiSplit implements RecordReader<Text, NetCDFArrayWritable> {
    private static final Log LOG
            = LogFactory.getLog(NetCDFReaderWithMetaPartToMemoryMultiSplit.class.getName());

    //private long start;
    private long pos;
    //private long end;
    //private long startTime = -1;
    //private long endTime = -1;
    //private long startLat = -1;
    //private long endLat = -1;
    //private long startLon = -1;
    //private long endLon = -1;

    private List<Long> start = new LinkedList<Long>();

    private List<Long> end = new LinkedList<Long>();
    private List<NetcdfFile> ncFile = new LinkedList<NetcdfFile>();
    private List<Variable> v = new LinkedList<Variable>();

    private List<Long> startTime = new LinkedList<Long>();
    private List<Long> endTime = new LinkedList<Long>();
    private List<Long> startLat = new LinkedList<Long>();
    private List<Long> endLat = new LinkedList<Long>();
    private List<Long> startLon = new LinkedList<Long>();
    private List<Long> endLon = new LinkedList<Long>();

    private int numberOfElements;

    int currChunk = 0;


    //private NetcdfFile ncFile;
    //private Variable v;

    public NetCDFReaderWithMetaPartToMemoryMultiSplit(Configuration job,
                                                        NetCDFFileSplit split) throws IOException {

        numberOfElements = split.getFileSplit().getPaths().size();
        for( int i = 0; i < numberOfElements; i++ ) {

            start.add(split.getFileSplit().startChunk.get(i)); //split.getStart();
            end.add(split.getFileSplit().endChunk.get(i)); //start + split.getLength();
            startTime.add(split.getFileSplit().timeStartLimit.get(i));
            endTime.add(split.getFileSplit().timeEndLimit.get(i));
            startLat.add(split.getFileSplit().latStartLimit.get(i));
            endLat.add(split.getFileSplit().latEndLimit.get(i));
            startLon.add(split.getFileSplit().lonStartLimit.get(i));
            endLon.add(split.getFileSplit().lonEndLimit.get(i));
            final Path file = split.getFileSplit().getPaths().get(i);

            //LOG.info("Map is reading from input: " + file + " start chunk " + start + " end chunk " + end);

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
            ArrayFloat.D3 chunk = null;
            int dimensionsSize = 0;
            try{

                //long first = System.currentTimeMillis();
                while(pos < end.get(currChunk)) {

                    dimensionsSize = v.get(currChunk).getDimensions().size();

                    //long third = System.currentTimeMillis();

                    if( startLat.get(currChunk) == -1 ) startLat.set(currChunk, (long)0);
                    if( endLat.get(currChunk) == -1 ) endLat.set(currChunk,  v.get(currChunk).getDimensions().get(1).getLength());
                    if( startLon.get(currChunk) == -1 ) startLon.set(currChunk, (long)0);
                    if( endLon.get(currChunk) == -1 ) endLon.set(currChunk, v.get(currChunk).getDimensions().get(2).getLength());
                    if( startTime.get(currChunk) != -1 ){
                        if( pos < startTime.get(currChunk) ){
                            System.out.println( "[SAMAN] section skipped!" );
                            pos++;
                            if( pos >= end.get(currChunk) ){
                                if( currChunk < numberOfElements-1 ){
                                    currChunk++;
                                    pos = start.get(currChunk);
                                }
                            }
                            continue;
                        }
                    }
                    if( endTime.get(currChunk) != -1 ){
                        if( pos > endTime.get(currChunk) ){
                            System.out.println( "[SAMAN] section skipped!" );
                            pos++;
                            if( pos >= end.get(currChunk) ){
                                if( currChunk < numberOfElements-1 ){
                                    currChunk++;
                                    pos = start.get(currChunk);
                                }
                            }
                            continue;
                        }
                    }

                    chunk = (ArrayFloat.D3)(ncFile.readSection("rsut(" + pos + ":" + pos + ","+startLat.get(currChunk)+":"+(endLat.get(currChunk)-1)+","+startLon.get(currChunk)+":"+(endLon.get(currChunk)-1)+")"));

                    break;
                }

                //long second = System.currentTimeMillis();
                //LOG.info( "[SAMAN][NetCDFReaderWithMeta][Next] read time = " + (second - first) );

            } catch (ucar.ma2.InvalidRangeException e)
            {
                LOG.info("section error " + e);
            }
            if (chunk == null) {LOG.info("chunk is null");return false;}
            LOG.info(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));
            System.out.println(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));


            System.out.println( "[SAMAN] startLat = " + startLat + ", endLat = " + endLat + ", startLon = " + startLon + ", endLon = " + endLon );

            FloatWritable[] fw = new FloatWritable[(int)(endLat.get(currChunk) - startLat.get(currChunk))*(int)(endLon.get(currChunk) - startLon.get(currChunk)) + dimensionsSize + 1];
            fw[0] = new FloatWritable( dimensionsSize );
            fw[1] = new FloatWritable( pos );
            fw[2] = new FloatWritable(endLat.get(currChunk) - startLat.get(currChunk));
            fw[3] = new FloatWritable(endLon.get(currChunk) - startLon.get(currChunk));


            /*
            int[] shape = chunk.getShape();
            //System.out.println( "[SAMAN] shape[0]=" + shape[1] +", shape[1]="+shape[2] );
            for( int i = 0; i < shape[1]; i++ ){
               if( i < startLat || i >= endLat ){
                   //System.out.println( "[SAMAN] Gone out of lat!" );
                   continue;
               }
                for( int j = 0; j < shape[2]; j++ ){
                    if( j < startLon || j >= endLon ){
                        //System.out.println( "[SAMAN] Gone out of lon!" );
                        continue;
                    }
                    fw[4+position] = new FloatWritable( chunk.get( 0, i, j ) );
                    //System.out.println( "[SAMNA] FW is " + fw[4+position].get() );
                    position++;
                }
            }

            */

            //System.out.println( "[SAMAN] position is: " + position );

            float[] my = (float[])chunk.get1DJavaArray(Float.class);

            //long fourth = System.currentTimeMillis();
            //long lonSize = v.getDimensions().get(2).getLength();
            //for( int i = (int)startLat; i < endLat; i++ ){
            //    int baseIndex = i*(int)lonSize;
            //    for( int j = (int)startLon; j < endLon; j++ ){
            //        //System.out.println( "[SAMAN] baseIndex+j = " + my[ baseIndex + j ] );
            //        fw[4+position] = new FloatWritable( my[ baseIndex + j ] );
            //        position++;
            //    }
            //}

            //System.out.println( "[SAMAN][NetCDFReaderWithMeta][Next] chunk array time = " + (fourth-third) );

            //FloatWritable[] fw = new FloatWritable[my.length + dimensionsSize + 1];
            //fw[0] = new FloatWritable( dimensionsSize );
            //fw[1] = new FloatWritable( pos );
            //for (int i = 2; i < dimensionsSize+1; i++ ){
            //    fw[i] = new FloatWritable(v.getDimensions().get(i-1).getLength());
            //}
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
    }

    public  synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        if (ncFile != null) {
            Iterator itr = ncFile.iterator();
            while(itr.hasNext()){
                NetcdfFile temp = (NetcdfFile)itr.next();
                temp.close();
            }
        }
    }
}
