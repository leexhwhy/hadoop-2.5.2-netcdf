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
    public class NetCDFReaderWithMeta implements RecordReader<Text, NetCDFArrayWritable> {
      private static final Log LOG
        = LogFactory.getLog(LineRecordReader.class.getName());

      private long start;
      private long pos;
      private long end;
      private NetcdfFile ncFile;
      private Variable v;

      public NetCDFReaderWithMeta(Configuration job,
                              FileSplit split) throws IOException {
        start = split.getFileSplit().startChunk; //split.getStart();
        end = split.getFileSplit().endChunk; //start + split.getLength();
        final Path file = split.getPath();

        //LOG.info("Map is reading from input: " + file +" start chunk "+ start+" end chunk "+end);

        ncFile = NetcdfDataset.openFile(file.toString(), null);
        List<Variable> vs = ncFile.getVariables();
        v = vs.get(vs.size()-1);
        //LOG.info("Variable is "+ v.getFullName());

        this.pos = start;
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
            chunk = ncFile.readSection("rsut("+pos+":"+pos+",:,:)");
          } catch (ucar.ma2.InvalidRangeException e)
          {
            LOG.info("section error " + e);
          }
          if (chunk == null) {LOG.info("chunk is null");return false;}
          //LOG.info(chunk.getSize()+" elements and "+chunk.getSizeBytes()+" bytes, shape is "+Arrays.toString(chunk.getShape()));
          int dimensionsSize = v.getDimensions().size();
          float[] my = (float[])chunk.get1DJavaArray(Float.class);
          FloatWritable[] fw = new FloatWritable[my.length + dimensionsSize + 1];
          fw[0] = new FloatWritable( dimensionsSize );
          fw[1] = new FloatWritable( pos );
          for (int i = 2; i < dimensionsSize+1; i++ ){
        fw[i] = new FloatWritable(v.getDimensions().get(i-1).getLength());
          }
          for (int i=dimensionsSize+1; i< my.length+dimensionsSize+1; i++) {
            fw[i]=new FloatWritable(my[i-dimensionsSize-1]);
          }
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
