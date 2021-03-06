package org.apache.hadoop.mapreduce.lib.output;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NetCDFArrayWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import ucar.ma2.*;
import ucar.nc2.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by saman on 12/21/15.
 */
public class NetCDFOutputFormatCompact2DirectSetting<Text, List> extends FileOutputFormat<Text, List> {

    public static final String NETCDF_INPUT_PATH = "hadoop.netcdf.outputformat.input";
    public static final String NETCDF_LOCAL_TEMPFILE_PREFIX = "hadoop.netcdfoutputformat.tempfileprefix";

    private static final Log LOG = LogFactory.getLog(NetCDFOutputFormatCompact2.class);


    public NetCDFOutputFormatCompact2DirectSetting(){
        super();
    }

    protected static class NetCDFRecordWriterCompactForLon<Text, List> extends RecordWriter<Text, List> {

        private FileSystem _fs;
        private TaskAttemptContext _job;
        private Path _output_path;
        private String netCDFLocalTempPrefix;

        public NetCDFRecordWriterCompactForLon( FileSystem _fs, TaskAttemptContext _job ) {

            Configuration conf = _job.getConfiguration();
            this._fs = _fs;
            this._job = _job;
            this._output_path = getOutputPath(_job);
            this.netCDFLocalTempPrefix = conf.get(NETCDF_LOCAL_TEMPFILE_PREFIX);

            // Checking if the temp file exists or not. If yes, remove the file.
        }

        public synchronized void write(Text key, List value)
                throws IOException {

            System.out.println( "[SAMAN][NetCDFRecordWriter][write] Beginning!" );

            //FloatWritable[] records = (FloatWritable[])((org.apache.hadoop.io.NetCDFArrayWritable)value).toArray();
            //for( int i = 0; i < 10; i++ ){
            //    System.out.println( "[SAMAN][NetCDFRecordWriter][Write] Records["+i+"]="+records[i] );
            //}


            //System.out.println( "[SAMAN][NetCDFRecordWriter][Write] records length is: " + records.length );

            String keyString = key.toString();
            String[] keySplitted = keyString.split(",");
            String currentCumulativeLat = keySplitted[0];
            String timeDimSize = keySplitted[1];
            String latDimSize = keySplitted[2];
            String lonDimSize = keySplitted[3];
            System.out.println( "Lat is: "+keySplitted[0]+",timeDim: "+keySplitted[1]
                    +",latDim: "+keySplitted[2]+",lonDim: "+keySplitted[3] );

            int blockSize = 256*1024*1024;
            int chunkSize = Integer.valueOf(timeDimSize)*Integer.valueOf(lonDimSize)*4;
            int numChunksPerKey = (blockSize/chunkSize);
            //boolean isBreak = false;
            //for( int i = 0; i < numChunksPerKey; i++ ){
            //    for( int j = 0; j < Integer.valueOf(timeDimSize); j++ ){
            //        for( int k = 0; k < Integer.valueOf(lonDimSize); k++ ){
            //            if( records[i*Integer.valueOf(timeDimSize)*Integer.valueOf(lonDimSize)+j*Integer.valueOf(lonDimSize)+k] == null )
            //                continue;
            //if( i*Integer.valueOf(timeDimSize)*Integer.valueOf(lonDimSize)+j*Integer.valueOf(lonDimSize)+k >= records.length ) {
            //    isBreak = true;
            //    break;
            //}
            //            System.out.println( "[SAMAN][NetCDFOutputFormatCompact][Write] ("+(i+Integer.valueOf(currentCumulativeLat)*chunkSize)+","+j+","+k+")="+records[i*Integer.valueOf(timeDimSize)*Integer.valueOf(lonDimSize)+j*Integer.valueOf(lonDimSize)+k].get() );
            //        }
            //        if( isBreak == true )
            //            break;
            //    }
            //    if( isBreak == true )
            //       break;
            //}

            int latIndexesSize = ((Integer.valueOf(currentCumulativeLat)+1)*numChunksPerKey <= Integer.valueOf(latDimSize))
                    ? ( numChunksPerKey )
                    : ( Integer.valueOf(latDimSize)-(Integer.valueOf(currentCumulativeLat))*numChunksPerKey );

            System.out.println( "[SAMAN][NetCDFOutputFormatCompact2][Write] latIndexesSize="+latIndexesSize );

            /* Writing partial NetCDF file into the temporary file */

            // Need to be taken out of being static.

            String fileName = "hdfs://c3n2:9000/rsut";
            String outputFileName = "/data/saman/lat-" + currentCumulativeLat + ".nc";
            //String outputFileName = "hdfs://c3n2:9000/rsutlatout";
            NetcdfFile dataFile = null;
            NetcdfFileWriter outputFile = null;

            long first = System.nanoTime();

            try {
                dataFile = NetcdfFile.open(fileName, null);
                outputFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, outputFileName);
                Variable vtime = dataFile.findVariable("time");
                Variable vtime_bnds = dataFile.findVariable("time_bnds");
                Variable vlat = dataFile.findVariable("lat");
                Variable vlat_bnds = dataFile.findVariable("lat_bnds");
                Variable vlon = dataFile.findVariable("lon");
                Variable vlon_bnds = dataFile.findVariable("lon_bnds");
                Variable vrsut = dataFile.findVariable("rsut");

                System.out.println("sizes are = vtime.size=" + vtime.getSize() + ", vtime_bnds.size=" + vtime_bnds.getSize() + ", vlat.size=" + vlat.getSize() +
                        ", vlat_bnds.size=" + vlat_bnds.getSize() + ", vlon.size=" + vlon.getSize() + ", vlon_bnds.size=" + vlon_bnds.getSize() +
                        ", vrsut.size=" + vrsut.getSize());
                System.out.println("dimension names are = " + vtime.getDimensionsString() + "," + vlat.getDimensionsString() + "," +
                        vlon.getDimensionsString() + "," + vrsut.getDimensionsString());

                //Dimension latDim = outputFile.addDimension(null, vlat.getDimensionsString(), (int) (vlat.getSize()));
                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Dimensions.");
                Dimension latDim = outputFile.addDimension(null, vlat.getDimensionsString(), latIndexesSize);
                latDim.setUnlimited(true);
                Dimension timeDim = outputFile.addDimension(null, vtime.getDimensionsString(), (int) (vtime.getSize()));
                Dimension lonDim = outputFile.addDimension(null, vlon.getDimensionsString(), (int) (vlon.getSize()));
                Dimension bndDim = outputFile.addDimension(null, "bnds", 2);
                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] After Dimensions.");
                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] latDim: " + latDim.getLength());


                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before List Dimensions;");
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

                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Variables, with vlat Dimension string: " + vlat.getDimensionsString());
                Variable vlatNew = outputFile.addVariable(null, vlat.getShortName(), vlat.getDataType(), vlat.getDimensionsString());
                Variable vlatbndsNew = outputFile.addVariable(null, vlat_bnds.getShortName(), vlat_bnds.getDataType(), lat_bnds_dim);
                Variable vtimeNew = outputFile.addVariable(null, vtime.getShortName(), vtime.getDataType(), vtime.getDimensionsString());
                Variable vtimebndsNew = outputFile.addVariable(null, vtime_bnds.getShortName(), vtime_bnds.getDataType(), time_bnds_dim);
                Variable vlonNew = outputFile.addVariable(null, vlon.getShortName(), vlon.getDataType(), vlon.getDimensionsString());
                Variable vlonbndsNew = outputFile.addVariable(null, vlon_bnds.getShortName(), vlon_bnds.getDataType(), lon_bnds_dim);
                Variable vrsutNew = outputFile.addVariable(null, vrsut.getShortName(), vrsut.getDataType(), rsut_dim);


                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Attributes;");
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

                long first1 = System.nanoTime();


                ArrayDouble.D1 latArray = (ArrayDouble.D1) vlat.read();
                long first111 = System.nanoTime();
                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLat;");
                Array dataLat = Array.factory(DataType.DOUBLE, new int[]{latIndexesSize});
                long first112 = System.nanoTime();
                int[] shape;
                for( int i = 0; i < latIndexesSize; i++ ){
                    //System.out.println( "[SAMAN][NetCDFOutputFormatCompact][Write] getting lat: " + (Integer.valueOf(currentCumulativeLat)*numChunksPerKey+i) );
                    dataLat.setDouble(i, Double.valueOf(latArray.get(Integer.valueOf(currentCumulativeLat)*numChunksPerKey+i)));
                }
                long first113 = System.nanoTime();

                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLatBnds;");
                ArrayDouble.D2 latBndsArray = (ArrayDouble.D2) vlat_bnds.read();

                long first121 = System.nanoTime();
                Array dataLatBnds = Array.factory(DataType.DOUBLE, new int[]{latIndexesSize, 2});
                long first122 = System.nanoTime();
                shape = dataLatBnds.getShape();
                Index2D idx = new Index2D(new int[]{latIndexesSize, 2});
                //idx.set(0, 0);
                //dataLatBnds.setDouble(idx, latBndsArray.get(Integer.valueOf(currentLat), 0));
                //idx.set(0, 1);
                //dataLatBnds.setDouble(idx, latBndsArray.get(Integer.valueOf(currentLat), 1));
                for (int i = 0; i < latIndexesSize; i++) {
                    for (int j = 0; j < shape[1]; j++) {
                        idx.set(i, j);
                        dataLatBnds.setDouble(idx, latBndsArray.get(Integer.valueOf(currentCumulativeLat)*numChunksPerKey+i, j));
                    }
                }
                long first123 = System.nanoTime();

                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataTime;");
                ArrayDouble.D1 timeArray = (ArrayDouble.D1) vtime.read();

                long first131 = System.nanoTime();

                Array dataTime = Array.factory(DataType.DOUBLE, new int[]{(int) (vtime.getSize())});
                long first132 = System.nanoTime();
                shape = timeArray.getShape();
                for (int i = 0; i < shape[0]; i++) {
                    dataTime.setDouble(i, timeArray.get(i));
                }
                long first133 = System.nanoTime();

                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataTimeBnds;");
                ArrayDouble.D2 timeBndsArray = (ArrayDouble.D2) vtime_bnds.read();

                long first141 = System.nanoTime();

                Array dataTimeBnds = Array.factory(DataType.DOUBLE, new int[]{(int) (vtime.getSize()), 2});

                long first142 = System.nanoTime();

                shape = dataTimeBnds.getShape();
                idx = new Index2D(new int[]{(int) (vtime.getSize()), 2});
                for (int i = 0; i < shape[0]; i++) {
                    for (int j = 0; j < shape[1]; j++) {
                        idx.set(i, j);
                        dataTimeBnds.setDouble(idx, timeBndsArray.get(i, j));
                    }
                }

                long first143 = System.nanoTime();

                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLon;");
                ArrayDouble.D1 lonArray = (ArrayDouble.D1) vlon.read();

                long first151 = System.nanoTime();

                Array dataLon = Array.factory(DataType.DOUBLE, new int[]{(int) (vlon.getSize())});

                long first152 = System.nanoTime();

                shape = lonArray.getShape();
                for (int i = 0; i < shape[0]; i++) {
                    dataLon.setDouble(i, lonArray.get(i));
                }

                long first153 = System.nanoTime();

                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLonBnds;");
                ArrayDouble.D2 lonBndsArray = (ArrayDouble.D2) vlon_bnds.read();

                long first161 = System.nanoTime();

                Array dataLonBnds = Array.factory(DataType.DOUBLE, new int[]{(int) (vlon.getSize()), 2});

                long first162 = System.nanoTime();



                shape = dataLonBnds.getShape();
                idx = new Index2D(new int[]{(int) (vlon.getSize()), 2});
                for (int i = 0; i < shape[0]; i++) {
                    for (int j = 0; j < shape[1]; j++) {
                        idx.set(i, j);
                        dataLonBnds.setDouble(idx, lonBndsArray.get(i, j));
                    }
                }


                long first2 = System.nanoTime();

                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataRsut;");
                //Index3D idx3 = new Index3D(new int[]{latIndexesSize, (int) (vtime.getSize()), (int) (vlon.getSize())});
                ArrayFloat.D3 dataRsut = (ArrayFloat.D3)(Array.factory(DataType.FLOAT, new int[]{latIndexesSize, (int) (vtime.getSize()), (int) (vlon.getSize())}));
                float[][] arrayVersion = new float[((java.util.List<NetCDFArrayWritable>)value).size()][];
                //System.out.println( "[SAMAN][NetCDFRecordWriter][write] ((java.util.List<NetCDFArrayWritable>)value).size() = " + ((java.util.List<NetCDFArrayWritable>)value).size() );
                //System.out.println( "[SAMAN][NetCDFRecordWriter][write] arrayVersion size = " + arrayVersion.length );

                Iterator valueItr = ((java.util.List) value).iterator();
                int counter = 0;
                while( valueItr.hasNext() ){
                    NetCDFArrayWritable temp = (NetCDFArrayWritable)valueItr.next();
                    //System.out.println( "[SAMAN][NetCDFRecordWriter][write] Current array size = " + (temp.get().length-2) );
                    /*
                    if( temp == null )
                        System.out.println( "[SAMAN][NetCDFRecordWriter][write] temp is null!" );
                    if( temp.toArrayFloat() == null )
                        System.out.println( "[SAMAN][NetCDFRecordWriter][write] temp.toArrayFloat() is null!" );
                    if( arrayVersion[counter] == null )
                        System.out.println( "[SAMAN][NetCDFRecordWriter][write] arrayVersion[counter] is null!" );
                    */
                    arrayVersion[counter] = (float[])temp.toArrayFloat();
                    counter++;
                }
                dataRsut.setUseDirectNetCDF(true);
                dataRsut.setNetcdfContentSize( ((java.util.List<NetCDFArrayWritable>)value).get(0).get().length-2 );
                dataRsut.setNetcdfContents( arrayVersion );

                //System.out.println( "[SAMAN][NetCDFRecordWriter][write] AxBxC = " + latIndexesSize * (int)(vtime.getSize()) * (int)(vlon.getSize()) );

                //System.out.println( "[SAMAN][NetCDFRecordWriter][write] AxB = " + ((java.util.List)value).size()*(((java.util.List<NetCDFArrayWritable>)value).get(0).get().length-2) );

                //System.out.println( "[SAMAN][NetCDFRecordWriter][write] bigIndex size is: " + ((java.util.List)value).size() );
                //System.out.println( "[SAMAN][NetCDFRecordWriter][write] content size is: " + (((java.util.List<NetCDFArrayWritable>)value).get(0).get().length-2)    );

                //System.out.println( "[SAMAN][NetCDFOutputFormat][Write] class is: " + dataRsut.getClass().getName() );
                //int globalIndex = 0

                //for( int i = 0; i < latIndexesSize; i++ ) {
                //    long first21 = System.nanoTime();
                //    for (int j = 0; j < vtime.getSize(); j++) {
                //        long first22 = System.nanoTime();

                //        NetCDFArrayWritable netCDFArrayWritable = ((java.util.List<NetCDFArrayWritable>)value).get(globalIndex);

                        //dataRsut.addNetCDFElement(netCDFArrayWritable);
                        //FloatWritable[] records = (FloatWritable[])netCDFArrayWritable.toArray();
                        //long first221 = System.nanoTime();
                        //((java.util.List<NetCDFArrayWritable>)value).addAll((java.util.List<NetCDFArrayWritable>)value);
                        //System.out.println( "[SAMAN][NetCDFOutputFormat][Write] first221-first22=" + (first221-first22) );
                        //for (int k = 0; k < vlon.getSize(); k++) {
                        //    long first23 = System.nanoTime();
                        //    try {
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] before idx.set("+i+"," + j + "," + k + ")");
                                //long first231 = System.nanoTime();
                                //idx3.set(i, j, k);
                        //        long first232 = System.nanoTime();
                                //System.out.println( "[SAMAN][NetCDFOutputFormat][Write] first232-first231=" + (first232-first231) );
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] after idx.set("+i+"," + j + "," + k + ")");
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] idx3 is: " + idx3);
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] index to get: " + (j * Integer.valueOf(lonDimSize) + k));
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] value is: "
                                //        + records[j * Integer.valueOf(lonDimSize) + k].get());
                                //dataRsut.setFloat(idx3, records[2 + k].get());
                                //dataRsut.setFloat(idx3, (((FloatWritable[])netCDFArrayWritable.get())[2+k]).get());
                         //       dataRsut.set(i, j, k, (((FloatWritable[]) netCDFArrayWritable.get())[2+k]).get());
                         //       long first233 = System.nanoTime();
                         //       System.out.println( "[SAMAN][NetCDFOutputFormat][Write] first233-first232=" + (first233-first232) );

                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] after dataRsut.setFloat(..)");
                         //   } catch (Exception e) {
                         //       e.printStackTrace();
                         //       System.out.println("[SAMAN][NetCDFOutputFormat][Write] Exception in rsut = " + e.getMessage());
                         //       throw e;
                         //   }
                         //   long first24 = System.nanoTime();
                         //   System.out.println( "[SAMAN][NetCDFOutputFormat][write] first24-first23=" + (first24-first23) );

                //        globalIndex++;
                //        long first25 = System.nanoTime();
                //        System.out.println( "[SAMAN][NetCDFOutputFormat][write] first25-first22=" + (first25-first22) );
                //    }
                //    long first26 = System.nanoTime();
                //    System.out.println( "[SAMAN][NetCDFOutputFormat][write] first26-first21=" + (first26-first21) );
                //}

                //dataRsut.setUseDirectNetCDF(true);
                //dataRsut.setNetcdfContentSize( ((java.util.List<NetCDFArrayWritable>)value).get(0).get().length - 2 );
                //dataRsut.setNetcdfContents( (java.util.List<NetCDFArrayWritable>)value );

                long first3 = System.nanoTime();

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Write;");

                //int storageListSize = dataRsut.getStorageSize();
                //System.out.println( "[SAMAN][NetCDFOutputFormat][Write] storage size at this moment is: " + storageListSize );

                dataRsut.getStorageFirstHundred();
                outputFile.create();
                outputFile.write(vlatNew, dataLat);
                outputFile.write(vlatbndsNew, dataLatBnds);
                outputFile.write(vtimeNew, dataTime);
                outputFile.write(vtimebndsNew, dataTimeBnds);
                outputFile.write(vlonNew, dataLon);
                outputFile.write(vlonbndsNew, dataLonBnds);
                outputFile.write(vrsutNew, dataRsut);
                outputFile.close();

                long second = System.nanoTime();

                _fs.copyFromLocalFile(new Path(outputFileName), new Path(_output_path + "/rsutlat" + currentCumulativeLat));
                //_fs.moveFromLocalFile(new Path(outputFileName), new Path(_output_path + "/rsutlat" + currentCumulativeLat));

                long third = System.nanoTime();

                System.out.println( "[SAMAN][NetCDFOutputFormat][write]" +
                        " first-first1=" + (first-first1) +
                        ", first111-first1=" + (first111-first1) +
                        ", first112-first111=" + (first112 - first111) +
                        ", first113-first112=" + (first113 - first112) +
                        ", first121-first113=" + (first121-first113) +
                        ", first122-first121=" + (first122-first121) +
                        ", first123-first122=" + (first123-first122) +
                        ", first131-first123=" + (first131-first123) +
                        ", first132-first131=" + (first132-first131) +
                        ", first133-first132=" + (first133-first132) +
                        ", first141-first133=" + (first141-first133) +
                        ", first142-first141=" + (first142-first141) +
                        ", first143-first142=" + (first143-first142) +
                        ", first151-first143=" + (first151-first143) +
                        ", first152-first151=" + (first152-first151) +
                        ", first153-first152=" + (first153-first152) +
                        ", first161-first153=" + (first161-first153) +
                        ", first162-first161=" + (first162-first161) +
                        ", first2-first162=" + (first2-first162) +
                        ", first3-first2=" + (first3-first2) +
                        ", second-first3=" + (second-first3) +
                        ", third-second=" + (third-second)  );

            } catch (Exception e) {
                System.out.println("[SAMAN][NetCDFOutputFormat][write] Exception in end = " + e.getMessage());
                throw new IOException(e);
            }

            System.out.println( "[SAMAN][NetCDFRecordWriter][write] End!" );

        }

        public synchronized void close(TaskAttemptContext context)
                throws IOException {
            // TODO: Maybe we can close the NetCDF file here??!!

        }
    }

    public RecordWriter<Text, List> getRecordWriter(TaskAttemptContext job) throws
            IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        Path outputPath = getOutputPath(job);
        FileSystem _fs = outputPath.getFileSystem(job.getConfiguration());

        System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] output path is: " + outputPath.getName() );

        return new NetCDFRecordWriterCompactForLon<Text, List>( _fs, job );

    }

}
