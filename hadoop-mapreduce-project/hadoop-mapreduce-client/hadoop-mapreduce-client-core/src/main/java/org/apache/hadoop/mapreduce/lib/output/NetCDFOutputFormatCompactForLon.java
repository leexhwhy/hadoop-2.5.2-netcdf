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
public class NetCDFOutputFormatCompactForLon<Text, List> extends FileOutputFormat<Text, List> {

    public static final String NETCDF_INPUT_PATH = "hadoop.netcdf.outputformat.input";
    public static final String NETCDF_LOCAL_TEMPFILE_PREFIX = "hadoop.netcdfoutputformat.tempfileprefix";

    private static final Log LOG = LogFactory.getLog(NetCDFOutputFormatCompact2.class);


    public NetCDFOutputFormatCompactForLon(){
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
            String currentCumulativeLon = keySplitted[0];
            String timeDimSize = keySplitted[1];
            String latDimSize = keySplitted[2];
            String lonDimSize = keySplitted[3];
            System.out.println( "Lon is: "+keySplitted[0]+",timeDim: "+keySplitted[1]
                    +",latDim: "+keySplitted[2]+",lonDim: "+keySplitted[3] );

            int blockSize = 256*1024*1024;
            int chunkSize = Integer.valueOf(timeDimSize)*Integer.valueOf(latDimSize)*4;
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

            int lonIndexesSize = ((Integer.valueOf(currentCumulativeLon)+1)*numChunksPerKey <= Integer.valueOf(lonDimSize))
                    ? ( numChunksPerKey )
                    : ( Integer.valueOf(lonDimSize)-(Integer.valueOf(currentCumulativeLon))*numChunksPerKey );

            System.out.println( "[SAMAN][NetCDFOutputFormatCompact2][Write] latIndexesSize="+lonIndexesSize );

            /* Writing partial NetCDF file into the temporary file */

            // Need to be taken out of being static.

            String fileName = "hdfs://c3n2:9000/rsut";
            String outputFileName = "/data/saman/lon-" + currentCumulativeLon + ".nc";
            NetcdfFile dataFile = null;
            NetcdfFileWriter outputFile = null;

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
                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Dimensions.");
                Dimension lonDim = outputFile.addDimension(null, vlon.getDimensionsString(), lonIndexesSize);
                lonDim.setUnlimited(true);
                Dimension timeDim = outputFile.addDimension(null, vtime.getDimensionsString(), (int) (vtime.getSize()));
                Dimension latDim = outputFile.addDimension(null, vlat.getDimensionsString(), (int) (vlat.getSize()));
                Dimension bndDim = outputFile.addDimension(null, "bnds", 2);
                System.out.println("[SAMAN][NetCDFOutputFormat][Write] After Dimensions.");
                System.out.println("[SAMAN][NetCDFOutputFormat][Write] latDim: " + latDim.getLength());


                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before List Dimensions;");
                java.util.List<Dimension> time_bnds_dim = new ArrayList<Dimension>();
                java.util.List<Dimension> lat_bnds_dim = new ArrayList<Dimension>();
                java.util.List<Dimension> lon_bnds_dim = new ArrayList<Dimension>();
                java.util.List<Dimension> rsut_dim = new ArrayList<Dimension>();

                lon_bnds_dim.add(lonDim);
                lon_bnds_dim.add(bndDim);
                time_bnds_dim.add(timeDim);
                time_bnds_dim.add(bndDim);
                lat_bnds_dim.add(latDim);
                lat_bnds_dim.add(bndDim);
                rsut_dim.add(lonDim);
                rsut_dim.add(timeDim);
                rsut_dim.add(latDim);

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Variables, with vlat Dimension string: " + vlat.getDimensionsString());
                Variable vlonNew = outputFile.addVariable(null, vlon.getShortName(), vlon.getDataType(), vlon.getDimensionsString());
                Variable vlonbndsNew = outputFile.addVariable(null, vlon_bnds.getShortName(), vlon_bnds.getDataType(), lon_bnds_dim);
                Variable vtimeNew = outputFile.addVariable(null, vtime.getShortName(), vtime.getDataType(), vtime.getDimensionsString());
                Variable vtimebndsNew = outputFile.addVariable(null, vtime_bnds.getShortName(), vtime_bnds.getDataType(), time_bnds_dim);
                Variable vlatNew = outputFile.addVariable(null, vlat.getShortName(), vlat.getDataType(), vlat.getDimensionsString());
                Variable vlatbndsNew = outputFile.addVariable(null, vlat_bnds.getShortName(), vlat_bnds.getDataType(), lat_bnds_dim);
                Variable vrsutNew = outputFile.addVariable(null, vrsut.getShortName(), vrsut.getDataType(), rsut_dim);


                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Attributes;");

                java.util.List<Attribute> attributes = vlon.getAttributes();
                Iterator itr = attributes.iterator();
                while (itr.hasNext()) {
                    Attribute attribute = (Attribute) itr.next();
                    vlonNew.addAttribute(attribute);
                }

                attributes = vtime.getAttributes();
                 itr = attributes.iterator();
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
                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLon;");
                Array dataLon = Array.factory(DataType.DOUBLE, new int[]{lonIndexesSize});
                int[] shape;
                for( int i = 0; i < lonIndexesSize; i++ ){
                    //System.out.println( "[SAMAN][NetCDFOutputFormatCompact][Write] getting lat: " + (Integer.valueOf(currentCumulativeLat)*numChunksPerKey+i) );
                    dataLon.setDouble(i, Double.valueOf(lonArray.get(Integer.valueOf(currentCumulativeLon)*numChunksPerKey+i)));
                }

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLonBnds;");
                ArrayDouble.D2 lonBndsArray = (ArrayDouble.D2) vlon_bnds.read();
                Array dataLonBnds = Array.factory(DataType.DOUBLE, new int[]{lonIndexesSize, 2});
                shape = dataLonBnds.getShape();
                Index2D idx = new Index2D(new int[]{lonIndexesSize, 2});
                //idx.set(0, 0);
                //dataLatBnds.setDouble(idx, latBndsArray.get(Integer.valueOf(currentLat), 0));
                //idx.set(0, 1);
                //dataLatBnds.setDouble(idx, latBndsArray.get(Integer.valueOf(currentLat), 1));
                for (int i = 0; i < lonIndexesSize; i++) {
                    for (int j = 0; j < shape[1]; j++) {
                        idx.set(i, j);
                        dataLonBnds.setDouble(idx, lonBndsArray.get(Integer.valueOf(currentCumulativeLon)*numChunksPerKey+i, j));
                    }
                }

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataTime;");
                ArrayDouble.D1 timeArray = (ArrayDouble.D1) vtime.read();
                Array dataTime = Array.factory(DataType.DOUBLE, new int[]{(int) (vtime.getSize())});
                shape = timeArray.getShape();
                for (int i = 0; i < shape[0]; i++) {
                    dataTime.setDouble(i, timeArray.get(i));
                }

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataTimeBnds;");
                ArrayDouble.D2 timeBndsArray = (ArrayDouble.D2) vtime_bnds.read();
                Array dataTimeBnds = Array.factory(DataType.DOUBLE, new int[]{(int) (vtime.getSize()), 2});
                shape = dataTimeBnds.getShape();
                idx = new Index2D(new int[]{(int) (vtime.getSize()), 2});
                for (int i = 0; i < shape[0]; i++) {
                    for (int j = 0; j < shape[1]; j++) {
                        idx.set(i, j);
                        dataTimeBnds.setDouble(idx, timeBndsArray.get(i, j));
                    }
                }

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLat;");
                ArrayDouble.D1 latArray = (ArrayDouble.D1) vlat.read();
                Array dataLat = Array.factory(DataType.DOUBLE, new int[]{(int) (vlat.getSize())});
                shape = latArray.getShape();
                for (int i = 0; i < shape[0]; i++) {
                    dataLat.setDouble(i, latArray.get(i));
                }

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataLatBnds;");
                ArrayDouble.D2 latBndsArray = (ArrayDouble.D2) vlat_bnds.read();
                Array dataLatBnds = Array.factory(DataType.DOUBLE, new int[]{(int) (vlat.getSize()), 2});
                shape = dataLatBnds.getShape();
                idx = new Index2D(new int[]{(int) (vlat.getSize()), 2});
                for (int i = 0; i < shape[0]; i++) {
                    for (int j = 0; j < shape[1]; j++) {
                        idx.set(i, j);
                        dataLatBnds.setDouble(idx, latBndsArray.get(i, j));
                    }
                }

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before DataRsut;");
                Index3D idx3 = new Index3D(new int[]{lonIndexesSize, (int) (vtime.getSize()), (int) (vlat.getSize())});
                Array dataRsut = Array.factory(DataType.FLOAT, new int[]{lonIndexesSize, (int) (vtime.getSize()), (int) (vlat.getSize())});
                int globalIndex = 0;

                for( int i = 0; i < lonIndexesSize; i++ ) {
                    for (int j = 0; j < vtime.getSize(); j++) {
                        NetCDFArrayWritable netCDFArrayWritable = ((java.util.List<NetCDFArrayWritable>)value).get(globalIndex);
                        FloatWritable[] records = (FloatWritable[])netCDFArrayWritable.toArray();
                        for (int k = 0; k < vlat.getSize(); k++) {
                            try {
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] before idx.set("+i+"," + j + "," + k + ")");
                                idx3.set(i, j, k);
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] after idx.set("+i+"," + j + "," + k + ")");
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] idx3 is: " + idx3);
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] index to get: " + (j * Integer.valueOf(lonDimSize) + k));
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] value is: "
                                //        + records[j * Integer.valueOf(lonDimSize) + k].get());
                                dataRsut.setFloat(idx3, records[2+k].get());
                                //System.out.println("[SAMAN][NetCDFOutputFormat][Write] after dataRsut.setFloat(..)");
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Exception in rsut = " + e.getMessage());
                                throw e;
                            }
                        }
                        globalIndex++;
                    }
                }

                System.out.println("[SAMAN][NetCDFOutputFormat][Write] Before Write;");
                outputFile.create();
                outputFile.write(vlonNew, dataLon);
                outputFile.write(vlonbndsNew, dataLonBnds);
                outputFile.write(vtimeNew, dataTime);
                outputFile.write(vtimebndsNew, dataTimeBnds);
                outputFile.write(vlatNew, dataLat);
                outputFile.write(vlatbndsNew, dataLatBnds);
                outputFile.write(vrsutNew, dataRsut);
                outputFile.close();

                _fs.copyFromLocalFile(new Path(outputFileName), new Path(_output_path + "/rsutlon" + currentCumulativeLon));

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
