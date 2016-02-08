package saman;

import java.io.IOException;
import java.lang.*;
import java.lang.Float;
import java.lang.Integer;
import java.lang.InterruptedException;
import java.lang.Override;
import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.NetCDFInputFormatWithDimensions;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NetCDFOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NetCDFOutputFormatCompact;
import org.apache.hadoop.mapreduce.lib.output.NetCDFOutputFormatCompact2;
import org.apache.hadoop.mapreduce.lib.output.NetCDFOutputFormatCompact2Reduced;
//import org.apache.hadoop.mapred.NetCDFInputFormat;
import org.apache.hadoop.io.NetCDFArrayWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucar.ma2.*;
import ucar.nc2.*;

public class NetCDFTransposeCompactForTime {
    private static final Log LOG = LogFactory.getLog(NetCDFTransposeCompactForTime.class);

    public static class VariableMapper
            extends Mapper<Text, NetCDFArrayWritable, Text, NetCDFArrayWritable> {


        @Override
        public void map(Text key, NetCDFArrayWritable value, Context context )
                throws IOException, InterruptedException {

            int blockSize = 512*1024*1024;

            FloatWritable[] records = (FloatWritable[]) value.toArray();

            int timeSize = (int)(records[0].get());
            int latSize = (int)(records[1].get());
            int lonSize = (int)(records[2].get());

            int chunkSize = (latSize*lonSize*4);
            int numChunksPerKey = (int)(blockSize / chunkSize);

            //for( int i = 0; i < timeSize; i++ ){
                FloatWritable[] result = new FloatWritable[1+latSize*lonSize];
                NetCDFArrayWritable resultNetCDF = new NetCDFArrayWritable();
                result[0] = new FloatWritable(Float.valueOf(key.toString()));
                //result[1] = new FloatWritable(Float.valueOf(key.toString()));
                //System.out.println( "[SAMAN][NetCDFTransposeCompact2][Map] key="+(i/numChunksPerKey) );
                java.lang.System.arraycopy(records, 0, result, 1, lonSize*latSize);
                resultNetCDF.set(result);
                context.write( new Text(Integer.toString(Integer.valueOf(key.toString())/numChunksPerKey)+","+timeSize+","+latSize+","+lonSize), resultNetCDF );
            //}

        }
    }


    public static class MergeChunkReducer
            extends Reducer<Text,NetCDFArrayWritable,Text,List> {

        @Override
        public void reduce(Text key, Iterable<NetCDFArrayWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int blockSize = 512*1024*1024;

            System.out.println( "[SAMAN][NetCDFTranspose2][Reducer] Reducer Beginning!" );
            //for( Text value : values ){
            //    String stringValue = value.toString();
            //    String[] parts = stringValue.split(",");
            //    System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Row:" +
            //            parts[0]+","+key+","+parts[1]+","+parts[2] );
            //}

            NetCDFArrayWritable result = new NetCDFArrayWritable();

            String keyString = key.toString();
            String[] dimensions = keyString.split(",");
            int accumulatedTimeIndex = Integer.valueOf(dimensions[0]);
            int timeDim = Integer.valueOf(dimensions[1]);
            int latDim = Integer.valueOf(dimensions[2]);
            int lonDim = Integer.valueOf(dimensions[3]);

            int chunkSize = (latDim*lonDim*4);
            int numChunksPerKey = (int)(blockSize / chunkSize);

            System.out.println("[SAMAN][NetCDFTranspose2][Reducer] " +
                    "timeDim=" + timeDim + ",latDim=" + latDim + ",lonDim=" + lonDim);

            System.out.println("[SAMAN][NetCDFTranposeCompact2][reduce] blockSize=" + blockSize + ",chunkSize=" + chunkSize + ",numChunksPerKey=" + numChunksPerKey);

            List listNetCDFArrayWritable = new LinkedList<NetCDFArrayWritable>();

            Iterator itr = values.iterator();
            while( itr.hasNext() ){
                NetCDFArrayWritable array = (NetCDFArrayWritable)itr.next();
                NetCDFArrayWritable newArray = new NetCDFArrayWritable();
                FloatWritable[] arrayFloat = (FloatWritable[])array.toArray();
                newArray.set(arrayFloat);
                //FloatWritable[] records = (FloatWritable[]) array.toArray();
                //System.out.println( "[SAMAN][NetCDFTransposeCompact2][reduce] latIndex="+array.get()[0]+", timeIndex="+array.get()[1] );
                listNetCDFArrayWritable.add(newArray);
            }

            Collections.sort(listNetCDFArrayWritable);

            itr = listNetCDFArrayWritable.iterator();
            //while( itr.hasNext() ){
            //    NetCDFArrayWritable array = (NetCDFArrayWritable)itr.next();
            //System.out.println( "[SAMAN][NetCDFTransposeCompact2][reduce] latIndex="+array.get()[0]+", timeIndex="+array.get()[1] );
            //}

            /*
            FloatWritable[] fw = new FloatWritable[numChunksPerKey*timeDim*lonDim];

            for( Text value : values ){
                String valueString = value.toString();
                String[] valueParts = valueString.split(",");
                int latIndex = Integer.valueOf(valueParts[0]);
                latIndex = latIndex - ((int)(latIndex/numChunksPerKey))*numChunksPerKey;
                int timeIndex = Integer.valueOf(valueParts[1]);
                int lonIndex = Integer.valueOf(valueParts[2]);
                //System.out.println( "[SAMAN][NetCDFTransposeCompact][Reducer] set index("+timeIndex
                //        +","+latIndex+","+lonIndex+") with value="+valueParts[3]
                //        +", and fw index of " + (latIndex*timeDim*lonDim+timeIndex*lonDim+lonIndex));
                fw[latIndex*timeDim*lonDim+timeIndex*lonDim+lonIndex] = new FloatWritable(Float.valueOf(valueParts[3]));
            }

            result.set( fw );
            */



            //System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Reducer Ending!" );

            context.write( key, listNetCDFArrayWritable );


        }

    }
    public static class FloatMaxReducer
            extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context )
                throws IOException, InterruptedException {

            //System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Reducer Beginning!" );

            String fileName = "hdfs://master:9000/rsutout";
            NetcdfFile dataFile = null;

            try {
                dataFile = NetcdfFile.open(fileName, null);
                //outputFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, outputFileName);
                Variable vtime = dataFile.findVariable("time");
                Variable vtime_bnds = dataFile.findVariable("time_bnds");
                Variable vlat = dataFile.findVariable("lat");
                Variable vlat_bnds = dataFile.findVariable("lat_bnds");
                Variable vlon = dataFile.findVariable("lon");
                Variable vlon_bnds = dataFile.findVariable("lon_bnds");
                Variable vrsut = dataFile.findVariable("rsut");

                //System.out.println("sizes are = vtime.size=" + vtime.getSize() + ", vtime_bnds.size=" + vtime_bnds.getSize() + ", vlat.size=" + vlat.getSize() +
                //        ", vlat_bnds.size=" + vlat_bnds.getSize() + ", vlon.size=" + vlon.getSize() + ", vlon_bnds.size=" + vlon_bnds.getSize() +
                //        ", vrsut.size=" + vrsut.getSize());
                //System.out.println("dimension names are = " + vtime.getDimensionsString() + "," + vlat.getDimensionsString() + "," +
                //        vlon.getDimensionsString() + "," + vrsut.getDimensionsString());
            }catch( Exception e ){
                System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Exception!" );
                System.out.println( "[SAMAN][NetCDFTranspose][Reducer] " + e.getMessage() );
            }

            //for (FloatWritable val : values) {
            //    System.out.println("[SAMAN][NetCDFTranspose][Reduce] key="+key+",value="+val );
            //    context.write(key, val);
            //}
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: NetCDFTranspose <in> <out>");
            System.exit(2);
        }

        int numPriority=0;
        try
        {
            numPriority = Integer.parseInt(otherArgs[2]);
            System.err.println("IO weight from "+otherArgs[2]);
        }
        catch (Exception e)
        {

            numPriority = 1;
        }
        if (numPriority <0)
        {
            numPriority=1;
        }
        Job job = new Job(conf, "NetCDFTransposeCompactForTime");
        job.setJarByClass(NetCDFTransposeCompactForTime.class);
        job.setMapperClass(VariableMapper.class);
        //job.setCombinerClass(Reducer.class);
        job.setReducerClass(MergeChunkReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NetCDFArrayWritable.class);
        job.setOutputValueClass(List.class);
        job.setInputFormatClass(NetCDFInputFormatWithDimensions.class);
        job.setOutputFormatClass(NetCDFOutputFormatCompactForTime.class);
        job.setNumReduceTasks(59);
        String singleInput = otherArgs[0];
        conf.set( NetCDFOutputFormat.NETCDF_INPUT_PATH, singleInput );

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            NetCDFInputFormatWithDimensions.addInputPath(job, new Path(otherArgs[i]));
        }
        TextOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}