package saman;

import java.io.IOException;
import java.lang.Float;
import java.lang.Integer;
import java.lang.InterruptedException;
import java.lang.Override;
import java.util.List;
import java.util.Iterator;
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
//import org.apache.hadoop.mapred.NetCDFInputFormat;
import org.apache.hadoop.io.NetCDFArrayWritable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucar.ma2.*;
import ucar.nc2.*;

public class NetCDFTranspose {
    private static final Log LOG = LogFactory.getLog(NetCDFTranspose.class);

    public static class VariableMapper
            extends Mapper<Text, NetCDFArrayWritable, Text, Text> {


        @Override
        public void map(Text key, NetCDFArrayWritable value, Context context )
                throws IOException, InterruptedException {
            FloatWritable[] records = (FloatWritable[]) value.toArray();

            int timeSize = (int)(records[0].get());
            int latSize = (int)(records[1].get());
            int lonSize = (int)(records[2].get());

            //System.out.println( "[SAMAN][NetCDFTranspose][Map] length is = " + records.length );

            //System.out.println( "[SAMAN][NetCDFTranspose][Map] latSize="+latSize+",lonSize="+lonSize );

            for( int i = 0; i < latSize; i++ ){
                for( int j = 0; j < lonSize; j++ ){
                    int index = i*lonSize+j+3;
                    context.write( new Text(Integer.toString(i)+","+timeSize+","+latSize+","+lonSize),
                            new Text(key+","+j+","+records[index].get()) );
                }
            }

        }
    }


    public static class MergeChunkReducer
            extends Reducer<Text,Text,Text,NetCDFArrayWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Reducer Beginning!" );
            //for( Text value : values ){
            //    String stringValue = value.toString();
            //    String[] parts = stringValue.split(",");
            //    System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Row:" +
            //            parts[0]+","+key+","+parts[1]+","+parts[2] );
            //}

            NetCDFArrayWritable result = new NetCDFArrayWritable();

            String keyString = key.toString();
            String[] dimensions = keyString.split(",");
            int timeDim = Integer.valueOf(dimensions[1]);
            int latDim = Integer.valueOf(dimensions[2]);
            int lonDim = Integer.valueOf(dimensions[3]);

            System.out.println( "[SAMAN][NetCDFTranspose][Reducer] " +
                    "timeDim="+timeDim+",latDim="+latDim+",lonDim="+lonDim);

            FloatWritable[] fw = new FloatWritable[timeDim*lonDim];

            for( Text value : values ){
                String valueString = value.toString();
                String[] valueParts = valueString.split(",");
                int timeIndex = Integer.valueOf(valueParts[0]);
                int lonIndex = Integer.valueOf(valueParts[1]);
                System.out.println( "[SAMAN][NetCDFTranspose][Reducer] set index("+timeIndex
                        +","+Integer.valueOf(dimensions[0])+","+lonIndex+") with value="+valueParts[2]
                        +", and fw index of " + (timeIndex*timeDim+lonIndex));
                fw[timeIndex*lonDim+lonIndex] = new FloatWritable(Float.valueOf(valueParts[2]));
            }

            result.set( fw );

            System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Reducer Ending!" );

            context.write( key, result );


        }

    }
    public static class FloatMaxReducer
            extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context )
                throws IOException, InterruptedException {

            System.out.println( "[SAMAN][NetCDFTranspose][Reducer] Reducer Beginning!" );

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

                System.out.println("sizes are = vtime.size=" + vtime.getSize() + ", vtime_bnds.size=" + vtime_bnds.getSize() + ", vlat.size=" + vlat.getSize() +
                        ", vlat_bnds.size=" + vlat_bnds.getSize() + ", vlon.size=" + vlon.getSize() + ", vlon_bnds.size=" + vlon_bnds.getSize() +
                        ", vrsut.size=" + vrsut.getSize());
                System.out.println("dimension names are = " + vtime.getDimensionsString() + "," + vlat.getDimensionsString() + "," +
                        vlon.getDimensionsString() + "," + vrsut.getDimensionsString());
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
        Job job = new Job(conf, "NetCDFTranspose");
        job.setJarByClass(NetCDFTranspose.class);
        job.setMapperClass(VariableMapper.class);
        //job.setCombinerClass(Reducer.class);
        job.setReducerClass(MergeChunkReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(NetCDFArrayWritable.class);
        job.setInputFormatClass(NetCDFInputFormatWithDimensions.class);
        job.setOutputFormatClass(NetCDFOutputFormat.class);
        job.setNumReduceTasks(2);
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