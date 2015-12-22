package saman;

import java.io.IOException;
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

public class NetCDFTranspose {
    private static final Log LOG = LogFactory.getLog(NetCDFTranspose.class);

    public static class VariableMapper
            extends Mapper<Text, NetCDFArrayWritable, Text, FloatWritable> {


        @Override
        public void map(Text key, NetCDFArrayWritable value, Context context )
                throws IOException, InterruptedException {
            FloatWritable[] records = (FloatWritable[]) value.toArray();
            float[] realValues = new float[records.length];

            System.out.println( "[SAMAN][NetCDFTranspose][Map] latSize="+realValues[0]+",lonSize="+realValues[1] );

            int latSize = (int)(records[0].get());
            int lonSize = (int)(records[1].get());

            for (int i = 0; i < latSize; i++) {
                for (int j = 0; j < lonSize; j++) {
                    int index = i * latSize + j + 2;
                    System.out.println( "[SAMAN][NetCDFTranspose][Map] record is="+records[index].get() );
                    context.write(new Text(key+","+i+","+j), new FloatWritable(records[index].get()));
                }
            }
        }
    }

    public static class FloatMaxReducer
            extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context )
                throws IOException, InterruptedException {

            for (FloatWritable val : values) {
                System.out.println("[SAMAN][NetCDFTranspose][Reduce] key="+key+",value="+val );
                context.write(key, val);
            }
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
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setInputFormatClass(NetCDFInputFormatWithDimensions.class);
        job.setOutputFormatClass(NetCDFOutputFormat.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            NetCDFInputFormatWithDimensions.addInputPath(job, new Path(otherArgs[i]));
        }
        TextOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}