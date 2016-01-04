package yiqi;

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
import org.apache.hadoop.mapreduce.lib.input.NetCDFInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NetCDFInputFormatYiqi;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapred.NetCDFInputFormat;
import org.apache.hadoop.io.NetCDFArrayWritable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NetCDF{
private static final Log LOG = LogFactory.getLog(NetCDF.class);
  public static class VariableMapper 
       extends Mapper<Text, NetCDFArrayWritable, Text, FloatWritable>{
    
    @Override  
    public void map(Text key, NetCDFArrayWritable value, Context context
                    ) throws IOException, InterruptedException {
      FloatWritable [] records = (FloatWritable[])value.toArray();	
      float[] realValues = new float[records.length];
      //int numberOfDimensions = (int)(records[0].get());
      //int[] dimensionsSize = new int[numberOfDimensions];
      //String dimensionsSizeString = new String(); 	
      //for (int i=0;i< numberOfDimensions;i++){
      //	dimensionsSize[i] = (int)(records[i+1].get());
      //	dimensionsSizeString = dimensionsSizeString + dimensionsSize[i] + ",";
      //}			 
      //for (int i=0;i< records.length;i++)
      //{
      //  realValues[i]=records[i].get();
      //}
      //Arrays.sort(realValues);
      float max = records[records.length-1].get();
      //LOG.info("M Writing out " + key + " " + max);
      //LOG.info( "[SAMAN] Number of dimensions is: " + numberOfDimensions + " " + dimensionsSizeString );
      		
      context.write(key, new FloatWritable(max));
    }
  }
  
  public static class Reducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private ArrayList<Float> temperatureList = new ArrayList<Float>();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      	

      for (FloatWritable val : values) {
	//LOG.info( "value added was " + val.get() );
        temperatureList.add(val.get());
      }
      Collections.sort(temperatureList);
      int size  = temperatureList.size(); 
      float maxValue = temperatureList.get(size -1);
      context.write(key, new FloatWritable(maxValue));
      //LOG.info("R Writing out "+maxValue);
      temperatureList.clear();
    }
  }


    
  public static void main(String[] args) throws Exception {
    //get the args w/o generic hadoop args
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: NetCDF <in> <out>");
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
    conf.setInt("mapred.job.ioweight", numPriority);
    System.err.println("IO weight "+numPriority);

    Job job = new Job(conf, "NetCDF");
    job.setJarByClass(NetCDF.class);
    job.setMapperClass(VariableMapper.class);
    job.setCombinerClass(Reducer.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setInputFormatClass(NetCDFInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      NetCDFInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    TextOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
