package org.apache.hadoop.mapreduce.lib.output;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by saman on 12/21/15.
 */
public class NetCDFOutputFormat<Text, FloatWritable> extends FileOutputFormat<Text, FloatWritable> {

    public static final String NETCDF_INPUT_PATH = "hadoop.netcdf.outputformat.input";

    private static final Log LOG = LogFactory.getLog(NetCDFOutputFormat.class);


    public NetCDFOutputFormat(){
        super();
        Configuration conf = new Configuration();

        //System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] output file name is : " + outputPath.getName() );
        System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] input file name is: " + conf.get(NetCDFOutputFormat.NETCDF_INPUT_PATH) );



    }

    protected static class NetCDFRecordWriter<Text, FloatWritable> extends RecordWriter<Text, FloatWritable> {

        public synchronized void write(Text key, FloatWritable value)
                throws IOException {



        }

        public synchronized void close(TaskAttemptContext context)
                throws IOException {



        }
    }

    public RecordWriter<Text, FloatWritable> getRecordWriter(TaskAttemptContext job) throws
            IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        Path outputPath = getOutputPath(job);

        //System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] output file name is : " + outputPath.getName() );
        //System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] input file name is: " + conf.get(NetCDFOutputFormat.NETCDF_INPUT_PATH) );

        return new NetCDFRecordWriter<Text, FloatWritable>();

    }

}
