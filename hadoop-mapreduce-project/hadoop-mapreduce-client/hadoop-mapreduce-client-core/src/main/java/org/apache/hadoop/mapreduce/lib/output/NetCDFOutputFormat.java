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
public class NetCDFOutputFormat<Text, NetCDFArrayWritable> extends FileOutputFormat<Text, NetCDFArrayWritable> {

    public static final String NETCDF_INPUT_PATH = "hadoop.netcdf.outputformat.input";

    private static final Log LOG = LogFactory.getLog(NetCDFOutputFormat.class);


    public NetCDFOutputFormat(){
        super();

        //System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] output file name is : " + outputPath.getName() );
        //System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] input file name is: " + conf.get(NetCDFOutputFormat.NETCDF_INPUT_PATH) );



    }

    protected static class NetCDFRecordWriter<Text, NetCDFArrayWritable> extends RecordWriter<Text, NetCDFArrayWritable> {

        public synchronized void write(Text key, NetCDFArrayWritable value)
                throws IOException {

            System.out.println( "[SAMAN][NetCDFRecordWriter][write] Beginning!" );

            String keyString = key.toString();
            String[] keySplitted = keyString.split(",");
            System.out.println( "Lat is: "+keySplitted[0]+",timeDim: "+keySplitted[1]
                    +",latDim: "+keySplitted[2]+",lonDim: "+keySplitted[3] );

            System.out.println( "[SAMAN][NetCDFRecordWriter][write] End!" );

        }

        public synchronized void close(TaskAttemptContext context)
                throws IOException {


        }
    }

    public RecordWriter<Text, NetCDFArrayWritable> getRecordWriter(TaskAttemptContext job) throws
            IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        Path outputPath = getOutputPath(job);

        Path file = getDefaultWorkFile(job, null);
        file.toString()

        System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] file path is: " + file.toString() );
        System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] output path is: " + outputPath.getName() );

        //System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] output file name is : " + outputPath.getName() );
        //System.out.println( "[SAMAN][NetCDFOutputFormat][getRecordWriter] input file name is: " + conf.get(NetCDFOutputFormat.NETCDF_INPUT_PATH) );

        return new NetCDFRecordWriter<Text, NetCDFArrayWritable>();

    }

}
