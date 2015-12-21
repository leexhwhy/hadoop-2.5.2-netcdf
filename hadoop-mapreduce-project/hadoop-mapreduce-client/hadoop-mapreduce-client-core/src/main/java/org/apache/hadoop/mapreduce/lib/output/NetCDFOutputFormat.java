package org.apache.hadoop.mapreduce.lib.output;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by saman on 12/21/15.
 */
public class NetCDFOutputFormat<Text, FloatWritable> extends FileOutputFormat<Text, FloatWritable> {

    private static final Log LOG = LogFactory.getLog(NetCDFOutputFormat.class);

    public RecordWriter<Text, FloatWritable> getRecordWriter(TaskAttemptContext job) throws
            IOException, InterruptedException {

        

        return null;

    }

    public synchronized void write(Text key, FloatWritable value)
            throws IOException {



    }

}
