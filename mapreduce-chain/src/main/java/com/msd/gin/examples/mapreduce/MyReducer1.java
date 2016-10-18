package com.msd.gin.examples.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer1 extends Reducer<Text, NullWritable, NullWritable, Text> {

    private static final NullWritable NULL = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(NULL, key);
    }

}