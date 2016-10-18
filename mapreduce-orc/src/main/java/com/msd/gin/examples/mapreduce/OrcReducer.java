package com.msd.gin.examples.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

public class OrcReducer extends Reducer<Text, LongWritable, NullWritable, OrcStruct> {

    public static final String SCHEMA = "struct<animal:string,count:bigint>";

    private TypeDescription schema = TypeDescription.fromString(SCHEMA);
    private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
    private final NullWritable nullWritable = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        pair.setFieldValue(0, key);
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        pair.setFieldValue(1, new LongWritable(sum));
        context.write(nullWritable, pair);
    }

}
