package com.msd.gin.examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperUsers extends Mapper<LongWritable, Text, UserPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        context.write(new UserPair(columns[0], "0"), new Text(columns[1]));
    }
}