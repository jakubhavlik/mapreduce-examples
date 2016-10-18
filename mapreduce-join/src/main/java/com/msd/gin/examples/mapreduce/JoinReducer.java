package com.msd.gin.examples.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<UserPair, Text, Text, Text> {

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text userName = new Text(iter.next());
        while (iter.hasNext()) {
            Text animalSold = iter.next();
            Text row = new Text(userName.toString() + "\t" + animalSold.toString());
            context.write(key.getFirst(), row);
        }
    }
}