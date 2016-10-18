package com.msd.gin.examples.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setPartitionerClass(JoinPartitioner.class);
        job.setGroupingComparatorClass(FirstComparator.class);

        job.setMapOutputKeyClass(UserPair.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new JoinMapReduce(), args);
        System.exit(exitCode);
    }

    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("inputDirs outputDir");
            return null;
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(tool.getClass());

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperSales.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperUsers.class);

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + File.separator + System.currentTimeMillis()));
        return job;
    }

}