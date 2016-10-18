package com.msd.gin.examples.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

public class OrcMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(OrcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(OrcReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);
        job.setOutputFormatClass(OrcOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new OrcMapReduce(), args);
        System.exit(exitCode);
    }

    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("inputDirs outputDir");
            return null;
        }
        conf.set("orc.compress", "SNAPPY");
        conf.set("hive.exec.orc.default.compress", "SNAPPY");
        conf.set("orc.mapred.output.schema", OrcReducer.SCHEMA);
        Job job = Job.getInstance(conf);

        OrcOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setJarByClass(tool.getClass());
        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        OrcOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + File.separator + System.currentTimeMillis()));
        return job;
    }

}