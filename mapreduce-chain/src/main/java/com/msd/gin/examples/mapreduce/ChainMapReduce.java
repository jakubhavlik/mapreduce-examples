package com.msd.gin.examples.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("inputDirs outputDir");
            return 1;
        }

        String originalOutput = args[args.length - 1] + File.separator + System.currentTimeMillis();
        String tempPath = args[args.length - 1] + File.separator + "tmp" + File.separator + System.currentTimeMillis();
        args[args.length - 1] = tempPath;

        Configuration conf = getConf();

        Job job1 = Job.getInstance(conf);
        job1.setJobName("Chained MapReduce - stage 1");
        job1.setJarByClass(ChainMapReduce.class);

        // input
        addInputPaths(job1, args);

        // mapper
        job1.setMapperClass(MyMapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        // reducer
        job1.setReducerClass(MyReducer1.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(1);

        // output
        job1.setOutputFormatClass(TextOutputFormat.class);
        addOutputPath(job1, args);

        String[] secondArgs = { tempPath, originalOutput };
        Job job2 = Job.getInstance(conf);
        job2.setJobName("Chained MapReduce - stage 2");
        job2.setJarByClass(ChainMapReduce.class);

        // input
        job2.setInputFormatClass(TextInputFormat.class);
        addInputPaths(job2, secondArgs);

        // mapper
        job2.setMapperClass(MyMapper2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        // reducer
        job2.setReducerClass(MyReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setNumReduceTasks(1);

        // output
        job2.setOutputFormatClass(TextOutputFormat.class);
        addOutputPath(job2, secondArgs);

        JobControl jbcntrl = new JobControl("jbcntrl");
        ControlledJob controlledJob1 = new ControlledJob(job1, null);
        List<ControlledJob> dependingJobs = Arrays.asList(new ControlledJob[] { controlledJob1 });
        ControlledJob controlledJob2 = new ControlledJob(job2, dependingJobs);
        jbcntrl.addJob(controlledJob1);
        jbcntrl.addJob(controlledJob2);

        handleRun(jbcntrl);

        /**
         * To cleanup after yourself type:
         * FileSystem fs = FileSystem.get(getConf());
         * fs.delete(new Path(tempPath), true);
         */

        return 0;
    }

    public void handleRun(JobControl control) throws InterruptedException {
        JobRunner runner = new JobRunner(control);
        Thread t = new Thread(runner);
        t.start();

        while (!control.allFinished()) {
            Thread.sleep(500);
        }
        control.stop();
    }

    private void addInputPaths(Job job, String[] args) throws IOException {
        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
    }

    private void addOutputPath(Job job, String[] args) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ChainMapReduce(), args);
    }

    private class JobRunner implements Runnable {
        private JobControl control;

        public JobRunner(JobControl control) {
            this.control = control;
        }

        @Override
        public void run() {
            this.control.run();
        }
    }
}
