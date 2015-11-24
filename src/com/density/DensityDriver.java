package com.density;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The DensityDriver class runs the MapReduce job
 */
public class DensityDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception{
		if(args.length !=2) {
			System.err.println("Usage: DensityDriver <input path> <outputpath>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(DensityDriver.class);
		job.setJobName("Population-weighted house density");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		job.setMapperClass(DensityMapper.class);
		job.setReducerClass(DensityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0:1); 

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		DensityDriver driver = new DensityDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}