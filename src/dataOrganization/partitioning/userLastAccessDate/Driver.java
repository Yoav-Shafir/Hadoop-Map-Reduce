package dataOrganization.partitioning.userLastAccessDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: PartitionedUsers <users> <outdir>");
			System.exit(2);
		}

		Job job = new Job(conf, "PartitionedUsers");
		job.setJarByClass(Driver.class);

		job.setMapperClass(LastAccessDateMapper.class);

		// Set custom partitioner and min last access date.
		job.setPartitionerClass(LastAccessDatePartitioner.class);
		// we want data only from the last 4 years.
		LastAccessDatePartitioner.setMinLastAccessDate(job, 2011);

		// we've set the minimum last access data to 2011,
		// which gives us 4 years of valid data.
		// wa want 1 partition for each year - 4 partitions.
		job.setNumReduceTasks(4);
		job.setReducerClass(LastAccessDateReducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapred.textoutputformat.separator", "");

		if (job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int returnCode = ToolRunner.run(new Driver(), args);
		System.exit(returnCode);
	}
}
