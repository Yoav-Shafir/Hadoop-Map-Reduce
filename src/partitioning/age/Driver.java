package partitioning.age;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
			System.exit(2);
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int numReduce = Integer.parseInt(args[2]);

		Job job = new Job(conf,
				"find the maximum scorer in each gender and three age categories");

		job.setJarByClass(Driver.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setNumReduceTasks(numReduce);
		job.setPartitionerClass(AgePartitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(AgeMapper.class);
		job.setReducerClass(AgeReducer.class);

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
