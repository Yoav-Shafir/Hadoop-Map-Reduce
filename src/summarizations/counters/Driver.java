package summarizations.counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountNumUsersByState <users> <out>");
			System.exit(2);
		}
		
		Path input = new Path(otherArgs[0]);
		Path outputDir = new Path(otherArgs[1]);

		Job job = new Job(conf, "Count Num Users By State");
		job.setJarByClass(Driver.class);

		job.setMapperClass(CountNumUsersByStateMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		int code = job.waitForCompletion(true) ? 0 : 1;
		
		// grabbing the counters after the job completes
		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
				System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
			}
		}

		// Clean up empty output directory
		FileSystem.get(conf).delete(outputDir, true);
		return code;
	}
	
	public static void main(String[] args) throws Exception {
		int returnCode = ToolRunner.run(new Driver(), args);
		System.exit(returnCode);
	}
}
