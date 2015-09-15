package dataOrganization.structuredToHierarchical;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: PostCommentHierarchy <posts> <comments> <outdir>");
			System.exit(2);
		}

		Job job = new Job(conf, "PostCommentHierarchy");
		job.setJarByClass(Driver.class);
		
		// create a MultipleInputs object and add the comments path and 
		// the posts path with their respective mappers.
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PostMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(PostCommentHierarchyReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		if(job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		int returnCode = ToolRunner.run(new Driver(), args);
		System.exit(returnCode);
	}
}
