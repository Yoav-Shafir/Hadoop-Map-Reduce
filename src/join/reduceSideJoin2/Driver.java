package join.reduceSideJoin2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new Driver(), args);
	}

	// the Grouping comparator compares the keys, and groups similar keys into a
	// single reduce call,
	// and in this case, the requirement is to send all the records having same
	// value of
	// productId in ProductIdKey class, regardless of the recordType value,
	public static class JoinGroupingComparator extends WritableComparator {
		public JoinGroupingComparator() {
			super(ProductIdKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			ProductIdKey first = (ProductIdKey) a;
			ProductIdKey second = (ProductIdKey) b;

			// The compare method of the WritableComparator uses only the
			// productId,
			// ensuring that the records having same productId will be sent to
			// the same reduce call.
			return first.productId.compareTo(second.productId);
		}
	}

	// This sorts the records making sure that the first record is the Product
	// record,
	// and then all the data comes.
	public static class JoinSortingComparator extends WritableComparator {
		public JoinSortingComparator() {
			super(ProductIdKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			ProductIdKey first = (ProductIdKey) a;
			ProductIdKey second = (ProductIdKey) b;

			return first.compareTo(second);
		}
	}

	@Override
	public int run(String[] allArgs) throws Exception {
		String[] args = new GenericOptionsParser(getConf(), allArgs)
				.getRemainingArgs();

		Job job = Job.getInstance(getConf());
		job.setJarByClass(Driver.class);

		// Set the InputFormat for the job.
		job.setInputFormatClass(TextInputFormat.class);
		// Set the OutputFormat for the job.
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the key class for the map output data. This allows the user to
		// specify the map output key class
		// to be different than the final output value class.
		job.setMapOutputKeyClass(ProductIdKey.class);
		// Set the value class for the map output data. This allows the user to
		// specify the map output
		// value class to be different than the final output value class.
		job.setMapOutputValueClass(JoinGenericWritable.class);

		// Add a Path with a custom InputFormat and Mapper to the list of inputs
		// for the map-reduce job.
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, ProductMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, SalesOrderDataMapper.class);

		// Define the comparator that controls which keys are grouped together
		// for a single call to Reducer.
		// reduce(Object, Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		job.setGroupingComparatorClass(JoinGroupingComparator.class);
		// Define the comparator that controls how the keys are sorted before
		// they are passed to the Reducer.
		job.setSortComparatorClass(JoinSortingComparator.class);

		// Set the Reducer for the job.
		job.setReducerClass(JoinRecuder.class);

		// job.setNumReduceTasks(0);

		// Set the key class for the job output data.
		job.setOutputKeyClass(NullWritable.class);
		// Set the value class for job outputs.
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
		boolean status = job.waitForCompletion(true);
		if (status) {
			return 0;
		} else {
			return 1;
		}
	}
}
