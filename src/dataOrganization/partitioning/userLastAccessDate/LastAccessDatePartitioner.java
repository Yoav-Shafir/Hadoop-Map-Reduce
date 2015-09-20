package dataOrganization.partitioning.userLastAccessDate;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;

// The partitioner examines each key/value pair output by the mapper to
// determine which partition the key/value pair will be written.

public class LastAccessDatePartitioner extends Partitioner<IntWritable, Text>
		implements Configurable {

	private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

	private Configuration conf = null;
	private int minLastAccessDateYear = 0;

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	// The setConf method is called during task
	// construction to configure the partitioner.
	// The driver is responsible for calling LastAccess
	// DatePartitioner.setMinLastAccessDate during job configuration.
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		// Here, the minimum value of the last access
		// date is pulled from the configuration.
		minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);

	}
	
	// This date is used to subtract from each key (last access date) 
	// to determine what partition it goes to.
	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return key.get() - minLastAccessDateYear;
	}

	/**
	 * Sets the minimum possible last access date to subtract from each key to
	 * be partitioned<br>
	 * <br>
	 * 
	 * That is, if the last min access date is "2011" and the key to partition
	 * is "2012", it will go to partition 2012 - 2011 = 1
	 * 
	 * @param job
	 *            The job to configure
	 * @param minLastAccessDateYear
	 *            The minimum access date.
	 */
	public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {
		job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,
				minLastAccessDateYear);
	}

}
