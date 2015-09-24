package dataOrganization.partitioning.age;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// The default org.apache.hadoop.mapreduce.lib.partition.hashPartitioner 
// has the following implementation of getPartition function:
/*
 * 
 * public int getPartition(K key, V value, int numReduceTasks) {
    	return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
 * 
 * */
// The logical & with the hashCode is to prevent negative value to be returned if -2147483648 
// is returned by key.getHashCode function.

public class AgePartitioner extends Partitioner<Text, Text> {
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {

		String[] nameAgeScore = value.toString().split("\t");
		String age = nameAgeScore[1];
		int ageInt = Integer.parseInt(age);

		// this is done to avoid performing mod with 0
		if (numReduceTasks == 0)
			return 0;

		// if the age is <20, assign partition 0
		if (ageInt <= 20) {
			return 0;
		}
		// else if the age is between 20 and 50, assign partition 1
		if (ageInt > 20 && ageInt <= 50) {
			return 1 % numReduceTasks;
		}
		// otherwise assign partition 2
		else
			return 2 % numReduceTasks;
	}

}
