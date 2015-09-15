package dataOrganization.partitioning.age;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

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
