package advertisement;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AdvertisementAnalysis {
	
	public static void main(String[] args) throws IOException,	ClassNotFoundException, InterruptedException 
	{
		
		//Setting up paths manually instead of command line arguments
		Path input_path = new Path("hdfs://localhost:9000/user/jaime/input/FacebookAdvertisement/");
		Path output_dir = new Path("hdfs://localhost:9000/user/jaime/out/FacebookAdvertisement/");

		//Creating new job
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Facebook analysis");

		// name of driver class
		job.setJarByClass(AdvertisementAnalysis.class);
		// name of mapper class
		job.setMapperClass(AdvertisementMapper.class);
		// name of reducer class
		job.setReducerClass(AdvertisementReducer.class);
					
		//Types of key value pairs
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//paths for file input and output
		FileInputFormat.addInputPath(job, input_path);
		FileOutputFormat.setOutputPath(job, output_dir);
		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,	true);

		job.waitForCompletion(true);
	}
}
