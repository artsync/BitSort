package nl.arthurvlug.hadoop.bitsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MergeSort {
	public static void main(String[] args) throws Exception {
		new MergeSort().runJob(new Path(args[0]), new Path(args[1]));
	}

	void runJob(Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

		Job job = new Job(conf);
		
		job.setOutputKeyClass(BitString.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(OrderedTextSortComparator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MergeSortMapper.class);
		job.setReducerClass(MergeSortReducer.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}

}
