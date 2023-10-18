package countReviews;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class countReviews {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		IntWritable one = new IntWritable(1);
		
		Text userId = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] fields = line.split(",");
			
			if(fields.length>=2) {
				String user = fields[0].trim();
				String category = fields[1].trim();
				
				if(category.equals("Instruments")) {
					userId.set(user);
					context.write(userId, one);
				}
			}
			
			
		}
		
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int totalReviews = 0;
			
			for (IntWritable value : values) {
				totalReviews += value.get();
			}
			
			result.set(totalReviews);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Reviews");
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
	}
