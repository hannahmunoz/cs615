package hw1_mapreduce.hw1_mapreduce;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wordcount {
	// both mapper and reducer function are similar to what we went over in class 
	// however this (https://www.youtube.com/watch?v=JDk-LYJMzEU&t=677s) is the tutorial I used to learn AWS
	// so it more closely resembles his code
	
	// basic mapper function
	public static class tweetMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			// current word
			StringTokenizer st = new StringTokenizer(value.toString());
			Text wordOut = new Text();
			// intial found value
			IntWritable one = new IntWritable (1);
			
			// loop through file until all words have been counted and found
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				//locate hashtag
				if (token.startsWith("#")) {
					//split in case of mutliple tags
					String[] tokenList = token.split("#");
					for (String i : tokenList) {
						wordOut.set(i);
						context.write(wordOut, one);
					}
				}
			}
		}
	}
	
	// basic reducer function
	public static class hashReducer extends Reducer <Text, IntWritable,  IntWritable, Text>{
		public void reduce(Text term, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException  {
			//top 100
			//for (int i = 0; i < 100; i++) {
				int count = 0;
				// count all the occurances of the tweet
				while (value.iterator().hasNext()) {
					count++;
					value.iterator().next();
				}
				
				// write it to a file
				IntWritable output = new  IntWritable (count);
				context.write(output, term);
			}
			//}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// get arguements
		Configuration conf = new Configuration();
		String[] argurments = new GenericOptionsParser (conf,args).getRemainingArgs();
		
		if (argurments.length !=2 ) {
			System.err.println("Not enough arguements <input file> <output file>");
			System.exit(2);
		}
		
		// set up tasks
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(wordcount.class);
		//gets and reduces all hashtag
		job.setMapperClass(tweetMapper.class);
		job.setReducerClass(hashReducer.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// set up input and output files
		FileInputFormat.addInputPath(job, new Path(argurments[0]));
		FileOutputFormat.setOutputPath(job, new Path(argurments[1]));
		
		// exit upon completion
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit( 0 );
		}else {
			System.exit( 1 );
		}
	}

}
