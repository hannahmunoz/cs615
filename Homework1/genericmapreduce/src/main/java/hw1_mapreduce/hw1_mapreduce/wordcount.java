package hw1_mapreduce.hw1_mapreduce;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wordcount {
	
	////////////////////////////////
	/// 		City Mapper		///
	//////////////////////////////
	
	 public static class cityMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			// current line
			String[] line = value.toString().split("\n");
			Text wordOut = new Text();
			// intial found value
			IntWritable one = new IntWritable (1);
			//locate day
			for (String x: line) {
				String [] temp = x.split("\t");
				String word = temp [1];
			   //for (int i = 2; i < temp.length; i++) {
				//	word.concat(temp[i]);
				//}
				wordOut.set(word);
				context.write(wordOut, one);
			}
		}
	}
	
	
	public static class hashReducer extends Reducer <Text, IntWritable, Text,  IntWritable>{
		public void reduce(Text term, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException  {
			//top 100
				int count = 0;
				// count all the occurances of the tweet
				while (value.iterator().hasNext()) {
					count++;
					value.iterator().next();
				}
				
				// write it to a file
				IntWritable output = new  IntWritable (count);
				context.write(term, output);
			}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// get arguements
		Configuration conf = new Configuration();
		String[] argurments = new GenericOptionsParser (conf,args).getRemainingArgs();
		
		if (argurments.length !=2 ) {
			System.err.println("Not enough arguements <users file> <output file>");
			System.exit(2);
		}
		
		// set up tasks
		Job job = Job.getInstance(conf, "City Count");
		job.setJarByClass(wordcount.class);
		//gets and reduces all days
		job.setMapperClass(cityMapper.class);
		job.setReducerClass(hashReducer.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);    
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// set up input and output files
		FileInputFormat.addInputPath(job, new Path(argurments[0]));
		//MultipleInputs.addInputPath(job, new Path(argurments[0]), TextInputFormat.class, cityMapper.class);
		//MultipleInputs.addInputPath(job, new Path(argurments[1]), TextInputFormat.class, IDMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(argurments[1]));
		
		// exit upon completion
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit( 0 );
		}else {
			System.exit( 1 );
		}
	}
	
	////////////////////////////////
	/// 		ID Mapper		///
	//////////////////////////////
	
	/*public static class IDMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			// current line
			String[] line = value.toString().split("\n");
			Text wordOut = new Text();
			// intial found value
			IntWritable one = new IntWritable (1);
			//locate day
			for (String x: line) {
				StringTokenizer token = new StringTokenizer(x);
				if (token.hasMoreTokens()) {
					String temp = token.nextToken();	
					if ( temp.startsWith ("1") || temp.startsWith("2") ||
								 temp.startsWith("3") || temp.startsWith("4") ||
								 temp.startsWith("5") || temp.startsWith("6") ||
								 temp.startsWith("7") || temp.startsWith("8") ||
								 temp.startsWith("9") || temp.startsWith("0") &&
								(temp.endsWith("1") || temp.endsWith("2") ||
										temp.endsWith("3") || temp.endsWith("4") ||
										temp.endsWith("5") || temp.endsWith("6") ||
										temp.endsWith("7") || temp.endsWith("8") ||
										temp.endsWith("9") || temp.endsWith("0")) && 
								!temp.contains("-") && !temp.contains(":") && !temp.contains("/") && !temp.contains(",") 
								&& !temp.contains(".") && !temp.contains("%") && !temp.contains("&")   ) {
								
							wordOut.set(temp);
							context.write(wordOut, one);
					}
				}
			}
		}
	}
	
	public static class hashReducer extends Reducer <Text, IntWritable, Text, IntWritable>{
		public void reduce(Text term, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException  {
			//top 100
				int count = 0;
				// count all the occurances of the tweet
				while (value.iterator().hasNext()) {
					count++;
					value.iterator().next();
				}
				
				// write it to a file
				IntWritable output = new  IntWritable (count);
				context.write (term, output);
			}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// get arguements
		Configuration conf = new Configuration();
		String[] argurments = new GenericOptionsParser (conf,args).getRemainingArgs();
		
		if (argurments.length !=2 ) {
			System.err.println("Not enough arguements <tweets> <output file>");
			System.exit(2);
		}
		
		// set up tasks
		Job job = Job.getInstance(conf, "Day Count");
		job.setJarByClass(wordcount.class);
		//gets and reduces all days
		job.setMapperClass(IDMapper.class);
		job.setReducerClass(hashReducer.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);    
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
	}	*/

	
	////////////////////////////////
	/// 		Day Mapper		///
	//////////////////////////////
	
/*	public static class dayMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			// current line
			String[] line = value.toString().split("\n");
			Text wordOut = new Text();
			// intial found value
			IntWritable one = new IntWritable (1);
			//locate day
			for (String x: line) {
				if (x.length() > 37) {
					String temp = x.substring(x.length()-19, x.length()-9);
					
					if (temp.startsWith("200") && (temp.endsWith("1") || temp.endsWith("2") ||
												 temp.endsWith("3") || temp.endsWith("4") ||
												 temp.endsWith("5") || temp.endsWith("6") ||
												 temp.endsWith("7") || temp.endsWith("8") ||
												 temp.endsWith("9") || temp.endsWith("0") )) {
						wordOut.set(temp);
						context.write(wordOut, one);
					}
				}
			}
		}
	}
	
	public static class hashReducer extends Reducer <Text, IntWritable, Text, IntWritable >{
		public void reduce(Text term, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException  {
			//top 100
				int count = 0;
				// count all the occurances of the tweet
				while (value.iterator().hasNext()) {
					count++;
					value.iterator().next();
				}
				
				// write it to a file
				IntWritable output = new  IntWritable (count);
				context.write(term, output);

			}
		

		
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
		Job job = Job.getInstance(conf, "Day Count");
		job.setJarByClass(wordcount.class);
		//gets and reduces all days
		job.setMapperClass(dayMapper.class);
		job.setReducerClass(hashReducer.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);    
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
	}*/
	
	////////////////////////////////////////
	///			HASH COUNT				///
	//////////////////////////////////////
	
	/*public static class hashMapper extends Mapper<Object, Text, IntWritable, Text>{
		
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			// current word
			StringTokenizer st = new StringTokenizer(value.toString());
			Text wordOut = new Text();
			// intial found value
			IntWritable k = new IntWritable();
			// loop through file until all words have been counted and found
			while (st.hasMoreTokens()) {
				int t = Integer.parseInt(st.nextToken());
				k.set(t);
				wordOut.set(st.nextToken());
				context.write(k, wordOut);
			}
		}
	}
	
	// basic reducer function
	public static class hashReducer extends Reducer <Text, Text, IntWritable, Text >{
		public void reduce(IntWritable term, Text value, Context context) throws IOException, InterruptedException  {
			//top 100
				// write it to a file
				context.write(term, value);
			}
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
		job.setMapperClass(hashMapper.class);
		job.setReducerClass(hashReducer.class);
		job.setSortComparatorClass(IntWritable.Comparator.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);   
	    
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
	}*/
	
	////////////////////////////////////////
	///			TWEETS COUNT		 	///
	//////////////////////////////////////
	
	// both mapper and reducer function are similar to what we went over in class 
	// however this (https://www.youtube.com/watch?v=JDk-LYJMzEU&t=677s) is the tutorial I used to learn AWS
	// so it more closely resembles his code
	
	// basic mapper function
	/*public static class tweetMapper extends Mapper<Object, Text, Text, IntWritable>{
		@Override
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
	public static class hashReducer extends Reducer <Text, IntWritable,  Text,IntWritable>{
		@Override
		public void reduce(Text term, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException  {
			//top 100
				int count = 0;
				// count all the occurances of the tweet
				while (value.iterator().hasNext()) {
					count++;
					value.iterator().next();
				}
				
				// write it to a file
				IntWritable output = new  IntWritable (count);
				context.write(term, output);
			}	
	}
	  
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// get arguements
		Configuration conf = new Configuration();
		String[] argurments = new GenericOptionsParser (conf,args).getRemainingArgs();
		
		if (argurments.length !=2 ) {
			System.err.println("Not enough arguements <tweets> <output file>");
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
	    
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
	}*/




}
