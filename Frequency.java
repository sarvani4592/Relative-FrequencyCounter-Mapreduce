import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Frequency{

	public static TreeSet<ResultPair> Output = new TreeSet<ResultPair>();
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>
		{
		protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	String content=value.toString();
        	content = content.replaceAll("\\<.*?\\>", "");
        	content = content.replaceAll("[^\\p{L}\\p{Z}]","");
        	content = content.replaceAll("[^\\w\\s]","");
        	content = content.replaceAll("[^\\x20-\\x7e]", "");
        	content = content.replaceAll("[^\\u0000-\\uFFFF]", "");
        	content=content.toLowerCase();
            String[] splitLine = content.split("\\s+");
            Text wordpairkey=new Text();
            for(String val : splitLine){
                if(val.matches("[a-z]+$")){  //^\\w+$
                    context.write(new Text(val.trim() + " " + "*"), new LongWritable(1));
                }
            }	            
            StringBuilder sb = new StringBuilder();
            int i;
            for(i = 0; i < splitLine.length; i++ ){
                if(i == splitLine.length - 1){
                    break;
                }
                else{
                    if(splitLine[i].matches("[a-z]+$") && splitLine[i+1].matches("[a-z]+$")){
                sb.append(splitLine[i]).append(" ").append(splitLine[i+1]);
                context.write(new Text(sb.toString()), new LongWritable(1));
                sb.delete(0, sb.length());
                    }
                }
            }
        }
		}


	public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable>
		{

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
			{
				long count = 0;
				LongWritable sumofval= new LongWritable();

				for (LongWritable val : values)
					{
						count = count + val.get();
					}
				sumofval.set(count);
				context.write(key, sumofval);
			}

		}


	
		public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

		private DoubleWritable totalCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();
	
		private HashMap<String,Double> hm = new HashMap<String,Double>();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			if (key.toString().split(" ")[1].equals("#")) {
				double sum_A =0.0;
				String A = key.toString().split(" ")[0];
				
					totalCount.set(0);
					sum_A= getTotalCount(values);
					totalCount.set(sum_A);
					hm.put(A,sum_A);
			} 


				
			else {
				String A = key.toString().split(" ")[0];

				double count = getTotalCount(values);
				double countofA = hm.get(A);
				relativeCount.set((double) count / countofA);
				Double relativeCountD = relativeCount.get();

					if(relativeCountD !=1.0)
						{

						Output.add(new ResultPair(relativeCountD,count, key.toString()));
							if (Output.size() > 100) { Output.pollLast(); } 

								}
					
			} 
			
		}

	private double getTotalCount(Iterable<LongWritable> values) {
			double count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			return count;
		}



     protected void cleanup(Context context) throws IOException, InterruptedException 
		  {
			while (!Output.isEmpty()) 
					{
					   ResultPair p1 = Output.pollFirst();
					  context.write(new Text(p1.key+" / "+p1.key.split(" ")[0] + " ="), new Text(Double.toString(p1.relativeFrequency)));
					 
					}
			}

	}
	
	
	
	public static void main(String[] args) throws Exception 
	{
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(Frequency.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);




	}


	public static class ResultPair implements Comparable<ResultPair> {
		double relativeFrequency;
		double value;
		String key;
		
		ResultPair(double relativeFrequency, double value, String key) {
			this.relativeFrequency = relativeFrequency;
			this.value = value;
			this.key = key;
			 
		}

		public int compareTo(ResultPair resultPair) {
			
				if(this.relativeFrequency<=resultPair.relativeFrequency){
				
				return 1;
			} else {
				return -1;
			}
			
		}

}

}