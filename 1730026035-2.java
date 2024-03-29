package wordcount;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;


public class wordcount {
	public static class InvertedIndexMapper
	        extends Mapper<LongWritable, Text, Text, Text>{	    
	        private Text Key = new Text();
	        private String pattern = "[^a-zA-Z]";
		    private Text Value = new Text();
		    private FileSplit split;
		    int row = 0;
		    
	        
	        public void map(LongWritable key, Text value, Context context ) throws
	                       IOException, InterruptedException {
	        	           row++;
	        	           String line = new String();
	        	           
	        	           split = (FileSplit)context.getInputSplit();
	        	           String fileName = split.getPath().getName();//get the article name
	        	           
	        	           String i = value.toString();
	        	           i = i.replaceAll(pattern, " ");
	        	           StringTokenizer itr = new StringTokenizer(i);
	        	           
	                       String[] words = line.split(" ");
	                       while(itr.hasMoreTokens()) {//same as Question1
	                    	   line = String.valueOf(row);
	                    	   Value.set(line); // line number of the word (value <linenumber>)
	                    	   String word = itr.nextToken()+  ", " + fileName; // Output key  <word,filename>
	                    	   Key.set(word);
	                    	   context.write(Key, Value);
	                       }
	        }
	}
	
	public static class InvertedIndexCombiner 
	        extends Reducer<Text, Text, Text, Text>{
		    private Text word = new Text();
		    private Text file = new Text();
		    
		    public void reduce(Text key, Iterable<Text> values, Context context)throws
		    IOException, InterruptedException {
		    	String[] st = key.toString().split("\\,");
		    	String w = st[1];
		    	String words = st[0];
		    	for(Text value: values) {
		    		w += " " + value.toString() + " ";
		    	}
		    	file.set(w);
		    	word.set(words.toString());
		    	context.write(word, file);
		    }
	}
	
	public static class IndexReducer
	        extends Reducer<Text,Text,Text,Text> {
	        Text word = new Text();
	        
	        public void reduce(Text key, Iterable<Text> values, Context
	                context ) throws IOException, InterruptedException {
	                String words= new String();
	               
	                for (Text value : values) {
	                         words = words + "[" + value.toString() + "]";
	                }
	                word.set(words);
	                context.write(key, word);
	        }
	}
	
	public static void main(String[] args) throws Exception {
		
		
		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word index");
		job.setJarByClass(wordcount.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(job, new Path("hdfs://master-m730026014:9000/data/ass4/hackerliang-xyz-assignment4"));
		FileInputFormat.addInputPath(job, new Path("hdfs://marco-1:9000/data/ass4/test"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://marco-1:9000/data/MR-2"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}