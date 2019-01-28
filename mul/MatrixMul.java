import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMul {

  public static class Map
       extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
			int m = Integer.parseInt(conf.get("m"));
			int n = Integer.parseInt(conf.get("n"));
			int o = Integer.parseInt(conf.get("o"));
			
			String line = value.toString();
			String[] splitValue = line.split(",");
			splitValue[3] = splitValue[3].replace("]", "");
			splitValue[1] = splitValue[1].replace(" ", "");
			splitValue[2] = splitValue[2].replace(" ", "");
			splitValue[3] = splitValue[3].replace(" ", "");

			Text outputKey = new Text();
			Text outputValue =  new Text();

			if(splitValue[0].equals("[\"a\"")) {
							for (int i=0; i<o; i++) {
											outputKey.set(splitValue[1] + "," + i);
											outputValue.set("a," + splitValue[2] + "," + splitValue[3]);
											context.write(outputKey, outputValue);
							}
			}
			else {
							for (int i=0; i<m; i++) {
											outputKey.set(i + "," + splitValue[2]);
											outputValue.set("b," + splitValue[1] + "," + splitValue[3]);
											context.write(outputKey, outputValue);
							}
			}
    }
  }

  public static class Reduce
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] value;

			HashMap<Integer, Integer> matrixA = new HashMap<Integer, Integer>();
			HashMap<Integer, Integer> matrixB = new HashMap<Integer, Integer>();

			for(Text inputData : values) {
							value = inputData.toString().split(",");
							if(value[0].equals("a")) {
											matrixA.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
							}
							else {
											matrixB.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
							}
			}

			int m = Integer.parseInt(conf.get("m"));
			int n = Integer.parseInt(conf.get("n"));
			int o = Integer.parseInt(conf.get("o"));

			float result = 0.0f;
			float A_mn, B_no;

			for (int i = 0; i<n; i++) {
						A_mn = matrixA.containsKey(i) ? matrixA.get(i) : 0.0f;
						B_no = matrixB.containsKey(i) ? matrixB.get(i) : 0.0f;
						result += A_mn * B_no;
			}

			Text returnValue = new Text();
			String temp = Float.toString(result);
			returnValue.set(temp);
		
			if (result != 0.0f) { 
							context.write(key, returnValue);
			}
			else {
							context.write(key, returnValue);
			}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("m", "5"); // A matrix col
		conf.set("n", "5"); // A matrix row, B matrix col
		conf.set("o", "5"); // B matrix row
		
		Job job = Job.getInstance(conf, "MatrixMultiply");
    job.setJarByClass(MatrixMul.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
