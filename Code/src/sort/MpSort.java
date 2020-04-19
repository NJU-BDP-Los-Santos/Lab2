package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

public class MpSort {
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            //Get Job Instance
            Job job = Job.getInstance(conf, "Second Sort");
            //Set Jar
            job.setJarByClass(MpSort.class);
            //set Map,Reduce and Partition functions
            job.setMapperClass(MpSort.SortMapper.class);
            job.setReducerClass(MpSort.SortReducer.class);
            job.setPartitionerClass(MpSort.SortPartition.class);
            //set map output class
            job.setMapOutputKeyClass(FloatWritable.class);
            //output settings
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //input settings
            job.setInputFormatClass(TextInputFormat.class);//attention
            //reduce nodes
            job.setNumReduceTasks(4);
            //input & output path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class SortMapper extends Mapper<Object,Text,FloatWritable,Text>
    {
        public static boolean isDouble(String str) {
            Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
            return pattern.matcher(str).matches();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            //check file name
            if(fileName.contains("SUCCESS")) return;
            //word  frequency, doc1:t1;doc2:t2...  ->  "word" "frequency"
            String[] line = value.toString().split("[ \t]");
            Text word = new Text(line[0]);
            String sFloat = line[1].split(",")[0];
            //check invalid input
            if(line.length < 2 || isDouble(sFloat) == false) return;
            //emit <key,value> = <frequency,word>
            FloatWritable frequency = new FloatWritable(Float.valueOf(sFloat));
            context.write(frequency,word);
        }
    }

    public static class SortPartition extends Partitioner<FloatWritable, Text>
    {
        @Override
        public int getPartition(FloatWritable key, Text value, int i) {
            //get partition according to frequency
            float x = key.get();
            if(x < 1.333f) return 0;
            else if(x >= 1.333f && x < 1.701f) return 1;
            else if(x >= 1.701f && x < 3.667f) return 2;
            else return 3;
        }
    }

    public static class SortReducer extends Reducer<FloatWritable, Text, Text, Text> {

        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            //sort function will be executed automatically by MapReduce Framework
            Float frequency = key.get();
            for(Text each : values)
            {
                context.write(each, new Text(String.valueOf(frequency)));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            super.cleanup(context);
        }
    }
}
