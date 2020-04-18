package njubdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: MinMaxCountDriver <in> <out>");
                System.exit(2);
            }*/
            //Get Job Instance
            Job job = Job.getInstance(conf, "Second Sort");
            //Set Jar
            job.setJarByClass(Main.class);
            //set Map and Reduce functions
            job.setMapperClass(TonkenizerMapper.class);
            job.setReducerClass(SecondSortReducer.class);
            //output settings
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //input settings
            job.setInputFormatClass(TextInputFormat.class);//attention
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

    public static class TonkenizerMapper extends Mapper<Object,Text,Text,Text>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            //exclude file extension
            fileName = fileName.substring(0,fileName.lastIndexOf('.'));
            fileName = fileName.substring(0,fileName.lastIndexOf('.'));
            //calculate word appeared times in a single line
            IntWritable count;
            HashMap<String, Integer> hashMap = new HashMap<>();
            String[] tokens = value.toString().split(" ");
            for(String word : tokens)
            {
                if(hashMap.containsKey(word))
                {
                    hashMap.put(word, hashMap.get(word)+1);
                }
                else
                {
                    hashMap.put(word, 1);
                }
                context.write(new Text(word), new Text(fileName+"#"+1));
            }
            //<key,value> = <word, fileName#appearedTimes>
            for (Iterator<String> it = hashMap.keySet().iterator(); it.hasNext(); )
            {
                String word = it.next();
                count = new IntWritable(hashMap.get(word));
                Text fileName_count = new Text(fileName+"#"+count);
                context.write(new Text(word), fileName_count);
            }
        }
    }

    public static class SecondSortReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            //key == word, the same word is processed by the same reduce function
            HashMap<String, Integer> hashMap = new HashMap<>();

            String prevFile = "";
            int count = 0;
            String fileName = "";
            //word appeared times in every files, because fileName#times is sorted by dictionary order
            for(Text val : values)
            {
                String fileName_count = val.toString();
                String[] splitString = fileName_count.split("#");
                if (splitString.length < 2) continue;
                fileName = splitString[0];
                String num = splitString[1];
                int tempCount = Integer.parseInt(num);
                if(prevFile.compareTo("") == 0)
                {
                    prevFile = fileName;
                }
                if(prevFile.compareTo(fileName) == 0)
                {
                    //same file
                    count += tempCount;
                }
                else
                {
                    //detected different file
                    hashMap.put(fileName, count);
                    count = 0;
                    prevFile = fileName;
                }
            }
            hashMap.put(fileName, count);
            //calculate frequency of word
            int sum = 0;
            StringBuilder stringBuilder = new StringBuilder();
            for(Iterator<String> it = hashMap.keySet().iterator(); it.hasNext(); )
            {
                String docName = it.next();
                int tempCount = hashMap.get(docName);
                sum += tempCount;
                //fileName:times;
                stringBuilder.append(docName+":"+String.valueOf(tempCount)+";");
            }
            float frequency = (float)sum / hashMap.keySet().size();
            //<key,value> = <word, file1:t1;file2:t2;file3:t3...>
            context.write(key, new Text(String.valueOf(frequency)+","+stringBuilder.toString()));
        }
    }
}

