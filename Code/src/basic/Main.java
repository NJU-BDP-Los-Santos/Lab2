package basic;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.util.StringTokenizer;

public class Main {
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: MinMaxCountDriver <in> <out>");
                System.exit(2);
            }

            Job job = new Job(conf, "Second Sort");
            job.setJarByClass(Main.class);
            job.setMapperClass(TonkenizerMapper.class);
            job.setReducerClass(SecondSortReducer.class);
            job.setCombinerClass(MyCountCombiner.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class TonkenizerMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            fileName = fileName.substring(0,fileName.lastIndexOf('.'));
            fileName = fileName.substring(0,fileName.lastIndexOf('.'));

            Text word = new Text();
//            IntWritable count;
//            HashMap<Text, Integer> hashMap = new HashMap<>();
            StringTokenizer itr = new StringTokenizer(value.toString());
//            while(itr.hasMoreTokens())
//            {
//                word.set(itr.nextToken());
//                if(hashMap.containsKey(word))
//                {
//                    hashMap.put(word, hashMap.get(word)+1);
//                }
//                else
//                {
//                    hashMap.put(word, 1);
//                }
//            }

            while(itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                Text word_filename = new Text(word + "#" + fileName);
                context.write(word_filename, new IntWritable(1));
//                context.write(word, new Text(fileName));
            }
//            for (Iterator<String> it = hashMap.keySet().iterator(); it.hasNext(); )
//            {
//                word = it.next();
//                count = new IntWritable(hashMap.get(word));
//                Text fileName_count = new Text(fileName+"#"+count);
//                context.write(new Text(word), fileName_count);
//            }
        }
    }
    public static class MyCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int total=0;
            for(IntWritable value:values)
            {
                total=total+value.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static class SecondSortReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
//            HashMap<String, Integer> hashMap = new HashMap<>();
//
//            String prevFile = "";
//            int count = 0;
//            for(Text val : values)
//            {
//                String fileName_count = val.toString();
//                String fileName = fileName_count.substring(0, val.find("#"));
//                String num = fileName_count.substring(val.find("#")+1,val.getLength());
//                int tempCount = Integer.parseInt(num);
//                if(prevFile.compareTo("") == 0)
//                {
//                    prevFile = fileName;
//                }
//                if(prevFile.compareTo(fileName) == 0)
//                {
//                    count += tempCount;
//                }
//                else
//                {
//                    hashMap.put(key.toString(), count);
//                    count = 0;
//                    prevFile = fileName;
//                }
//            }
//
//            int sum = 0;
//            StringBuilder stringBuilder = new StringBuilder();
//            for(Iterator<String> it = hashMap.keySet().iterator(); it.hasNext(); )
//            {
//                String docName = it.next();
//                int tempCount = hashMap.get(docName);
//                sum += tempCount;
//                stringBuilder.append(docName+":"+String.valueOf(tempCount)+";");
//            }
//            float frequency = (float)sum / hashMap.keySet().size();
//            context.write(key, new Text(String.valueOf(frequency)+","+stringBuilder.toString()));
            Iterator<IntWritable> it = values.iterator();
            StringBuilder all = new StringBuilder();
//            if (it.hasNext())
//                all.append(it.next().toString());
            while(it.hasNext())
            {
                all.append(";");
                all.append(it.next().toString());
            }
            context.write(key, new Text(all.toString()));
        }
    }
}

