package it.unipi;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 

public class LetterFreq 
{
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
        if (otherArgs.length < 2) { 
            System.err.println("Usage: letterfreq <in> [<in>...] <out>"); 
            System.exit(2); 
        }
        Job job = Job.getInstance(conf, "letter frequency"); 
        job.setJarByClass(LetterFreq.class); 
        job.setMapperClass(Mapper.class); 
        job.setCombinerClass(Reducer.class); 
        job.setReducerClass(Reducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(FrequencyTotalPairWritable.class); 
        // job.setNumReduceTasks(numTasks);
        for (int i = 0; i < otherArgs.length - 1; ++i) { 
            FileInputFormat.addInputPath(job, new Path(otherArgs[i])); 
        }
        FileOutputFormat.setOutputPath(job, 
        new Path(otherArgs[otherArgs.length - 1])); 
        int res = job.waitForCompletion(true) ? 0 : 1;
        System.exit(res); 
    }
}
