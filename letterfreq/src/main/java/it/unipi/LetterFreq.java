package it.unipi;

import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 

public class LetterFreq 
{
    public static void main(String[] args) throws Exception { 
        Instant start = Instant.now();
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
        job.setOutputKeyClass(Char.class); 
        job.setOutputValueClass(CountTotalPairWritable.class); 
        // job.setNumReduceTasks(3);
        for (int i = 0; i < otherArgs.length - 1; ++i) { 
            FileInputFormat.addInputPath(job, new Path(otherArgs[i])); 
        }
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(otherArgs[otherArgs.length - 1]))) {
            hdfs.delete(new Path(otherArgs[otherArgs.length - 1]), true);
        }

        FileOutputFormat.setOutputPath(job, 
        new Path(otherArgs[otherArgs.length - 1])); 
        int res = job.waitForCompletion(true) ? 0 : 1;
        Instant end = Instant.now();
        System.out.println("Execution time: " + java.time.Duration.between(start, end).toMillis()/1000.0 + "s");
        System.exit(res); 
    }
}
