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
    public static void printHelp() {
        System.out.println("Usage: hadoop jar letterfreq-0.1.0.jar it.unipi.LetterFreq -i <input_path> [-o <output_path>] [-r <num_reducers>]");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception { 
        Instant start = Instant.now();
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
    
        String input_path = null;
        String output_path = "output";
        int num_reducers = 1;

        for (int i = 0; i < otherArgs.length; i++) { 
            switch (otherArgs[i]) {
                case "-i":
                case "--input":
                    if (i + 1 < otherArgs.length) {
                        input_path = otherArgs[i + 1];
                        i++;
                    }
                    break;
                case "-o":
                case "--output":
                    if (i + 1 < otherArgs.length) {
                        output_path = otherArgs[i + 1];
                        i++;
                    }
                    break;
                case "-r":
                case "--reducers":
                    if (i + 1 < otherArgs.length) {
                        num_reducers = Integer.parseInt(otherArgs[i + 1]);
                        i++;
                    }
                    break;
                case "-h":
                case "--help":
                default:
                    printHelp();
                    break;
            }
        }

        if (input_path == null) {
            printHelp();
        }

        Job job = Job.getInstance(conf, "letter frequency"); 
        job.setJarByClass(LetterFreq.class); 
        job.setMapperClass(Mapper.class); 
        job.setCombinerClass(Reducer.class); 
        job.setReducerClass(Reducer.class); 
        job.setOutputKeyClass(Char.class); 
        job.setOutputValueClass(CountTotalPairWritable.class); 
        job.setNumReduceTasks(num_reducers);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(output_path))) {
            hdfs.delete(new Path(output_path), true);
        }

        FileOutputFormat.setOutputPath(job, 
        new Path(output_path)); 
        int res = job.waitForCompletion(true) ? 0 : 1;
        Instant end = Instant.now();
        System.out.println("Execution time: " + java.time.Duration.between(start, end).toMillis()/1000.0 + "s");
        System.exit(res); 
    }
}
