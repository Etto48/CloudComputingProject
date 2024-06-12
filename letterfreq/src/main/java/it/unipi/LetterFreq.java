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
        System.out.println("Usage: hadoop jar letterfreq-0.1.0.jar it.unipi.LetterFreq -i|--input <input_path> [-o|--output <output_path>] [-r|--reducers <num_reducers>] [-n|--no-combiner] [-m|--no-in-mapper-combiner]");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception { 
        Instant start = Instant.now();
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
    
        String inputPath = null;
        String outputPath = "output";
        boolean combiner = true;
        boolean inMapperCombiner = true;
        int numReducers = 1;

        for (int i = 0; i < otherArgs.length; i++) { 
            switch (otherArgs[i]) {
                case "-i":
                case "--input":
                    if (i + 1 < otherArgs.length) {
                        inputPath = otherArgs[i + 1];
                        i++;
                    }
                    break;
                case "-o":
                case "--output":
                    if (i + 1 < otherArgs.length) {
                        outputPath = otherArgs[i + 1];
                        i++;
                    }
                    break;
                case "-r":
                case "--reducers":
                    if (i + 1 < otherArgs.length) {
                        numReducers = Integer.parseInt(otherArgs[i + 1]);
                        i++;
                    }
                    break;
                case "-n":
                case "--no-combiner":
                    combiner = false;
                    break;
                case "-m":
                case "--no-in-mapper-combiner":
                    inMapperCombiner = false;
                    break;
                case "-h":
                case "--help":
                default:
                    printHelp();
                    break;
            }
        }

        if (inputPath == null) {
            printHelp();
        }

        Job job = Job.getInstance(conf, "letter frequency"); 
        job.setJarByClass(LetterFreq.class); 
        if (inMapperCombiner)
            job.setMapperClass(Mapper.class); 
        else 
            job.setMapperClass(MapperNoCombiner.class);
        if (combiner)
            job.setCombinerClass(Reducer.class); 
        job.setReducerClass(Reducer.class); 
        job.setOutputKeyClass(Char.class); 
        job.setOutputValueClass(CountTotalPairWritable.class); 
        job.setNumReduceTasks(numReducers);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputPath))) {
            hdfs.delete(new Path(outputPath), true);
        }

        FileOutputFormat.setOutputPath(job, 
        new Path(outputPath)); 
        int res = job.waitForCompletion(true) ? 0 : 1;
        Instant end = Instant.now();
        System.out.println("Execution time: " + java.time.Duration.between(start, end).toMillis()/1000.0 + "s");
        System.exit(res); 
    }
}
