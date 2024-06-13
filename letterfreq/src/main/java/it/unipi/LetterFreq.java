package it.unipi;

import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; 

public class LetterFreq 
{
    public static void printHelp() {
        System.out.println(
            "Usage: hadoop jar letterfreq-0.1.0.jar it.unipi.LetterFreq "+
            "-i|--input <input_path> "+
            "[-t|--tmp <tmp_path>] "+
            "[-o|--output <output_path>] "+
            "[-r|--reducers <num_reducers>] "+
            "[-n|--no-combiner] "+
            "[-m|--no-in-mapper-combiner]"
        );
        System.exit(0);
    }

    public static void main(String[] args) throws Exception { 
        Instant start = Instant.now();
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
    
        String inputPath = null;
        String tmpPath = "tmp";
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
                case "-t":
                case "--tmp":
                    if (i + 1 < otherArgs.length) {
                        tmpPath = otherArgs[i + 1];
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

        Job job1 = Job.getInstance(conf, "letter sum"); 
        job1.setJarByClass(LetterFreq.class); 
        job1.setInputFormatClass(TextInputFormat.class);

        if (inMapperCombiner)
            job1.setMapperClass(Mapper.class); 
        else 
            job1.setMapperClass(MapperNoCombiner.class);
        if (combiner)
            job1.setCombinerClass(Reducer.class); 
        job1.setReducerClass(Reducer.class); 
        job1.setOutputKeyClass(Char.class); 
        job1.setOutputValueClass(LongWritable.class); 
        job1.setNumReduceTasks(numReducers);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputPath))) {
            hdfs.delete(new Path(outputPath), true);
        }
        if (hdfs.exists(new Path(tmpPath))) {
            hdfs.delete(new Path(tmpPath), true);
        }
        
        FileOutputFormat.setOutputPath(job1, new Path(tmpPath)); 

        int res1 = job1.waitForCompletion(true) ? 0 : 1;
        if (res1 != 0) {
            throw new RuntimeException("Job1 failed");
        }

        long total = job1.getCounters().findCounter("LetterFreq", "total").getValue();
        if (total == 0) {
            throw new RuntimeException("Total count is zero");
        }

        Job job2 = Job.getInstance(conf, "letter frequency");
        job2.getConfiguration().setLong("total", total);
        job2.setJarByClass(LetterFreq.class);
        // Identity mapper
        job2.setMapOutputKeyClass(Char.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setReducerClass(ReducerFrequency.class);
        job2.setOutputKeyClass(Char.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(tmpPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));
        int res2 = job2.waitForCompletion(true) ? 0 : 1;
        if (res2 != 0) {
            throw new RuntimeException("Job2 failed");
        }

        Instant end = Instant.now();
        System.out.println("Execution time: " + java.time.Duration.between(start, end).toMillis()/1000.0 + "s");
    }
}
