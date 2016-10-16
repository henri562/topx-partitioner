/*
 * Student Info: Name=Mengchuan Lin, ID=12861
 * Subject: CS570D_HW2_Fall_2016
 * Author: Mengchuan Lin
 * Filename: PartitionByYear.java
 * Date and Time: Oct 16, 2016 8:42:12 AM
 * Project Name: topx-partitioner
 */
package npu.topx.partitioner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Mengchuan Lin
 */
public class PartitionByYear {
    
    public static class ByYearMapper extends Mapper<Object, Text, Text, NullWritable> {
        
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(value.toString()), NullWritable.get());
        }
    }
    
    public static class ByYearReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
    
    public static class ByYearPartitioner extends Partitioner<Text, NullWritable> {
        @Override
        public int getPartition(Text key, NullWritable value, int numOfPartitions) {
            String empData[] = key.toString().split("[|]+");
            switch (empData[2]) {
                case "2010":
                    return 0;
                case "2011":
                    return 1;
                case "2012":
                    return 2;
                case "2013":
                    return 3;
                case "2014":
                    return 4;
                default:
                    return 5;
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        java.nio.file.Path path = Paths.get("employee_output");
        if (Files.exists(path)) {
            FileUtils.deleteDirectory(path.toFile());
        }
        
        Job job = new Job();
        job.setJarByClass(TopFiveWordCount.class);
        job.setJobName("PartitionByYear");
        
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setMapperClass(ByYearMapper.class);
        job.setPartitionerClass(ByYearPartitioner.class);
        job.setReducerClass(ByYearReducer.class);
        
        job.setNumReduceTasks(6);
        
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);        
    }    
}
