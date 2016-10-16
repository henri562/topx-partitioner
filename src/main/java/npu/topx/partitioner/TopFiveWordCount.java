/*
 * Student Info: Name=Mengchuan Lin, ID=12861
 * Subject: CS570D_HW2_Fall_2016
 * Author: Mengchuan Lin
 * Filename: TopFiveWordCount.java
 * Date and Time: Oct 16, 2016 5:24:44 AM
 * Project Name: topx-partitioner
 */
package npu.topx.partitioner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Mengchuan Lin
 */
public class TopFiveWordCount {
    
    public static class TopFiveMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        private final PriorityQueue<String> pq = new PriorityQueue<>(new StringComparator());
        
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //remove tab and blank space from original value string
            String word = value.toString().replaceAll("[\t ]+", "");
            //add word string to priority queue
            pq.add(word);            
            //if queue exceeds five elements, remove head element
            if (pq.size() > 5)
                pq.poll();
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //write new word and its number of occurrences to file after parsing
            for (String i : pq) {
                context.write(new Text(i.replaceAll("[0-9]+", "")),
                        new IntWritable(Integer.parseInt(i.replaceAll("[a-zA-Z]+", ""))));
            }
        }
    }
    
    public static class TopFiveReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private final PriorityQueue<String> pq = new PriorityQueue<>(new StringComparator());
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sbr = new StringBuilder();
            for (IntWritable value : values) {
                sbr = sbr.append(key.toString()).append(value.toString());
                pq.add(sbr.toString());
                if (pq.size() > 5)
                    pq.poll();
            }
        }
        
        @Override
        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            for (String i : pq) {
                context.write(new Text(i.replaceAll("[0-9]+", "")),
                        new IntWritable(Integer.parseInt(i.replaceAll("[a-zA-Z]+", ""))));
            }
        }
    }
    /**
     Custom comparator class to compare strings based on the digits contained therein
     */
    private static class StringComparator implements Comparator<String> {

                @Override
        public int compare(String x, String y){
            x = x.replaceAll("[A-Za-z]+", "");
            y = y.replaceAll("[A-Za-z]+", "");
            
            return Integer.parseInt(x) - Integer.parseInt(y);
        }
    }
    
    public static void main(String[] args) throws Exception {
        java.nio.file.Path path = Paths.get("wordcount_output");
        if (Files.exists(path)) {
            FileUtils.deleteDirectory(path.toFile());
        }
        
        Job job = new Job();
        job.setJarByClass(TopFiveWordCount.class);
        job.setJobName("TopFiveWordCount");
        
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setMapperClass(TopFiveMapper.class);
        job.setReducerClass(TopFiveReducer.class);
        
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);        
    }
}
