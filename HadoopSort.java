import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;

public class HadoopSort {
    public static class HadoopSortMapper extends Mapper<Text,Text,Text,Text> {
        public void map(Text mapperKey, Text mapperValue, Context context) throws IOException, InterruptedException {
            context.write((mapperValue.toString()).substring(0,10),(mapperValue.toString()).substring(10));
        }
    }    
    public static class HadoopSortReducer extends Mapper<Text,Text,Text,Text> {
        Text values=new Text();
        public void reduce(Text reducerKey, Iterator<Text> reducerValues, Context context) throws IOException, InterruptedException {
            for(Text temp : reducerValues)
                values=temp;
            context.write(reducerKey,values);
        }
    }   
    public static void main(String[] args) {   
        if(args.length != 2) {
            System.out.println("Arguments missing!");
            System.exit(1);
        }
        long start = System.currentTimeMillis();
        
        Job job = new Job();
        job.setJarByClass(HadoopSortDriver.class);
        job.setJobName("Hadoop Sort");
        
        job.setMapperClass(HadoopSortMapper.class);
        job.setReducerClass(HadoopSortReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        long end = System.currentTimeMillis();
        double elapsedTime = (end-start)/1000.0;
        
        System.out.println("Total time taken to sort data is=" + elapsedTime);
        System.exit(job.waitForCompletion(true) ? 0 : 1);       
    }
}