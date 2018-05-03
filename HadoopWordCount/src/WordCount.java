import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.*;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        System.out.println("processing111");
        Configuration configuration = new Configuration();
        System.out.println("processing222");
        if (args.length != 2) {
            System.err.println("Usage:wordcount <input><output>");
            System.exit(2);
        }

        Job job = new Job(configuration, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(configuration).delete(outputPath,true);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.out.println("processing");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}