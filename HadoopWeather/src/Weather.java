import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.*;

public class Weather {
    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        System.out.println("processing111");
        Configuration configuration = new Configuration();
        System.out.println("processing222");
        if (args.length != 5) {
            System.err.println("Usage:weather <input><output>");
            System.exit(2);
        }

        Job job = new Job(configuration, "weather");

        job.setJarByClass(Weather.class);
        job.setMapperClass(WeatherMapper.class);
        job.setCombinerClass(WeatherReducer.class);
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[3]));
        Path outputPath = new Path(args[4]);
        outputPath.getFileSystem(configuration).delete(outputPath,true);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.out.println("processing");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}