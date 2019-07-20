package cn.com.runtrend.analysis.hadoop.idmapping;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @Auther: 张乔齐
 * @Description: 从生成的关系中提取终端相关数据
 * @Date: 2017/9/7
 * @Modified By:
 */
public class Id2Terminal {

  public static boolean run(String[] args) {
    Configuration conf = new Configuration();
    try {
      String[] otherArgs = new GenericOptionsParser(
          conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
        System.exit(2);
      }

      Job job = Job.getInstance(conf, "Id2Terminal");
      conf.setBoolean("mapred.compress.map.output", true);
      conf.setClass("mapred.map.output.compression.codec",GzipCodec.class,CompressionCodec.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "_id1"));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[0] + "_Id2Terminal"));

      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

      configureJob(job);

      return job.waitForCompletion(true);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  private static void configureJob(Job job) {
    job.setJarByClass(Id2Terminal.class);

    job.setMapperClass(Id2Terminal.Id2TerminalMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setReducerClass(Id2Terminal.Id2TerminalReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
  }

  public static class Id2TerminalMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] values = value.toString().split("\t", -1);
      if (values.length == 3) {
        values[1] = values[1].replaceAll("imei_", "").replaceAll("idfa_", "")
            .replaceAll("mac_", "");
        values[2] = values[2].replaceAll("terminal_", "");
        context
            .write(new Text(values[0] + "\t" + values[1] + "\t" + values[2]), NullWritable.get());
      }
    }
  }

  public static class Id2TerminalReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }
}
