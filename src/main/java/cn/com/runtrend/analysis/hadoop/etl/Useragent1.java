package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.commons.StringUtils;
import cn.com.runtrend.analysis.core.UnicodeDecode;
import cn.com.runtrend.analysis.hadoop.HdfsUtil;
import cn.com.runtrend.analysis.hadoop.MapreduceField;
import cn.com.runtrend.analysis.hadoop.MrProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: 张乔齐
 * @Description: ua 提取
 * @Date: 2017/8/11
 * @Modified By:
 */
public class Useragent1 {

  public static class UserAgentMapper extends Mapper<Object, Text, Text, NullWritable> {

    List<String> list=new ArrayList<>();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
      BufferedReader br = HdfsUtil.readFile(context.getConfiguration().get("path"), context);
      String line;
      while ((line = br.readLine()) != null) {
        line = UnicodeDecode.decodeExt(line);
        list.add(line);
      }
      br.close();
    }

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
      if (!value.toString().isEmpty()) {
        System.out.println("Hello1");
        String ua = value.toString();
        System.out.println("Hello2");
        ua=UnicodeDecode.decodeExt(ua);
        System.out.println("Hello3");
          for (String regex : list) {
            String str = StringUtils.regex(regex, ua);
            if (!str.equals("")) {
              Text data = new Text();
              data.set(str);
              context.write(data, NullWritable.get());
              break;
            }
          }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.task.timeout", Constants.MAPRED_TASK_TIMEOUT);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Useragent1 <in> <join> <out>");
      System.exit(2);
    }

    conf.set("path", otherArgs[1]);

    Job job = Job.getInstance(conf, "Useragent1");
    job.setJobName("traffic_bi_MR_Useragent1");

    job.setNumReduceTasks(0);

    job.setJarByClass(Useragent1.class);
    job.setMapperClass(Useragent1.UserAgentMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
