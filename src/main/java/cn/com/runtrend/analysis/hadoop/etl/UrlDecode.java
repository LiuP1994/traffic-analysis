package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Base64;
import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.core.UnicodeDecode;
import cn.com.runtrend.analysis.hadoop.MapreduceField;
import cn.com.runtrend.analysis.hadoop.MrProperty;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
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

/**
 * @Auther: 张乔齐
 * @Description: mr程序  不断迭代流量数据，直到 urlencode 码全部解析
 * @Date: 2017/8/9
 * @Modified By:
 */
public class UrlDecode {

  public static class UrlDecodeMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      MrProperty mrProperty = MapreduceField
          .getMrProperty(value.toString(), context.getConfiguration().get("fields"));
      Map<String, Integer> fieldMap = mrProperty.getFieldMap();
      List<String> row = mrProperty.getRow();
      List<String> result = new ArrayList<String>(row);

      if (!row.isEmpty() && fieldMap.get("url") < row.size() && fieldMap.get("cookie") < row.size()
          && fieldMap
          .get("ua") < row.size()) {
        String ua = row.get(fieldMap.get("ua"));
        String url = row.get(fieldMap.get("url"));
        String cookie = row.get(fieldMap.get("cookie"));
        if (context.getConfiguration().get("base64").equals("1")) {
          ua = Base64.getFromBase64(ua);
          cookie = Base64.getFromBase64(cookie);
        }
        url = getEncode(url);
        cookie = getEncode(cookie);
        result.set(fieldMap.get("ua"), ua);
        result.set(fieldMap.get("url"), url);
        result.set(fieldMap.get("cookie"), cookie);
      }

      Text data = new Text();
      data.set(MapreduceField.list2String(result));
      context.write(data, NullWritable.get());
    }
  }

  private static String getEncode(String str) throws UnsupportedEncodingException {
    if (cn.com.runtrend.analysis.core.UrlDecode.checkEncodeRegx(str)) {
      String decode = UnicodeDecode
          .decodeExt(cn.com.runtrend.analysis.core.UrlDecode.decode(str, "UTF-8"));
      String encode = URLEncoder.encode(decode, "UTF-8");
      String pattern = ".*%EF%BF%BD%EF%BF%BD.*";
      if (Pattern.matches(pattern, encode)) {
        decode = UnicodeDecode
            .decodeExt(cn.com.runtrend.analysis.core.UrlDecode.decode(str, "gbk"));
        return decode;
      } else {
        return decode;
      }
    } else {
      return str;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.task.timeout", Constants.MAPRED_TASK_TIMEOUT);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3 && otherArgs.length != 4) {
      System.err.println("UrlDecode <in> <out> <fields>");
      System.exit(2);
    }
    conf.set("fields", otherArgs[2]);
    if (otherArgs.length == 4) {
      conf.set("base64", otherArgs[3]);
    } else {
      conf.set("base64", "0");
    }
    Job job = Job.getInstance(conf, "UrlDecode");
    job.setJobName("traffic_bi_MR_UrlDecode");

    job.setNumReduceTasks(0);

    job.setJarByClass(UrlDecode.class);
    job.setMapperClass(UrlDecode.UrlDecodeMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}