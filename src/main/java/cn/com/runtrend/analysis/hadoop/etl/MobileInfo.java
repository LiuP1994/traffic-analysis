package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.core.HttpGetParser;
import cn.com.runtrend.analysis.hadoop.MapreduceField;
import cn.com.runtrend.analysis.hadoop.MrProperty;
import java.io.IOException;
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
 * @Description: 获取手机信息
 * @Date: 2017/8/21
 * @Modified By:
 */
public class MobileInfo {

  public static class MobileInfoMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      MrProperty mrProperty = MapreduceField
          .getMrProperty(value.toString(), context.getConfiguration().get("fields"));
      Map<String, Integer> fieldMap = mrProperty.getFieldMap();
      List<String> row = mrProperty.getRow();
      List<String> result = new ArrayList<String>(row);

      if (!row.isEmpty() && fieldMap.get("url") < row.size()) {
        List<String> paramList = HttpGetParser.paramList(row.get(fieldMap.get("url")));
        List<String> paramList2 = HttpGetParser.paramList2(row.get(fieldMap.get("url")));
        result.add(MobileInfoUtil.phone(paramList, paramList2));
        result.add(MobileInfoUtil.imei(paramList, paramList2));
        result.add(MobileInfoUtil.imsi(paramList, paramList2));
        result.add(MobileInfoUtil.mac(paramList, paramList2));
        result.add(MobileInfoUtil.idfa(paramList, paramList2));
      }

      Text data = new Text();
      data.set(MapreduceField.list2String(result));
      context.write(data, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.task.timeout", Constants.MAPRED_TASK_TIMEOUT);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("MobileInfo <in> <out> <fields>");
      System.exit(2);
    }
    conf.set("fields", otherArgs[2]);
    Job job = Job.getInstance(conf, "MobileInfo");
    job.setJobName("traffic_bi_MR_MobileInfo");

    job.setNumReduceTasks(0);

    job.setJarByClass(MobileInfo.class);
    job.setMapperClass(MobileInfo.MobileInfoMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  static class MobileInfoUtil {
    static String phone(List<String> paramList, List<String> paramList2) {
      String pattern = "(0|86|17951)?(13[0-9]|15[012356789]|17[678]|18[0-9]|14[57])[0-9]{8}";
      String temp = check(pattern, paramList);
      if ("".equals(temp)) {
        temp = check(pattern, paramList2);
      }
      return temp;
    }

    static String imei(List<String> paramList, List<String> paramList2) {
      String pattern = "\\d{15}";
      String str = check(pattern, paramList);
      if ("".equals(str)) {
        str = check(pattern, paramList2);
      }
      if (!"".equals(str)) {
        String[] imeis = str.split(",", -1);
        if (imeis.length == 1) {
          String code = genCode(str.substring(0, 14));
          String imei = str.substring(0, 14) + code;
          if (imei.equals(str)) {
            return str;
          } else {
            return "";
          }
        } else {
          StringBuilder sb = new StringBuilder();
          for (String im : imeis) {
            String code = genCode(im.substring(0, 14));
            String imei = im.substring(0, 14) + code;
            if (imei.equals(im)) {
              sb.append(im);
              sb.append(",");
            }
          }
          if (sb.length() > 0) {
            sb.delete(sb.length() - 1, sb.length());
          }
          return sb.toString();
        }
      } else {
        return "";
      }
    }

    static String imsi(List<String> paramList, List<String> paramList2) {
      String pattern = "4600([0-3]|[5-7])\\d{10}";
      String temp = check(pattern, paramList);
      if ("".equals(temp)) {
        temp = check(pattern, paramList2);
      }
      return temp;
    }

    static String mac(List<String> paramList, List<String> paramList2) {
      String pattern = "([0-9a-fA-F]{2})(([:-][0-9a-fA-F]{2}){5})";
      String temp = check(pattern, paramList);
      if ("".equals(temp)) {
        temp = check(pattern, paramList2);
      }
      return temp;
    }

    static String idfa(List<String> paramList, List<String> paramList2) {
      String pattern = "([0-9a-zA-Z]{8})(([-][0-9a-zA-Z]{4}){3})([-][0-9a-zA-Z]{12})";
      String temp = check(pattern, paramList);
      if ("".equals(temp)) {
        temp = check(pattern, paramList2);
      }
      return temp;
    }

    private static String check(String pattern, List<String> paramList) {
      StringBuilder sb = new StringBuilder();
      for (String str : paramList) {
        if (Pattern.matches(pattern, str)) {
          if (sb.length() < 2) {
            sb.append(str);
          } else {
            sb.append(",").append(str);
          }
        }
      }
      return sb.toString().trim();
    }

    private static String genCode(String code) {
      int total = 0, sum1 = 0, sum2 = 0;
      int temp = 0;
      char[] chs = code.toCharArray();
      for (int i = 0; i < chs.length; i++) {
        int num = chs[i] - '0';     // ascii to num
        //System.out.println(num);
            /*(1)将奇数位数字相加(从1开始计数)*/
        if (i % 2 == 0) {
          sum1 = sum1 + num;
        } else {
                /*(2)将偶数位数字分别乘以2,分别计算个位数和十位数之和(从1开始计数)*/
          temp = num * 2;
          if (temp < 10) {
            sum2 = sum2 + temp;
          } else {
            sum2 = sum2 + temp + 1 - 10;
          }
        }
      }
      total = sum1 + sum2;
        /*如果得出的数个位是0则校验位为0,否则为10减去个位数 */
      if (total % 10 == 0) {
        return "0";
      } else {
        return (10 - (total % 10)) + "";
      }

    }
  }
}

