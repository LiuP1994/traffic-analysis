package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.commons.StringUtils;
import cn.com.runtrend.analysis.core.UnicodeDecode;
import cn.com.runtrend.analysis.hadoop.HdfsUtil;
import cn.com.runtrend.analysis.hadoop.MapreduceField;
import cn.com.runtrend.analysis.hadoop.MrProperty;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * @Description: ua 提取
 * @Date: 2017/8/11
 * @Modified By:
 */
public class Useragent {

  public static class UseragentMapper extends Mapper<Object, Text, Text, NullWritable> {

    Map<String, List<String>> regxMap = new HashMap<String, List<String>>();

    @Override
    protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
        throws IOException, InterruptedException {
      BufferedReader br = HdfsUtil.readFile(context.getConfiguration().get("path"), context);
      String line;
      while ((line = br.readLine()) != null) {
        line = UnicodeDecode.decodeExt(line);
        String startName = startName(line);

        if (!"".equals(startName)) {
          if (regxMap.containsKey(startName)) {
            List<String> list = regxMap.get(startName);
            list.add(line);
            regxMap.put(startName, list);
          } else {
            List<String> list = new ArrayList<String>();
            list.add(line);
            regxMap.put(startName, list);
          }
        }
      }
      br.close();
    }

    private static String startName(String line) {
      StringBuilder sb = new StringBuilder();
      char[] lineChars = line.toCharArray();
      for (char c : lineChars) {
        if ((c >= '\u4e00' && c <= '\u9fa5') || (c >= 'a' && c <= 'z')) {
          sb.append(c);
        } else {
          break;
        }
      }
      return sb.toString();
    }

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      MrProperty mrProperty = MapreduceField
          .getMrProperty(value.toString(), context.getConfiguration().get("fields"));
      Map<String, Integer> fieldMap = mrProperty.getFieldMap();
      List<String> row = mrProperty.getRow();
      List<String> result = new ArrayList<String>(row);
      result.add("");

      if (!row.isEmpty() && fieldMap.get("ua") < row.size() && fieldMap.get("url") < row.size()) {
        String terminal = "";
        String ua = row.get(fieldMap.get("ua")).toLowerCase();
        String url = row.get(fieldMap.get("url")).toLowerCase();

        if (terminal.equals("")) {
          if (url.contains("iphone")) {
            terminal = StringUtils.regex(".*\\=(iphone[0-9]{1,2},[0-9]{1,2}).*", url);
          }
        }

        if (terminal.equals("")) {
          if (url.contains("ipad")) {
            terminal = StringUtils.regex(".*\\=(ipad[0-9]{1,2},[0-9]{1,2}).*", url);
          }
        }

        if (terminal.equals("")) {
          if (url.contains("ipod")) {
            terminal = StringUtils.regex(".*\\=(ipod[0-9]{1,2},[0-9]{1,2}).*", url);
          }
        }

        if (terminal.equals("")) {
          String startName = startName(ua);
          if (regxMap.containsKey(startName)) {
            for (String regex : regxMap.get(startName)) {
              String str = StringUtils.regex(regex, ua);
              if (!str.equals("")) {
                terminal = str;
                break;
              }
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&device_type=")) {
            String tb = StringUtils.regex(".*&device_brand\\=([0-9a-zA-Z]+)", url);
            String tm = StringUtils.regex(".*&device_type\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&model=")) {
            String tb = StringUtils.regex(".*&brand\\=([0-9a-zA-Z]+)", url);
            String tm = StringUtils.regex(".*&model\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&md=")) {
            String tb = StringUtils.regex(".*&br\\=([0-9a-zA-Z]+)", url);
            String tm = StringUtils.regex(".*&md\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&m=")) {
            String tb = StringUtils.regex(".*&b\\=([0-9a-zA-Z]+)", url);
            String tm = StringUtils.regex(".*&m\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&mdl=")) {
            String tb = StringUtils.regex(".*&bd\\=([0-9a-zA-Z]+)", url);
            String tm = StringUtils.regex(".*&mdl\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("\"c1\":")) {
            String tb = StringUtils.regex(".*\"c0\":\"([0-9a-zA-Z]+)\"", url);
            String tm = StringUtils.regex(".*\"c1\":\"([0-9a-zA-Z\\+\\-\\_]+)\"", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }
        if (terminal.equals("")) {
          if (url.contains("\"pm\":")) {
            String tb = StringUtils.regex(".*\"br\":\"([0-9a-zA-Z]+)\"", url);
            String tm = StringUtils.regex(".*\"pm\":\"([0-9a-zA-Z\\+\\-\\_]+)\"", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&systemmodel=")) {
            String tb = StringUtils.regex(".*&systemphone\\=([0-9a-zA-Z]+)", url);
            String tm = StringUtils.regex(".*&systemmodel\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
            if (tb.equals("android")){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("\"device\":")) {
            String tb = StringUtils.regex(".*\"device\":\"([0-9a-zA-Z]+)", url);
            String tm = StringUtils.regex(".*\"device\":\"[0-9a-zA-Z]+\\+([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
            if (tm.contains(tb)){
              terminal = tm;
            }
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&model=") || url.contains("?model=")) {
            terminal = StringUtils.regex(".*[&\\?]model\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&p16=")) {
            terminal = StringUtils.regex(".*&p16\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&dev_ua=")) {
            terminal = StringUtils.regex(".*&dev_ua\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
          }
        }

        if (terminal.equals("")) {
          if (url.contains("&m_ver=")) {
            terminal = StringUtils.regex(".*&m_ver\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
          }
        }
        result.set(result.size() - 1, terminal);
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
    if (otherArgs.length != 4) {
      System.err.println("Useragent <in> <join> <out> <fields>");
      System.exit(2);
    }
    conf.set("fields", otherArgs[3]);
    conf.set("path", otherArgs[1]);

    Job job = Job.getInstance(conf, "Useragent");
    job.setJobName("traffic_bi_MR_Useragent");

    job.setNumReduceTasks(0);

    job.setJarByClass(Useragent.class);
    job.setMapperClass(Useragent.UseragentMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
