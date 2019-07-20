package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.core.Trie;
import cn.com.runtrend.analysis.core.TrieObject;
import cn.com.runtrend.analysis.core.UnicodeDecode;
import cn.com.runtrend.analysis.hadoop.HdfsUtil;
import cn.com.runtrend.analysis.hadoop.MapreduceField;
import cn.com.runtrend.analysis.hadoop.MrProperty;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @Auther: 张乔齐
 * @Description: mr程序 根据手机号码获取运营商信息
 * @Date: 2017/8/8
 * @Modified By:
 */
public class CorpArea {

  public static class CorpAreaMapper extends Mapper<Object, Text, Text, NullWritable> {

    Trie.TrieTree tree = new Trie.TrieTree();

    @Override
    protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
        throws IOException, InterruptedException {
      BufferedReader br = HdfsUtil.readFile(context.getConfiguration().get("path"), context);
      String line;
      while ((line = br.readLine()) != null) {
        String[] result = UnicodeDecode.decodeExt(line).split(",", -1);
        if (result.length == 7) {
          tree.insert(result[1],
              new TrieObject.Builder().mString1(result[4]).mString2(result[5]).build());
        }
      }
      br.close();
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
      result.add("");

      // 当行内不为空，并且手机号码下标在行内
      if (!row.isEmpty() && fieldMap.get("phone") < row.size()) {
        String phone = row.get(fieldMap.get("phone"));
        // 当号码不为空
        if (null != phone && !"".equals(phone)) {
          // 当号码为11位
          if (phone.length() == 11) {
            String k = phone.substring(0, 7);
            // 当手机号码头在配置文件内
            TrieObject trieObject = tree.search(k);
            if (!"".equals(trieObject.getmString1())) {
              result.set(result.size() - 2, trieObject.getmString1());
              result.set(result.size() - 1, trieObject.getmString2());
            }
          }
        }
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
      System.err.println("CorpArea <in> <join> <out> <fields>");
      System.exit(2);
    }
    conf.set("fields", otherArgs[3]);
    conf.set("path", otherArgs[1]);
    //conf.setBoolean("mapred.compress.map.output", true);  
    //conf.setClass("mapred.map.output.compression.codec",GzipCodec.class,CompressionCodec.class);  

    Job job = Job.getInstance(conf, "CorpArea");
    job.setJobName("traffic_bi_MR_CorpArea");

    job.setNumReduceTasks(0);

    job.setJarByClass(CorpArea.class);
    job.setMapperClass(CorpAreaMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}