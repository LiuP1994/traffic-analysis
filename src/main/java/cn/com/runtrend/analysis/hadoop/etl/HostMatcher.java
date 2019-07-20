package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.commons.Tld;
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
 * @Description: 根据host匹配终端名称
 * @Date: 2017/8/21
 * @Modified By:
 */
public class HostMatcher {

  public static class HostMatcherMapper extends Mapper<Object, Text, Text, NullWritable> {

    Trie.TrieTree tree = new Trie.TrieTree();

    @Override
    protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
        throws IOException, InterruptedException {
      BufferedReader br = HdfsUtil.readFile(context.getConfiguration().get("path"), context);
      String line;
      while ((line = br.readLine()) != null) {
        String[] hostDetail = UnicodeDecode.decodeExt(line).split(Constants.FIELDS_TERMINATED, -1);
        tree.insert(hostDetail[2],
            new TrieObject.Builder().mInt1(1).mString1(hostDetail[0]).mString2(hostDetail[1])
                .build());
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
      if (!row.isEmpty() && fieldMap.get("host") < row.size()) {
        String host = row.get(fieldMap.get("host"));
        if (null != host && !"".equals(host)) {
          List<String> domainList = Tld.getTlds(host);
          for (String domain : domainList) {
            TrieObject trieObject = tree.search(domain);
            if (trieObject.getmInt1() == 1) {
              result.set(result.size() - 2, trieObject.getmString2());
              result.set(result.size() - 1, trieObject.getmString1());
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
      System.err.println("HostMatcher <in> <join> <out> <fields>");
      System.exit(2);
    }
    conf.set("fields", otherArgs[3]);
    conf.set("path", otherArgs[1]);
    
    Job job = Job.getInstance(conf, "HostMatcher");
    job.setJobName("traffic_bi_MR_HostMatcher");

    job.setNumReduceTasks(0);

    job.setJarByClass(HostMatcher.class);
    job.setMapperClass(HostMatcher.HostMatcherMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
