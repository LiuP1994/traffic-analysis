package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.commons.StringUtils;
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
 * @Description: 终端型号对应终端名称匹配
 * @Date: 2017/8/17
 * @Modified By:
 */
public class TerminalName {

  public static class TerminalNameMapper extends Mapper<Object, Text, Text, NullWritable> {

    Trie.TrieTree tree = new Trie.TrieTree();
    Trie.TrieTree iphoneTree = new Trie.TrieTree();

    @Override
    protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
        throws IOException, InterruptedException {
      {
        BufferedReader br = HdfsUtil.readFile(context.getConfiguration().get("path"), context);
        String line;
        while ((line = br.readLine()) != null) {
          String[] terminalDetail = UnicodeDecode.decodeExt(line)
              .split(Constants.FIELDS_TERMINATED, -1);
          String terminal1 = terminalDetail[3];
          String terminal2 = terminalDetail[4];
          addTree("[\u4e00-\u9fa5]+([a-zA-Z0-9\\s\\-\\_\\+\\.]+).*", terminal1, terminal1);
          addTree("([\u4e00-\u9fa5]+[a-zA-Z0-9\\s\\-\\_\\+\\.]+).*", terminal1, terminal1);
          addTree("([a-zA-Z0-9\\s\\-\\_\\+\\.]+).*", terminal1, terminal1);
          addTree(".*（([a-zA-Z0-9\\-\\s\\_\\+\\.]+).*", terminal1, terminal1);
          addTree(".*/([a-zA-Z0-9\\-\\s\\_\\+\\.]+).*", terminal1, terminal1);
          String[] tmnls = terminal2.split(",");
          for (String tmnl : tmnls) {
            addTree("^[\u4e00-\u9fa5]+([a-zA-Z0-9\\s\\-\\_\\+\\.]+)$", tmnl, terminal1);
            addTree("^([a-zA-Z0-9\\-\\s\\_\\+\\.]+)$", tmnl, terminal1);
            addTree("^([0-9]+)$", tmnl, terminal1);
          }
        }
        br.close();
      }
      {
        BufferedReader br = HdfsUtil.readFile(context.getConfiguration().get("path1"), context);
        String line;
        while ((line = br.readLine()) != null) {
          String[] terminalDetail = line.split("=", -1);
          String terminal1 = terminalDetail[0];
          String terminal2 = terminalDetail[1];
          iphoneTree.insert(terminal1, new TrieObject.Builder().mString1(terminal2).build());
        }
        br.close();
      }
    }

    private void addTree(String regx, String terminal, String terminalName) {
      String tmnl = StringUtils.regex(regx, terminal).trim().toLowerCase();
      if (!tmnl.equals("") && !tmnl.equals("null")) {
        if (tmnl.length() >= 3) {
          if (!Pattern.matches("^[0-9]+gb\\s.*$", tmnl) && !Pattern.matches("^.*[0-9]+gb$", tmnl)
              && !Pattern.matches("^[0-9]+gb$", tmnl)) {
            if (tree.search(tmnl).getmString1().equals("")) {
              tree.insert(tmnl, new TrieObject.Builder().mString1(terminalName).build());
            }
          }
        }
      }
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

      if (!row.isEmpty() && fieldMap.get("terminal") < row.size()) {
        String terminal = row.get(fieldMap.get("terminal"));
        terminal = filterTerminal(terminal);
        if (!terminal.equals("") && terminal.length() > 2) {
          boolean flag = false;
          TrieObject trieObject = iphoneTree.search(terminal);
          if (!trieObject.getmString1().equals("")) {
            result.set(result.size() - 1, trieObject.getmString1());
            flag = true;
          }
          if (!flag) {
            TrieObject trieObject2 = tree.search(terminal);
            result.set(result.size() - 1, trieObject2.getmString1());
          }
        }
      }

      Text data = new Text();
      data.set(MapreduceField.list2String(result));
      context.write(data, NullWritable.get());
    }

    private String filterTerminal(String terminal) {
      String fTerminal = terminal;
      if (terminal.contains("xiaomi")) {
        fTerminal = terminal.replace("xiaomi", "");
      }

      if (terminal.equals("pad")) {
        fTerminal = "";
      }
      if (Pattern.compile("^m[0-9]$").matcher(fTerminal).find()) {
        if (terminal.equals("m1")) {
          fTerminal = "m456m";
        } else {
          fTerminal = terminal.replace("m", "魅族魅蓝");
        }
      }

      if (terminal.startsWith("konka android tv")) {
        fTerminal = "konka android tv";
      }
      if (terminal.startsWith("baofeng_tv")) {
        fTerminal = "baofeng_tv";
      }
      if (terminal.startsWith("android tv on tcl") || terminal.startsWith("androidtvontcl")
          || terminal.startsWith("android_tv_on_tcl")) {
        fTerminal = "android tv on tcl";
      }
      if (terminal.startsWith("android tv on haier") || terminal.startsWith("full aosp on haier")) {
        fTerminal = "android tv on haier";
      }
      if (terminal.startsWith("kktv")) {
        fTerminal = "kktv";
      }

      return fTerminal;
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.task.timeout", Constants.MAPRED_TASK_TIMEOUT);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 5) {
      System.err.println("TerminalName <in> <join1> <join2> <out> <fields>");
      System.exit(2);
    }
    conf.set("fields", otherArgs[4]);
    conf.set("path", otherArgs[1]);
    conf.set("path1", otherArgs[2]);

    Job job = Job.getInstance(conf, "TerminalName");
    job.setJobName("traffic_bi_MR_TerminalName");

    job.setNumReduceTasks(0);

    job.setJarByClass(TerminalName.class);
    job.setMapperClass(TerminalName.TerminalNameMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}