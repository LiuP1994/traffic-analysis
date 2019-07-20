package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.commons.Tld;
import cn.com.runtrend.analysis.core.CookieParser;
import cn.com.runtrend.analysis.core.HttpGetParser;
import cn.com.runtrend.analysis.core.Trie;
import cn.com.runtrend.analysis.core.TrieObject;
import cn.com.runtrend.analysis.core.UnicodeDecode;
import cn.com.runtrend.analysis.hadoop.HdfsUtil;
import cn.com.runtrend.analysis.hadoop.MapreduceField;
import cn.com.runtrend.analysis.hadoop.MrProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @Auther: 张乔齐
 * @Description: 根据配置获取应用id
 * @Date: 2017/8/23
 * @Modified By:
 */
public class IdCatch {

    public static class IdCatchMapper extends Mapper<Object, Text, Text, NullWritable> {

        Trie.TrieTree tree = new Trie.TrieTree();

        @Override
        protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
            BufferedReader br = HdfsUtil.readFile(context.getConfiguration().get("path"), context);
            String line;
            while ((line = br.readLine()) != null) {
                String[] idmDetail = UnicodeDecode.decodeExt(line).split(";", -1);
                IdModel idModel = new IdModel();
                if (idmDetail.length >= 3) {
                    String[] hosts = idmDetail[2].split(",");
                    for (String field : idmDetail) {
                        if (field.startsWith("getuid:")) {
                            idModel.setGetuid(getValue(field, "getuid:"));
                        } else if (field.startsWith("cookieuid:")) {
                            idModel.setCookieuid(getValue(field, "cookieuid:"));
                        }
                    }
                    for (String host : hosts) {
                        tree.insert(host,
                                new TrieObject.Builder().mInt1(1).mString1(idmDetail[0])
                                        .mString2(idModel.getGetuid()).mString3(idModel.getCookieuid()).build());
                    }
                }
            }
            br.close();
        }

        private String getValue(String field, String kw) {
            if (field.startsWith(kw)) {
                return field.replaceAll(kw, "");
            }
            return "";
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            MrProperty mrProperty = MapreduceField
                    .getMrProperty(value.toString(), context.getConfiguration().get("fields"));
            Map<String, Integer> fieldMap = mrProperty.getFieldMap();
            List<String> row = mrProperty.getRow();
            List<String> result = new ArrayList<String>();
            result.add("");
            if (!row.isEmpty() && fieldMap.get("url") < row.size() && fieldMap.get("cookie") < row
                    .size()) {
                String url = row.get(fieldMap.get("url"));
                String cookie = row.get(fieldMap.get("cookie"));
                if (null != url && !"".equals(url)) {
                    List<String> domainList = Tld.getTlds(url);
                    for (String domain : domainList) {
                        TrieObject trieObject = tree.search(domain);
                        if (trieObject.getmInt1() == 1) {
                            String uid = getValue(url, cookie, trieObject.getmString2(),
                                    trieObject.getmString3());
                            if (!uid.equals("")) {
                                result.set(result.size() - 1,
                                        trieObject.getmString1() + ":" + uid);
                            }
                        }
                    }
                }
            }

            Text data = new Text();
            data.set(MapreduceField.list2String(result));
            context.write(data, NullWritable.get());
        }

        private String getValue(String url, String cookie, String gf, String cf) {
            String value = "";
            if (value.equals("")) {
                String[] params = gf.split(",");
                for (String param : params) {
                    String temp = HttpGetParser.parseParam(url, param);
                    if (!"".equals(temp)) {
                        value = temp;
                        break;
                    }
                }
            }
            if (value.equals("")) {
                String[] params = cf.split(",");
                for (String param : params) {
                    String temp = CookieParser.parseParam(cookie, param);
                    if (!"".equals(temp)) {
                        value = temp;
                        break;
                    }
                }
            }
            return value;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.task.timeout", Constants.MAPRED_TASK_TIMEOUT);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("IdCatch <in> <join> <out> <fields>");
            System.exit(2);
        }
        conf.set("fields", otherArgs[3]);
        conf.set("path", otherArgs[1]);
        Job job = Job.getInstance(conf, "IdCatch");
        job.setJarByClass(IdCatch.class);
        job.setMapperClass(IdCatch.IdCatchMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setJobName("NUPD");
        FileOutputFormat.setCompressOutput(job, false);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class IdModel {

    private String getuid = "";
    private String cookieuid = "";
    public String getGetuid() {
        return getuid;
    }

    public void setGetuid(String getuid) {
        this.getuid = getuid;
    }

    public String getCookieuid() {
        return cookieuid;
    }

    public void setCookieuid(String cookieuid) {
        this.cookieuid = cookieuid;
    }



}