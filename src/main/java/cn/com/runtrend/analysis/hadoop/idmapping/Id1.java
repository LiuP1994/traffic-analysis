package cn.com.runtrend.analysis.hadoop.idmapping;

import cn.com.runtrend.analysis.hadoop.MapreduceField;
import cn.com.runtrend.analysis.hadoop.MrProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @Auther: 张乔齐
 * @Description: ID图谱基类，生成号码，账号，终端及终端码、mac之间的关系
 * @Date: 2017/9/6
 * @Modified By:
 */
public class Id1 {

  public static boolean run(String[] args) {
    Configuration conf = new Configuration();
    try {
      String[] otherArgs = new GenericOptionsParser(
          conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
        System.exit(2);
      }

      conf.set("fields", otherArgs[1]);
      Job job = Job.getInstance(conf, "Id1");
      job.setJobName("traffic_bi_MR_Id1");
      conf.setBoolean("mapred.compress.map.output", true);
      conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[0] + "_id1"));

      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

      configureJob(job);

      return job.waitForCompletion(true);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  private static void configureJob(Job job) {
    job.setJarByClass(Id1.class);

    job.setMapperClass(Id1.Id1Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(Id1.Id1Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  }

  public static class Id1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      MrProperty mrProperty = MapreduceField
          .getMrProperty(value.toString(), context.getConfiguration().get("fields"));
      Map<String, Integer> fieldMap = mrProperty.getFieldMap();
      List<String> row = mrProperty.getRow();

      if (!row.isEmpty() && fieldMap.get("username") < row.size() && fieldMap.get("phone") < row
          .size()
          && fieldMap.get("imei") < row.size() && fieldMap.get("mac") < row.size()
          && fieldMap.get("idfa") < row.size() && fieldMap.get("uid") < row.size()
          && fieldMap.get("ua") < row.size() && fieldMap.get("terminal_name") < row.size()) {
        IDModel idModel = new IDModel();
        idModel.setUsername(row.get(fieldMap.get("username")));
        idModel.setPhone(row.get(fieldMap.get("phone")));
        idModel.setImei(row.get(fieldMap.get("imei")));
        idModel.setMac(row.get(fieldMap.get("mac")));
        idModel.setIdfa(row.get(fieldMap.get("idfa")));
        idModel.setUid(row.get(fieldMap.get("uid")));
        idModel.setUa(row.get(fieldMap.get("ua")));
        idModel.setTerminal_name(row.get(fieldMap.get("terminal_name")));

        setUsernameKey(idModel, context);

      }
    }

    private void setUsernameKey(IDModel idModel, Context context)
        throws IOException, InterruptedException {
      if (!"".equals(idModel.getPhone()) || !"".equals(idModel.getImei()) || !""
          .equals(idModel.getMac())
          || !"".equals(idModel.getIdfa()) || !"".equals(idModel.getUid())) {

        if ((idModel.getUa().toLowerCase().contains("ios") || idModel.getUa().toLowerCase()
            .contains("iphone"))
            && !"".equals(idModel.getIdfa())) {
          String[] idfaArray = idModel.getIdfa().split(",", -1);
          String[] macArray = idModel.getMac().split(",", -1);
          if (idfaArray.length > macArray.length) {
            for (String idfa : idfaArray) {
              for (String mac : macArray) {
                addResult("idfa_" + idfa, "mac_" + mac, idModel, context);
              }
            }
          } else {
            for (String mac : macArray) {
              for (String idfa : idfaArray) {
                addResult("idfa_" + idfa, "mac_" + mac, idModel, context);
              }
            }
          }
        } else {
          String[] imeiArray = idModel.getImei().split(",", -1);
          String[] macArray = idModel.getMac().split(",", -1);
          if (imeiArray.length > macArray.length) {
            for (String idfa : imeiArray) {
              for (String mac : macArray) {
                addResult("imei_" + idfa, "mac_" + mac, idModel, context);
              }
            }
          } else {
            for (String mac : macArray) {
              for (String idfa : imeiArray) {
                addResult("imei_" + idfa, "mac_" + mac, idModel, context);
              }
            }
          }
        }
      }
    }

    private void addResult(String tid, String mac, IDModel idModel, Context context)
        throws IOException, InterruptedException {

      String[] phoneArray = idModel.getPhone().split(",", -1);
      if (phoneArray.length > 0) {
        for (String phone : phoneArray) {
          idModel.setPhone(phone);
          ckeckResult(tid, mac, idModel, context);
        }
      } else {
        ckeckResult(tid, mac, idModel, context);
      }
    }

    private void ckeckResult(String tid, String mac, IDModel idModel,
        Context context) throws IOException, InterruptedException {
      List<String> result = new ArrayList<String>();
      if (tid.startsWith("idfa_") && tid.length() > 5) {
        result.add(tid);
      }
      if (tid.startsWith("imei_") && tid.length() > 5) {
        result.add(tid);
      }
      if (mac.startsWith("mac_") && mac.length() > 5) {
        result.add(mac);
      }
      if (!"".equals(idModel.getPhone())) {
        result.add("phone_" + idModel.getPhone());
      }
      if (!"".equals(idModel.getUid())) {
        result.add("uid_" + idModel.getUid());
      }
      if (!"".equals(idModel.getTerminal_name())) {
        result.add("terminal_" + idModel.getTerminal_name());
      }
      setResult(context, result, idModel);
    }

    private void setResult(
        Context context, List<String> result, IDModel idModel)
        throws IOException, InterruptedException {
      if (result.size() > 0) {
        if (result.size() == 1) {
          if (!result.get(0).equals("terminal_" + idModel.getTerminal_name())) {
            context.write(new Text(idModel.getUsername()),
                new Text(MapreduceField.list2String(result).replaceAll("\t", ",")));
          }
        } else {
          context.write(new Text(idModel.getUsername()),
              new Text(MapreduceField.list2String(result).replaceAll("\t", ",")));
        }
      }
    }

  }

  public static class Id1Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      List<String> dataList = new ArrayList<String>();
      for (Text value : values) {
        dataList.add(value.toString());
      }
      Map<String, List<String>> terminal_imei = get_terminal_imei(dataList);
      Map<String, List<String>> mac_terminal = get_mac_terminal(dataList);
      Map<String, List<String>> idfa_terminal = get_idfa_terminal(dataList);
      Map<String, List<String>> imei_terminal = get_imei_terminal(dataList);

      Map<String, List<String>> uid_terminal = get_uid_terminal(dataList);
      Map<String, List<String>> uid_imei = get_uid_imei(dataList);
      Map<String, List<String>> idfa_uid = get_idfa_uid(dataList);

      Map<String, List<String>> phone_terminal = get_phone_terminal(dataList);
      Map<String, List<String>> phone_imei = get_phone_imei(dataList);
      Map<String, List<String>> idfa_phone = get_idfa_phone(dataList);
      Map<String, List<String>> mac_phone = get_mac_phone(dataList);

      for (Map.Entry<String, List<String>> entry_uid_terminal : uid_terminal.entrySet()) {
        if (entry_uid_terminal.getValue().size() == 1) {
          String terminal = entry_uid_terminal.getValue().get(0);
          if (terminal_imei.containsKey(terminal)) {
            if (terminal_imei.get(terminal).size() == 1) {
              context.write(key, new Text(terminal_imei.get(terminal).get(0) + "\t"
                  + entry_uid_terminal.getKey() + "\t" + terminal));
            }
          }
        }
      }

      for (Map.Entry<String, List<String>> entry_uid_imei : uid_imei.entrySet()) {
        String uid = entry_uid_imei.getKey();
        if (uid_terminal.containsKey(uid)) {
          if (uid_terminal.get(uid).size() == 1) {
            String terminal = uid_terminal.get(uid).get(0);
            if (terminal_imei.containsKey(terminal)) {
              if (terminal_imei.get(terminal).size() == 1) {
                context.write(key, new Text(terminal_imei.get(terminal).get(0) + "\t"
                    + uid + "\t" + terminal));
              }
            }
          }
        }
      }

      for (Map.Entry<String, List<String>> entry_idfa_uid : idfa_uid.entrySet()) {
        if (entry_idfa_uid.getValue().size() == 1) {
          String idfa = entry_idfa_uid.getKey();
          String uid = entry_idfa_uid.getValue().get(0);
          String terminal = "";
          if (idfa_terminal.containsKey(idfa)) {
            if (idfa_terminal.get(idfa).size() == 1) {
              terminal = idfa_terminal.get(idfa).get(0);
            }
          }

          if (uid_terminal.containsKey(uid)) {
            terminal = uid_terminal.get(uid).get(0);
          }

          if ("".equals(terminal)) {
            terminal = "苹果iPhone";
          }

          context.write(key, new Text(idfa + "\t" + uid + "\t" + terminal));
        }
      }

      for (Map.Entry<String, List<String>> entry_phone_terminal : phone_terminal.entrySet()) {
        if (entry_phone_terminal.getValue().size() == 1) {
          String terminal = entry_phone_terminal.getValue().get(0);
          if (terminal_imei.containsKey(terminal)) {
            if (terminal_imei.get(terminal).size() == 1) {
              context.write(key, new Text(terminal_imei.get(terminal).get(0) + "\t"
                  + entry_phone_terminal.getKey() + "\t" + terminal));
            }
          }
        }
      }

      for (Map.Entry<String, List<String>> entry_idfa_phone : idfa_phone.entrySet()) {
        if (entry_idfa_phone.getValue().size() == 1) {
          String idfa = entry_idfa_phone.getKey();
          String phone = entry_idfa_phone.getValue().get(0);
          String terminal = "";
          if (idfa_terminal.containsKey(idfa)) {
            if (idfa_terminal.get(idfa).size() == 1) {
              terminal = idfa_terminal.get(idfa).get(0);
            }
          }

          if (idfa_terminal.containsKey(phone)) {
            terminal = idfa_terminal.get(phone).get(0);
          }

          if ("".equals(terminal)) {
            terminal = "苹果iPhone";
          }

          context.write(key, new Text(idfa + "\t" + phone + "\t" + terminal));
        }
      }

      for (Map.Entry<String, List<String>> entry_phone_imei : phone_imei.entrySet()) {
        String imei = entry_phone_imei.getValue().get(0);
        if (imei_terminal.containsKey(imei)) {
          String terminal = imei_terminal.get(imei).get(0);
          context.write(key, new Text(imei + "\t"
              + entry_phone_imei.getKey() + "\t" + terminal));
        } else {
          context.write(key, new Text(imei + "\t"
              + entry_phone_imei.getKey() + "\t" + ""));
        }
      }

      for (Map.Entry<String, List<String>> entry_mac_phone : mac_phone.entrySet()) {
        if (entry_mac_phone.getValue().size() == 1) {
          String phone = entry_mac_phone.getValue().get(0);
          if (mac_terminal.containsKey(entry_mac_phone.getKey())) {
            if (mac_terminal.get(entry_mac_phone.getKey()).size() == 1) {
              context.write(key, new Text(entry_mac_phone.getKey() + "\t"
                  + phone + "\t" + mac_terminal.get(entry_mac_phone.getKey()).get(0)));
            } else {
              context.write(key, new Text(entry_mac_phone.getKey() + "\t"
                  + phone + "\t" + ""));
            }
          } else {
            context.write(key, new Text(entry_mac_phone.getKey() + "\t"
                + phone + "\t" + ""));
          }
        }
      }

      for (Map.Entry<String, List<String>> entry_mac_terminal : mac_terminal.entrySet()) {
        if (entry_mac_terminal.getValue().size() == 1) {
          context.write(key, new Text(entry_mac_terminal.getKey() + "\t"
              + entry_mac_terminal.getValue().get(0)));
        }
      }

      for (Map.Entry<String, List<String>> entry_idfa_terminal : idfa_terminal.entrySet()) {
        if (entry_idfa_terminal.getValue().size() == 1) {
          context.write(key, new Text(entry_idfa_terminal.getKey() + "\t"
              + entry_idfa_terminal.getValue().get(0)));
        }
      }

      for (Map.Entry<String, List<String>> entry_imei_terminal : imei_terminal.entrySet()) {
        if (entry_imei_terminal.getValue().size() == 1) {
          context.write(key, new Text(entry_imei_terminal.getKey() + "\t"
              + entry_imei_terminal.getValue().get(0)));
        }
      }
    }

    private Map<String, List<String>> get_mac_phone(List<String> values) {
      Map<String, List<String>> mac_phone = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String phone = "";
        String mac = "";
        for (String val : vals) {
          if (val.startsWith("mac")) {
            mac = val;
          }
          if (val.startsWith("phone")) {
            phone = val;
          }
        }
        if (!"".equals(phone) && !"".equals(mac)) {
          if (!mac_phone.containsKey(mac)) {
            mac_phone.put(mac, new ArrayList<String>());
          }
          if (mac_phone.containsKey(mac)) {
            List<String> list = mac_phone.get(mac);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(phone)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(phone);
            }
            mac_phone.put(mac, list);
          }
        }
      }
      return mac_phone;
    }


    private Map<String, List<String>> get_imei_terminal(List<String> values) {
      Map<String, List<String>> imei_terminal = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String terminal = "";
        String imei = "";
        for (String val : vals) {
          if (val.startsWith("imei")) {
            imei = val;
          }
          if (val.startsWith("terminal")) {
            terminal = val;
          }
        }
        if (!"".equals(terminal) && !"".equals(imei)) {
          if (!imei_terminal.containsKey(imei)) {
            imei_terminal.put(imei, new ArrayList<String>());
          }
          if (imei_terminal.containsKey(imei)) {
            List<String> list = imei_terminal.get(imei);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(terminal)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(terminal);
            }
            imei_terminal.put(imei, list);
          }
        }
      }
      return imei_terminal;
    }

    private Map<String, List<String>> get_idfa_phone(List<String> values) {
      Map<String, List<String>> idfa_phone = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String phone = "";
        String idfa = "";
        for (String val : vals) {
          if (val.startsWith("idfa")) {
            idfa = val;
          }
          if (val.startsWith("phone")) {
            phone = val;
          }
        }
        if (!"".equals(phone) && !"".equals(idfa)) {
          if (!idfa_phone.containsKey(idfa)) {
            idfa_phone.put(idfa, new ArrayList<String>());
          }
          if (idfa_phone.containsKey(idfa)) {
            List<String> list = idfa_phone.get(idfa);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(phone)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(phone);
            }
            idfa_phone.put(idfa, list);
          }
        }
      }
      return idfa_phone;
    }

    private Map<String, List<String>> get_idfa_uid(List<String> values) {
      Map<String, List<String>> idfa_uid = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String uid = "";
        String idfa = "";
        for (String val : vals) {
          if (val.startsWith("idfa")) {
            idfa = val;
          }
          if (val.startsWith("uid")) {
            uid = val;
          }
        }
        if (!"".equals(uid) && !"".equals(idfa)) {
          if (!idfa_uid.containsKey(idfa)) {
            idfa_uid.put(idfa, new ArrayList<String>());
          }
          if (idfa_uid.containsKey(idfa)) {
            List<String> list = idfa_uid.get(idfa);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(uid)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(uid);
            }
            idfa_uid.put(idfa, list);
          }
        }
      }
      return idfa_uid;
    }

    private Map<String, List<String>> get_idfa_terminal(List<String> values) {
      Map<String, List<String>> idfa_terminal = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String terminal = "";
        String idfa = "";
        for (String val : vals) {
          if (val.startsWith("idfa")) {
            idfa = val;
          }
          if (val.startsWith("terminal")) {
            terminal = val;
          }
        }
        if (!"".equals(terminal) && !"".equals(idfa)) {
          if (!idfa_terminal.containsKey(idfa)) {
            idfa_terminal.put(idfa, new ArrayList<String>());
          }
          if (idfa_terminal.containsKey(idfa)) {
            List<String> list = idfa_terminal.get(idfa);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(terminal)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(terminal);
            }
            idfa_terminal.put(idfa, list);
          }
        }
      }
      return idfa_terminal;
    }

    private Map<String, List<String>> get_mac_terminal(List<String> values) {
      Map<String, List<String>> mac_terminal = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String terminal = "";
        String mac = "";
        for (String val : vals) {
          if (val.startsWith("mac")) {
            mac = val;
          }
          if (val.startsWith("terminal")) {
            terminal = val;
          }
        }
        if (!"".equals(terminal) && !"".equals(mac)) {
          if (!mac_terminal.containsKey(mac)) {
            mac_terminal.put(mac, new ArrayList<String>());
          }
          if (mac_terminal.containsKey(mac)) {
            List<String> list = mac_terminal.get(mac);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(terminal)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(terminal);
            }
            mac_terminal.put(mac, list);
          }
        }
      }
      return mac_terminal;
    }

    private Map<String, List<String>> get_phone_imei(List<String> values) {
      Map<String, List<String>> phone_imei = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String imei = "";
        String phone = "";
        for (String val : vals) {
          if (val.startsWith("phone")) {
            phone = val;
          }
          if (val.startsWith("imei")) {
            imei = val;
          }
        }
        if (!"".equals(imei) && !"".equals(phone)) {
          if (!phone_imei.containsKey(phone)) {
            phone_imei.put(phone, new ArrayList<String>());
          }
          if (phone_imei.containsKey(phone)) {
            List<String> list = phone_imei.get(phone);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(imei)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(imei);
            }
            phone_imei.put(phone, list);
          }
        }
      }
      return phone_imei;
    }

    private Map<String, List<String>> get_phone_terminal(List<String> values) {
      Map<String, List<String>> phone_terminal = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String phone = "";
        String terminal = "";
        String imei = "";
        for (String val : vals) {
          if (val.startsWith("phone")) {
            phone = val;
          }
          if (val.startsWith("terminal")) {
            terminal = val;
          }
          if (val.startsWith("imei")) {
            imei = val;
          }
        }
        if (!"".equals(terminal) && !"".equals(phone) && "".equals(imei)) {
          if (!phone_terminal.containsKey(phone)) {
            phone_terminal.put(phone, new ArrayList<String>());
          }
          if (phone_terminal.containsKey(phone)) {
            List<String> list = phone_terminal.get(phone);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(terminal)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(terminal);
            }
            phone_terminal.put(phone, list);
          }
        }
      }
      return phone_terminal;
    }

    private Map<String, List<String>> get_uid_imei(List<String> values) {
      Map<String, List<String>> uid_imei = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String imei = "";
        String uid = "";
        for (String val : vals) {
          if (val.startsWith("uid")) {
            uid = val;
          }
          if (val.startsWith("imei")) {
            imei = val;
          }
        }
        if (!"".equals(imei) && !"".equals(uid)) {
          if (!uid_imei.containsKey(uid)) {
            uid_imei.put(uid, new ArrayList<String>());
          }
          if (uid_imei.containsKey(uid)) {
            List<String> list = uid_imei.get(uid);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(imei)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(imei);
            }
            uid_imei.put(uid, list);
          }
        }
      }
      return uid_imei;
    }

    private Map<String, List<String>> get_uid_terminal(List<String> values) {
      Map<String, List<String>> uid_terminal = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String terminal = "";
        String uid = "";
        for (String val : vals) {
          if (val.startsWith("uid")) {
            uid = val;
          }
          if (val.startsWith("terminal")) {
            terminal = val;
          }
        }
        if (!"".equals(terminal) && !"".equals(uid)) {
          if (!uid_terminal.containsKey(uid)) {
            uid_terminal.put(uid, new ArrayList<String>());
          }
          if (uid_terminal.containsKey(uid)) {
            List<String> list = uid_terminal.get(uid);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(terminal)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(terminal);
            }
            uid_terminal.put(uid, list);
          }
        }
      }
      return uid_terminal;
    }

    private Map<String, List<String>> get_terminal_imei(List<String> values) {
      Map<String, List<String>> terminal_imei = new HashMap<String, List<String>>();
      for (String value : values) {
        String[] vals = value.split(",", -1);
        String terminal = "";
        String imei = "";
        for (String val : vals) {
          if (val.startsWith("terminal")) {
            terminal = val;
          }
          if (val.startsWith("imei")) {
            imei = val;
          }
        }
        if (!"".equals(terminal) && !"".equals(imei)) {
          if (!terminal_imei.containsKey(terminal)) {
            terminal_imei.put(terminal, new ArrayList<String>());
          }
          if (terminal_imei.containsKey(terminal)) {
            List<String> list = terminal_imei.get(terminal);
            boolean flag = true;
            for (String li : list) {
              if (li.equals(imei)) {
                flag = false;
              }
            }
            if (flag) {
              list.add(imei);
            }
            terminal_imei.put(terminal, list);
          }
        }
      }
      return terminal_imei;
    }
  }
}

class IDModel {

  private String username;
  private String phone;
  private String imei;
  private String mac;
  private String idfa;
  private String uid;
  private String ua;
  private String terminal_name;

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPhone() {
    return phone;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public String getImei() {
    return imei;
  }

  public void setImei(String imei) {
    this.imei = imei;
  }

  public String getMac() {
    return mac;
  }

  public void setMac(String mac) {
    this.mac = mac;
  }

  public String getIdfa() {
    return idfa;
  }

  public void setIdfa(String idfa) {
    this.idfa = idfa;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public String getUa() {
    return ua;
  }

  public void setUa(String ua) {
    this.ua = ua;
  }

  public String getTerminal_name() {
    return terminal_name;
  }

  public void setTerminal_name(String terminal_name) {
    this.terminal_name = terminal_name;
  }
}
