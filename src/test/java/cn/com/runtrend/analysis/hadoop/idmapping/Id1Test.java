package cn.com.runtrend.analysis.hadoop.idmapping;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作者：张乔齐
 * cn.com.runtrend.analysis.hadoop.idm4
 * 2017/9/6
 * 简介：<描述本类作用>
 */
public class Id1Test extends TestCase {
  public void testUidImeiTerminal() throws Exception {

    List<String> values = new ArrayList<String>();
    values.add("imei_868930023106502,terminal_小米5（标准版/全网通）");
    values.add("uid_QQ:o2658717130,terminal_小米5（标准版/全网通）");
    values.add("uid_QQ:o2658717130,imei_86000999999999999");

    Map<String, List<String>> terminal_imei = get_terminal_imei(values);
    Map<String, List<String>> uid_terminal = get_uid_terminal(values);
    Map<String, List<String>> uid_imei = get_uid_imei(values);

    System.out.println(terminal_imei);
    System.out.println(uid_terminal);
    System.out.println(uid_imei);
    for (Map.Entry<String, List<String>> entry_uid : uid_terminal.entrySet()) {
      if (entry_uid.getValue().size() == 1) {
        String terminal = entry_uid.getValue().get(0);
        if (terminal_imei.containsKey(terminal)) {
          if (terminal_imei.get(terminal).size() == 1) {
            System.out.println((terminal_imei.get(terminal).get(0) + "\t"
                    + entry_uid.getKey() + "\t" + terminal));
          }
        }
      }
    }
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

}