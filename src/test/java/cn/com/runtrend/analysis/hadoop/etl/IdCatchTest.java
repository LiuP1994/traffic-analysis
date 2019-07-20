package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.core.TrieObject;
import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * 作者：张乔齐
 * cn.com.runtrend.analysis.hadoop.etl
 * 2017/8/23
 * 简介：<描述本类作用>
 */
public class IdCatchTest extends TestCase {
  public boolean testMain() throws Exception {
    String target = "";//添加的流量
    String rule = "";//等待校验的规则
    boolean result = false;
    String[] s = rule.split(";", -1);
    String[] hosts = s[s.length - 1].split(",", -1);
    for (String host : hosts) {
      if (target.contains(host)) {
        result = true;
      }
    }
    if (result) {
      String getuid = "";
      String cookieuid = "";
      String getf1 = "";
      String cookief1 = "";
      String getf2 = "";
      String cookief2 = "";
      String getf3 = "";
      String cookief3 = "";
      for (String field : s) {
        if (field.startsWith("getuid:")) {
          getuid = field.replaceAll("getuid:", "");
          String[] ts=getuid.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
        if (field.startsWith("cookieuid:")) {
          cookieuid = field.replaceAll("cookieuid:", "");
          String[] ts=cookieuid.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
        if (field.startsWith("getf1:")) {
          getf1 = field.replaceAll("getf1:", "");
          String[] ts=getf1.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
        if (field.startsWith("cookief1:")) {
          cookief1 = field.replaceAll("cookief1:", "");
          String[] ts=cookief1.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
        if (field.startsWith("getf2:")) {
          getf2 = field.replaceAll("getf2:", "");
          String[] ts=getf2.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
        if (field.startsWith("cookief2:")) {
          cookief2 = field.replaceAll("cookief2:", "");
          String[] ts=cookief2.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
        if (field.startsWith("getf3:")) {
          getf3 = field.replaceAll("getf3:", "");
          String[] ts=getf3.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
        if (field.startsWith("cookief3:")) {
          cookief3 = field.replaceAll("cookief3:", "");
          String[] ts=cookief3.split(",", -1);
          for (String t:ts){
            if (target.contains(t+"=")){
              return true;
            }
          }
        }
      }
    }return false;//失败
  }
}