package cn.com.runtrend.analysis.hadoop.etl;

import junit.framework.TestCase;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 作者：张乔齐
 * cn.com.runtrend.analysis.hadoop.etl
 * 2017/8/23
 * 简介：<描述本类作用>
 */
public class uaTest extends TestCase {
  public boolean testMain() throws Exception {
    String target = "";//添加的流量
    String rule = "";//等待校验的正则
    Pattern pattern = Pattern.compile(rule);
    Matcher matcher = pattern.matcher(target);
    if (matcher.find()) {
      return true;//成功
      }
    return false;//失败
  }
}