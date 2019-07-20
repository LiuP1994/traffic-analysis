package cn.com.runtrend.analysis.commons;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: 张乔齐
 * @Description: 字符串操作类
 * @Date: 2017/8/14
 * @Modified By:
 */
public class StringUtils {

  /**
   * 正则提取字符串
   *
   * @param regex 正则
   * @return 字符串
   */
  public static String regex(String regex, String str) {
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(str);
    if (matcher.find()) {
      if (matcher.groupCount() > 0) {
        return matcher.group(1);
      }
    }
    return "";
  }

}
