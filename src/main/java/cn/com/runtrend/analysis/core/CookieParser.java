package cn.com.runtrend.analysis.core;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: 张乔齐
 * @Description: 用于解析 cookie 中的参数
 * @Date: 2017/8/8
 * @Modified By:
 */
public class CookieParser {

  /**
   * 根据分割标记提取 cookie
   *
   * @param cookie cookie
   * @param tag 分割标记
   * @return cookies
   */
  public static Map<String, String> cookieWithTag(String cookie, String tag) {
    String cook = cookie.replaceAll("\\s", "");
    String[] cookieParam = cook.split(tag);
    Map<String, String> cookies = new HashMap<String, String>();
    if (cookieParam.length > 0) {
      for (String cp : cookieParam) {
        String[] p = cp.split("=");
        if (p.length == 2) {
          cookies.put(p[0], p[1]);
        }
      }
    }
    return cookies;
  }

  public static String parseParam(String cookie, String key) {
    Map<String, String> map = cookieWithTag(cookie, ";");
    if (map.containsKey(key)) {
      return map.get(key);
    }
    return "";
  }

}
