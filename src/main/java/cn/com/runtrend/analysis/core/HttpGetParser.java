package cn.com.runtrend.analysis.core;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: 张乔齐
 * @Description: 用于解析 http get 请求中的参数
 * @Date: 2017/8/8
 * @Modified By:
 */
public class HttpGetParser {

  /**
   * 在请求中根据关键字找到所要的值
   *
   * @param httpGet http get 流量
   * @param keyWord 关键字
   * @return value
   */
  public static String parseParam(String httpGet, String keyWord) {
    if (httpGet == null || "".equals(httpGet) || keyWord == null || keyWord.equals("")) {
      return "";
    }
    if (httpGet.contains("&" + keyWord + "=")) {
      int keyIndex = httpGet.indexOf("&" + keyWord + "=");
      return findValue(keyIndex, keyWord, httpGet);
    } else if (httpGet.contains("?" + keyWord + "=")) {
      int keyIndex = httpGet.indexOf("?" + keyWord + "=");
      return findValue(keyIndex, keyWord, httpGet);
    } else {
      return "";
    }
  }

  /**
   * 根据关键字下标返回对应的值
   *
   * @param keyIndex 关键字下标
   * @param keyWord 关键字
   * @param httpGet http get 流量
   * @return value
   */
  private static String findValue(int keyIndex, String keyWord, String httpGet) {
    int keyIndexLast = keyIndex + keyWord.length() + 2;
    int index = httpGet.substring(keyIndexLast).indexOf("&");
    if (index >= 0) {
      return httpGet.substring(keyIndexLast, keyIndexLast + index);
    } else {
      return httpGet.substring(keyIndexLast);
    }
  }

  /**
   * 返回所有参数值列表
   *
   * @param httpGet http get 流量
   * @return param list
   */
  public static List<String> paramList(String httpGet) {
    List<String> paramList = new ArrayList<String>();
    if (httpGet == null || "".equals(httpGet)) {
      return paramList;
    }
    int index = httpGet.indexOf("?");
    if (index > 0) {
      String[] params = httpGet.substring(index).split("&");
      for (String param : params) {
        int idx = param.indexOf("=");
        if (idx > 0) {
          String value = param.substring(idx + 1);
          if (!value.equals("")) {
            paramList.add(value);
          }
        }
      }
    }

    return paramList;
  }

  public static List<String> paramList2(String httpGet) {
    List<String> paramList = new ArrayList<String>();
    if (httpGet == null || "".equals(httpGet)) {
      return paramList;
    }

    int index = httpGet.indexOf(",");
    if (index > 0) {
      String params[] = httpGet.split(",", -1);
      for (String p : params) {
        int pindex = p.indexOf(":");
        if (pindex > 0 && p.length() > pindex + 2) {
          String temp = p.substring(pindex + 1).replaceAll("\"", "").replaceAll("\\{", "")
              .replaceAll("\\}", "");
          if (!"".equals(temp)) {
            paramList.add(temp);
          }
        } else {
          String temp = p.replaceAll("\"", "").replaceAll("\\{", "").replaceAll("\\}", "");
          if (!"".equals(temp)) {
            paramList.add(temp);
          }
        }
      }
    }
    return paramList;
  }
}