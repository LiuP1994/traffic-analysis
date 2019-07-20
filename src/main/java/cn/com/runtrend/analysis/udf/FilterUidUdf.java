package cn.com.runtrend.analysis.udf;

import cn.com.runtrend.analysis.commons.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Auther: 张乔齐
 * @Description:
 * @Date: 2017/12/7
 * @Modified By:
 */
public class FilterUidUdf extends UDF {

  public String evaluate(String tid, String uid) {
    String mUid = "";
    if (uid.startsWith("QQ:")) {
      mUid = filterByQQUid(uid);
    }
    if (uid.startsWith("爱奇艺:")) {
      mUid = filterByIqiyiUid(tid,uid);
    }
    if (uid.startsWith("淘宝:")) {
      mUid = filterByTaobaoUid(uid);
    }
    if (uid.startsWith("微博:")) {
      mUid = filterByWeiboUid(uid);
    }
    return mUid;
  }

  private String filterByWeiboUid(String uid) {
    return uid;
  }

  private String filterByTaobaoUid(String uid) {
    return uid;
  }

  private String filterByIqiyiUid(String tid,String uid) {
    if (null != uid && !"爱奇艺:null".equals(uid)) {
      uid = uid.replaceAll("爱奇艺:", "");
      if (tid.equals(uid)){
        return "";
      }
      if (uid.matches("^[0-9]+$")){
        return "爱奇艺:" + uid;
      }else{
        return "";
      }
    } else {
      return "";
    }
  }

  private String filterByQQUid(String uid) {
    if (null != uid && !"QQ:null".equals(uid)) {
      uid = uid.replaceAll("QQ:", "");
      if (uid.startsWith("o") || uid.startsWith("O")) {
        uid = uid.substring(1, uid.length());
      }
      while (uid.startsWith("0")) {
        uid = uid.substring(1, uid.length());
      }
      return "QQ:" + uid;
    } else {
      return "";
    }
  }
}
