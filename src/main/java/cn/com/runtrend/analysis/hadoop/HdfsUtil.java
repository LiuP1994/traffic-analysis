package cn.com.runtrend.analysis.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Auther: 张乔齐
 * @Description: HDFS操作工具类
 * @Date: 2017/9/1
 * @Modified By:
 */
public class HdfsUtil {

  public static BufferedReader readFile(String path, Mapper.Context context) throws IOException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream inputStream = fs.open(new Path(path));
    return new BufferedReader(
        new InputStreamReader(inputStream));
  }
}
