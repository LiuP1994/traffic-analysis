package cn.com.runtrend.analysis.commons;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Created by Administrator on 12/10/2017.
 */
public class ConvertHdfsZipFileToGzipFile {

  public static boolean isRecur = false;
  public final static int NUM = 50;

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      errorMessage("1filesmerge [-r|-R] <hdfsTargetDir> <hdfsFileName>");
    }
    if (args[0].equals("-r") || args[0].equals("-R")) {
      isRecur = true;
    }
    if ((!isRecur && args.length != 4) || (!isRecur && args.length != 3)) {
      errorMessage(args[0]);
      errorMessage("2filesmerge [-r|-R] <hdfsTargetDir> <hdfsFileName>");
    }

    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);

    Path inputDir;
    Path hdfsFile;
    Text pcgroupText;

    // hadoop jar myjar.jar ConvertHdfsZipFileToGzipFile -r /zip/(待转换文件路径，在HDFS上) /user/j/pconline/(转换完成后的文件存储地址，也在HDFS上) pconline(待转换的文件名包含的字符)
    if (isRecur) {
      inputDir = new Path(args[1]);
      hdfsFile = new Path(args[2]);
      pcgroupText = new Text(args[3]);
    }
    // hadoop jar myjar.jar ConvertHdfsZipFileToGzipFile /zip/(待转换文件路径，在HDFS上) /user/j/pconline/(转换完成后的文件存储地址，也在HDFS上) pconline(待转换的文件名包含的字符)
    else {
      inputDir = new Path(args[0]);
      hdfsFile = new Path(args[1]);
      pcgroupText = new Text(args[2]);
    }

    if (!hdfs.exists(inputDir)) {
      errorMessage("3hdfsTargetDir not exist!");
    }
    if (hdfs.exists(hdfsFile)) {
      errorMessage("4hdfsFileName exist!");
    }
    merge(inputDir, hdfsFile, hdfs, pcgroupText);
    System.exit(0);
  }

  /**
   * @param inputDir zip文件的存储地址
   * @param hdfsFile 解压结果的存储地址
   * @param hdfs 分布式文件系统数据流
   * @param pcgroupText 需要解压缩的文件关键名
   * @author
   */
  public static void merge(Path inputDir, Path hdfsFile,
      FileSystem hdfs, Text pcgroupText) {
    try {
      //文件系统地址inputDir下的FileStatus
      FileStatus[] inputFiles = hdfs.listStatus(inputDir);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(NUM, NUM, 200, TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(NUM));
      for (int i = 0; i < inputFiles.length; i++) {
        if (!hdfs.isFile(inputFiles[i].getPath())) {
          if (isRecur) {
            merge(inputFiles[i].getPath(), hdfsFile, hdfs, pcgroupText);
            return;
          } else {
            System.out.println(inputFiles[i].getPath().getName()
                + "is not file and not allow recursion, skip!");
            continue;
          }
        }
        int flag = 0;
        while (flag == 0) {
          if (executor.getActiveCount() < NUM) {
            MergeTask mergeTask = new MergeTask();
            mergeTask.setHdfs(hdfs);
            mergeTask.setHdfsFile(hdfsFile);
            mergeTask.setInputFile(inputFiles[i]);
            mergeTask.setPcgroupText(pcgroupText);
            executor.execute(mergeTask);
            flag = 1;
          } else {
            flag = 0;
          }
        }
      }

      int flag = 0;
      while (flag == 0) {
        if (executor.getActiveCount() == 0) {
          executor.shutdown();
          flag = 1;
        } else {
          flag = 0;
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void errorMessage(String str) {
    System.out.println("Error Message: " + str);
    System.exit(1);
  }

}


class MergeTask implements Runnable {

  FileSystem hdfs;
  FileStatus inputFile;
  Text pcgroupText;
  Path hdfsFile;

  public void setHdfs(FileSystem hdfs) {
    this.hdfs = hdfs;
  }

  public void setInputFile(FileStatus inputFile) {
    this.inputFile = inputFile;
  }

  public void setPcgroupText(Text pcgroupText) {
    this.pcgroupText = pcgroupText;
  }

  public void setHdfsFile(Path hdfsFile) {
    this.hdfsFile = hdfsFile;
  }

  @Override
  public void run() {
    try {
      //判断文件名是否在需要解压缩的关键名内
      if (inputFile.getPath().getName().contains(pcgroupText.toString()) == true) {
        //输出待解压的文件名
        System.out.println(inputFile.getPath().getName());
        //将数据流指向待解压文件
        FSDataInputStream in = hdfs.open(inputFile.getPath());
        /**
         *数据的解压执行过程
         */
        ZipInputStream zipInputStream = null;
        zipInputStream = new ZipInputStream(in);
        ZipEntry entry;
        //解压后有多个文件一并解压出来并实现合并
        //合并后的地址
        FSDataOutputStream mergerout = hdfs.create(new Path(hdfsFile + File.separator +
            inputFile.getPath().getName()
                .substring(0, inputFile.getPath().getName().indexOf("."))));
        while ((entry = zipInputStream.getNextEntry()) != null) {
          int bygeSize1 = 2 * 1024 * 1024;
          byte[] buffer1 = new byte[bygeSize1];
          int nNumber;
          while ((nNumber = zipInputStream.read(buffer1, 0, bygeSize1)) != -1) {
            mergerout.write(buffer1, 0, nNumber);
          }
        }

        mergerout.flush();
        mergerout.close();
        zipInputStream.close();

        in.close();
        /**
         *将解压合并后的数据压缩成gzip格式
         */
        GZIPOutputStream gzipOutputStream = null;

        FSDataOutputStream outputStream = null;
        outputStream = hdfs.create(new Path(hdfsFile + File.separator +
            inputFile.getPath().getName()
                .substring(0, inputFile.getPath().getName().indexOf(".")) + ".gz"));
        FSDataInputStream inputStream = null;
        gzipOutputStream = new GZIPOutputStream(outputStream);
        inputStream = hdfs.open(new Path(
            hdfsFile + File.separator + inputFile.getPath().getName()
                .substring(0, inputFile.getPath().getName().indexOf("."))));
        int bygeSize = 2 * 1024 * 1024;
        byte[] buffer = new byte[bygeSize];
        int len;
        while ((len = inputStream.read(buffer)) > 0) {
          gzipOutputStream.write(buffer, 0, len);
        }
        inputStream.close();
        gzipOutputStream.finish();
        gzipOutputStream.flush();
        outputStream.close();

        gzipOutputStream.close();
        //删除zip文件解压合并后的临时文件
        String tempfiles = hdfsFile + File.separator + inputFile.getPath().getName()
            .substring(0, inputFile.getPath().getName().indexOf("."));
        if (hdfs.exists(new Path(tempfiles))) {
          hdfs.delete(new Path(tempfiles), true);
        }

      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}



