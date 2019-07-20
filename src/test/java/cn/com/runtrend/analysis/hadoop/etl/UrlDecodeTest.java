package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.core.UrlDecode;
import junit.framework.TestCase;

import java.net.URLDecoder;

/**
 * 作者：张乔齐
 * cn.com.runtrend.analysis.hadoop.etl
 * 2017/9/12
 * 简介：<描述本类作用>
 */
public class UrlDecodeTest extends TestCase {
    public void testMain() throws Exception {
        System.out.println(UrlDecode.decode("%e4%bd%a0%e5%a5%bd%e4%b8%96%e7%95%8c%3","utf-8"));
        System.out.println("%u8a57".replaceAll("%u","\\\\u"));
//        System.out.println(URLDecoder.decode("%u4f60%u597d","utf-8"));
//        System.out.println(UrlDecode.decode("%u4f60%u597d","utf-8"));
    }

}