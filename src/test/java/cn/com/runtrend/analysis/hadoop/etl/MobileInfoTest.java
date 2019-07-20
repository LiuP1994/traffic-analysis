package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.core.HttpGetParser;
import junit.framework.TestCase;

import java.util.List;

/**
 * 作者：张乔齐
 * cn.com.runtrend.analysis.hadoop.etl
 * 2017/9/12
 * 简介：<描述本类作用>
 */
public class MobileInfoTest extends TestCase {
    public void testMain() throws Exception {
        String url = "http://api.gamev2.mycheering.com/package/getPkg?data={\"signature\":\"2268064c183d386729ec4b84dcb2bb1f\",\"baseinfo\":{\"androidid\":\"44f92ce889e17626\",\"sdk\":22,\"packageName\":\"com.fengyou.games\",\"net\":1,\"runtime\":\"11\",\"imei\":\"861717035481230,861717032481233\""
            + ",\"appversion\":50002,\"local\":\"CN\",\"uid\":\"EZBMFZVMJhWkWj2hTwQETNAw4w9M4M9k2D9R4A130w8U4N4U1\",\"resolution\":\"720*1184\",\"language\":\"zh_CN\",\"channel\":\"5085\",\"imsi\":\"460003060447920\",\"model\":\"HUAWEI+TIT-AL00\"}}";
        List<String> list = HttpGetParser.paramList(url);
        List<String> list2 = HttpGetParser.paramList2(url);
        System.out.println(list2);
        System.out.println(MobileInfo.MobileInfoUtil.imei(list,list2));
        System.out.println(MobileInfo.MobileInfoUtil.phone(list,list2));
        System.out.println(MobileInfo.MobileInfoUtil.mac(list,list2));
        System.out.println(MobileInfo.MobileInfoUtil.imsi(list,list2));
    }

}