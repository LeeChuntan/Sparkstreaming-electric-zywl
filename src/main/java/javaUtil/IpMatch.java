package javaUtil;






import cn.hutool.core.io.resource.ClassPathResource;
import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;
import org.lionsoul.ip2region.Util;


import java.io.*;
import java.lang.reflect.Method;

/**
 * @program: maven_jar
 * @author: LiChuntao
 * @create: 2020-06-24 10:27
 */
public class IpMatch {

    public String getCityInfo(String ip){

        //db
//    不能用    String dbPath = IpMatch.class.getResource("/home/zywl/jar_spark/conf/ip2region.db").getPath();
//        String dbPath = "/home/zywl/jar_spark/conf/ip2region.db";
        String dbPath = IpMatch.class.getResource("/ip2region.db").getPath();

        File file = new File(dbPath);
        if ( file.exists() == false ) {
            System.out.println("Error: Invalid ip2region.db file");
        }

        //查询算法
        int algorithm = DbSearcher.BTREE_ALGORITHM; //B-tree
        //DbSearcher.BINARY_ALGORITHM //Binary
        //DbSearcher.MEMORY_ALGORITYM //Memory
        try {
            DbConfig config = new DbConfig();
            DbSearcher searcher = new DbSearcher(config, dbPath);

            //define the method
            Method method = null;
            switch ( algorithm )
            {
                case DbSearcher.BTREE_ALGORITHM:
                    method = searcher.getClass().getMethod("btreeSearch", String.class);
                    break;
                case DbSearcher.BINARY_ALGORITHM:
                    method = searcher.getClass().getMethod("binarySearch", String.class);
                    break;
                case DbSearcher.MEMORY_ALGORITYM:
                    method = searcher.getClass().getMethod("memorySearch", String.class);
                    break;
            }

            DataBlock dataBlock = null;
            if ( Util.isIpAddress(ip) == false ) {
                System.out.println("Error: Invalid ip address");
            }

            dataBlock  = (DataBlock) method.invoke(searcher, ip);

            return dataBlock.getRegion();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;


    }

    public static void main(String[] args)  throws Exception{
        IpMatch ip = new IpMatch();
        System.err.println(ip.getCityInfo("10.107.41.221"));
    }
}
