package test;

import javaUtil.Utc2Timestamp;
import org.apache.commons.net.util.SubnetUtils;
import sun.applet.Main;

/**
 * @program: sparkstreaming
 * @author: LiChuntao
 * @create: 2020-07-01 17:11
 */
public class test {
    public static void main(String[] args) {
        SubnetUtils.SubnetInfo subnet = new SubnetUtils("192.168.1.0/24").getInfo();
        boolean test = subnet.isInRange("192.168.2.2");
        System.out.println(test);
    }
}
