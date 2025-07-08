// src/main/java/CAPExperiment.java
import redis.clients.jedis.Jedis;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class CAPExperiment {
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    
    public static void main(String[] args) throws Exception {
        System.out.println("🚀 Redis CAP特性验证实验");
        System.out.println("=====================================");
        
        // 连接Master和Slave
        Jedis master = new Jedis("localhost", 6379);
        Jedis slave = new Jedis("localhost", 6380);
        
        // 测试连接
        System.out.println("📡 连接测试:");
        System.out.println("Master: " + master.ping());
        System.out.println("Slave:  " + slave.ping());
        
        // 实验1：观察复制延迟
        System.out.println("\n🔍 实验1：复制延迟观察");
        testReplicationDelay(master, slave);
        
        // 实验2：一致性vs可用性权衡
        System.out.println("\n⚖️  实验2：一致性vs可用性权衡");
        testConsistencyVsAvailability(master, slave);
        
        // 清理资源
        master.close();
        slave.close();
        
        System.out.println("\n✅ 实验完成！");
    }
    
    static void testReplicationDelay(Jedis master, Jedis slave) throws Exception {
        for (int i = 1; i <= 5; i++) {
            String key = "delay_test:" + i;
            String value = "value_" + LocalTime.now().format(TIME_FORMAT);
            
            // 写入Master
            String writeTime = LocalTime.now().format(TIME_FORMAT);
            master.set(key, value);
            System.out.printf("写入Master [%s]: %s = %s%n", writeTime, key, value);
            
            // 立即从Slave读取
            String immediateRead = slave.get(key);
            String readTime = LocalTime.now().format(TIME_FORMAT);
            System.out.printf("立即从Slave读取 [%s]: %s%n", readTime, 
                immediateRead != null ? immediateRead : "❌ null (未同步)");
            
            // 等待100ms后再读取
            TimeUnit.MILLISECONDS.sleep(100);
            String delayedRead = slave.get(key);
            String delayedTime = LocalTime.now().format(TIME_FORMAT);
            System.out.printf("100ms后读取 [%s]: %s%n", delayedTime, delayedRead);
            
            System.out.println("---");
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }
    
    static void testConsistencyVsAvailability(Jedis master, Jedis slave) throws Exception {
        System.out.println("💡 模拟电商场景：");
        
        // 场景1：商品库存更新（需要强一致性）
        System.out.println("\n📦 场景1：库存更新 (需要CP)");
        master.set("product:iphone15:stock", "100");
        TimeUnit.MILLISECONDS.sleep(50); // 模拟网络延迟
        
        String slaveStock = slave.get("product:iphone15:stock");
        if (slaveStock == null) {
            System.out.println("⚠️  Slave读取库存失败，如果是下单场景会有问题");
            System.out.println("💡 解决方案：强制从Master读取关键数据");
        } else {
            System.out.println("✅ Slave成功读取库存: " + slaveStock);
        }
        
        // 场景2：商品浏览（可以接受最终一致性）
        System.out.println("\n🛍️  场景2：商品描述更新 (可以AP)");
        master.set("product:iphone15:desc", "最新iPhone15，现货发售！");
        
        String slaveDesc = slave.get("product:iphone15:desc");
        System.out.println("从Slave读取商品描述: " + 
            (slaveDesc != null ? slaveDesc : "稍有延迟，用户可以接受"));
    }
}
