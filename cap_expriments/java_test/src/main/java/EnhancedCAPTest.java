// EnhancedCAPTest.java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EnhancedCAPTest {
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    
    public static void main(String[] args) throws Exception {
        System.out.println("🎯 增强版Redis CAP特性深度验证");
        System.out.println("=========================================");
        
        Jedis master = new Jedis("localhost", 6379);
        Jedis slave = new Jedis("localhost", 6380);
        
        // 实验1：并发读写一致性测试
        System.out.println("🔄 实验1：并发读写一致性测试");
        testConcurrentReadWrite(master, slave);
        
        // 实验2：故障恢复测试
        System.out.println("\n🛠️  实验2：故障恢复能力测试");
        testFailureRecovery(master, slave);
        
        // 实验3：电商秒杀场景模拟
        System.out.println("\n⚡ 实验3：电商秒杀场景模拟");
        testSeckillScenario(master, slave);
        
        // 实验4：读写分离性能对比
        System.out.println("\n📈 实验4：读写分离性能对比");
        testReadWriteSeparation(master, slave);
        
        master.close();
        slave.close();
        System.out.println("\n✅ 增强版实验完成！");
    }
    
    static void testConcurrentReadWrite(Jedis master, Jedis slave) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        
        // 模拟高并发场景：一个线程写，多个线程读
        Future<?> writer = executor.submit(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    String key = "concurrent:user:" + i;
                    String value = "user_data_" + System.currentTimeMillis();
                    master.set(key, value);
                    System.out.printf("📝 [%s] 写入: %s = %s%n", 
                        LocalTime.now().format(TIME_FORMAT), key, value);
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        
        // 多个读取线程
        for (int t = 0; t < 3; t++) {
            final int threadId = t;
            executor.submit(() -> {
                for (int i = 0; i < 10; i++) {
                    try {
                        String key = "concurrent:user:" + i;
                        String value = slave.get(key);
                        System.out.printf("👁️  [%s] 线程%d读取: %s = %s%n", 
                            LocalTime.now().format(TIME_FORMAT), threadId, key, 
                            value != null ? value : "❌ 未同步");
                        TimeUnit.MILLISECONDS.sleep(150);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        
        writer.get(); // 等待写入完成
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        
        System.out.println("💡 观察：多线程读取时的一致性表现");
    }
    
    static void testFailureRecovery(Jedis master, Jedis slave) throws Exception {
        System.out.println("准备测试故障恢复...");
        
        // 先写入一些数据
        master.set("recovery:test", "before_failure");
        TimeUnit.MILLISECONDS.sleep(100);
        
        System.out.println("正常状态 - Slave读取: " + slave.get("recovery:test"));
        
        System.out.println("\n💥 模拟Master临时不可用（请在另一个终端执行）:");
        System.out.println("docker pause redis-master");
        System.out.println("按回车键继续测试...");
        System.in.read();
        
        // 测试Master不可用时的情况
        try {
            master.set("recovery:during_failure", "this_should_fail");
            System.out.println("❌ 意外：Master写入成功了");
        } catch (JedisConnectionException e) {
            System.out.println("✅ 预期：Master不可用，写入失败");
        }
        
        // 测试Slave是否还能提供读服务
        try {
            String value = slave.get("recovery:test");
            System.out.println("✅ AP特性体现：Slave仍可读取: " + value);
        } catch (Exception e) {
            System.out.println("❌ Slave也不可用: " + e.getMessage());
        }
        
        System.out.println("\n🔧 请恢复Master（执行）: docker unpause redis-master");
        System.out.println("按回车键继续...");
        System.in.read();
        
        // 测试恢复后的情况
        try {
            master.set("recovery:after_recovery", "master_back");
            System.out.println("✅ Master恢复正常，可以写入");
            
            TimeUnit.MILLISECONDS.sleep(200);
            String value = slave.get("recovery:after_recovery");
            System.out.println("✅ 数据同步恢复: " + value);
        } catch (Exception e) {
            System.out.println("❌ 恢复异常: " + e.getMessage());
        }
    }
    
    static void testSeckillScenario(Jedis master, Jedis slave) throws Exception {
        System.out.println("🛒 模拟iPhone15秒杀场景");
        
        // 设置初始库存
        int initialStock = 100;
        master.set("seckill:iphone15:stock", String.valueOf(initialStock));
        master.set("seckill:iphone15:sold", "0");
        
        TimeUnit.MILLISECONDS.sleep(100); // 等待同步
        
        System.out.println("📦 初始库存: " + master.get("seckill:iphone15:stock"));
        
        // 模拟多用户同时查看库存
        System.out.println("\n👥 10个用户同时查看库存:");
        for (int i = 1; i <= 10; i++) {
            String stock = slave.get("seckill:iphone15:stock");
            System.out.printf("用户%d看到库存: %s%n", i, 
                stock != null ? stock : "❌ 查看失败");
        }
        
        // 模拟库存扣减
        System.out.println("\n💸 模拟用户下单扣减库存:");
        for (int i = 1; i <= 5; i++) {
            try {
                // 模拟下单流程：先查库存，再扣减
                String currentStock = master.get("seckill:iphone15:stock");
                int stock = Integer.parseInt(currentStock);
                
                if (stock > 0) {
                    master.decr("seckill:iphone15:stock");
                    master.incr("seckill:iphone15:sold");
                    System.out.printf("✅ 用户%d下单成功，剩余库存: %s%n", 
                        i, master.get("seckill:iphone15:stock"));
                } else {
                    System.out.printf("❌ 用户%d下单失败：库存不足%n", i);
                }
                
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (Exception e) {
                System.out.printf("❌ 用户%d下单异常: %s%n", i, e.getMessage());
            }
        }
        
        // 检查最终一致性
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("\n📊 最终状态对比:");
        System.out.println("Master库存: " + master.get("seckill:iphone15:stock"));
        System.out.println("Slave库存:  " + slave.get("seckill:iphone15:stock"));
        System.out.println("已售出:    " + master.get("seckill:iphone15:sold"));
        
        System.out.println("\n💡 秒杀场景的CAP权衡:");
        System.out.println("- 库存扣减：必须从Master操作(CP)");
        System.out.println("- 库存查看：可以从Slave查看(AP)");
        System.out.println("- 风险：Slave延迟可能导致超卖提示");
    }
    
    static void testReadWriteSeparation(Jedis master, Jedis slave) throws Exception {
        System.out.println("⚡ 读写分离性能测试");
        
        // 写性能测试
        long writeStart = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            master.set("perf:write:" + i, "data_" + i);
        }
        long writeTime = System.currentTimeMillis() - writeStart;
        System.out.printf("📝 Master写入1000条记录耗时: %d ms%n", writeTime);
        
        // 等待同步
        TimeUnit.MILLISECONDS.sleep(500);
        
        // 读性能测试
        long readStart = System.currentTimeMillis();
        int successCount = 0;
        for (int i = 0; i < 1000; i++) {
            String value = slave.get("perf:write:" + i);
            if (value != null) successCount++;
        }
        long readTime = System.currentTimeMillis() - readStart;
        System.out.printf("👁️  Slave读取1000条记录耗时: %d ms%n", readTime);
        System.out.printf("📈 读取成功率: %.1f%% (%d/1000)%n", 
            (successCount * 100.0 / 1000), successCount);
        
        System.out.println("\n💡 性能洞察:");
        System.out.printf("- 读写分离可以提升整体性能%n");
        System.out.printf("- 写操作延迟: %d ms%n", writeTime);
        System.out.printf("- 读操作延迟: %d ms%n", readTime);
        if (successCount < 1000) {
            System.out.printf("- 一致性代价: %.1f%% 的读取可能不一致%n", 
                ((1000 - successCount) * 100.0 / 1000));
        }
    }
}
