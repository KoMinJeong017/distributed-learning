
// ExtremeCAPTest.java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ExtremeCAPTest {
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    
    public static void main(String[] args) throws Exception {
        System.out.println("⚡ 极端条件Redis CAP验证实验");
        System.out.println("目标：制造足够的压力来观察CAP权衡");
        System.out.println("==========================================");
        
        Jedis master = new Jedis("localhost", 6379);
        Jedis slave = new Jedis("localhost", 6380);
        
        // 实验1：大数据量同步延迟测试
        System.out.println("📦 实验1：大数据量同步延迟测试");
        testLargeDataReplication(master, slave);
        
        // 实验2：高并发写入压力测试
        System.out.println("\n🚀 实验2：高并发写入压力测试");
        testHighConcurrencyWrites(master, slave);
        
        // 实验3：网络延迟模拟
        System.out.println("\n🌐 实验3：模拟网络延迟影响");
        testNetworkLatencyImpact(master, slave);
        
        // 实验4：故障切换测试
        System.out.println("\n💥 实验4：模拟故障切换");
        testFailoverScenario(master, slave);
        
        master.close();
        slave.close();
        System.out.println("\n✅ 极端测试完成！");
    }
    
    static void testLargeDataReplication(Jedis master, Jedis slave) throws Exception {
        System.out.println("写入大数据对象...");
        
        // 创建1MB的大数据
        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            largeData.append("这是一个很长的商品描述数据，用于测试大数据同步延迟，序号：").append(i).append("\n");
        }
        String bigValue = largeData.toString();
        System.out.printf("数据大小: %.2f MB%n", bigValue.getBytes().length / 1024.0 / 1024.0);
        
        // 写入大数据并立即读取
        long writeStart = System.currentTimeMillis();
        master.set("large_data:product_desc", bigValue);
        long writeEnd = System.currentTimeMillis();
        
        // 立即从Slave读取
        long readStart = System.currentTimeMillis();
        String slaveValue = slave.get("large_data:product_desc");
        long readEnd = System.currentTimeMillis();
        
        System.out.printf("Master写入耗时: %d ms%n", writeEnd - writeStart);
        System.out.printf("Slave读取耗时: %d ms%n", readEnd - readStart);
        
        if (slaveValue == null) {
            System.out.println("❌ 大数据同步延迟！Slave未能立即读取到数据");
            
            // 等待同步完成
            int attempts = 0;
            while (slaveValue == null && attempts < 10) {
                TimeUnit.MILLISECONDS.sleep(100);
                slaveValue = slave.get("large_data:product_desc");
                attempts++;
                System.out.printf("等待%d00ms后重试... %s%n", attempts, 
                    slaveValue != null ? "✅ 同步完成" : "❌ 仍未同步");
            }
        } else {
            boolean dataMatch = bigValue.equals(slaveValue);
            System.out.printf("✅ 立即读取成功，数据完整性: %s%n", 
                dataMatch ? "完整" : "❌ 不完整");
        }
    }
    
    static void testHighConcurrencyWrites(Jedis master, Jedis slave) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(100);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger readFailCount = new AtomicInteger(0);
        
        System.out.println("启动100个并发写入线程...");
        
        long testStart = System.currentTimeMillis();
        
        // 100个并发写入任务
        for (int i = 0; i < 100; i++) {
            final int taskId = i;
            executor.submit(() -> {
                try {
                    // 高频写入
                    String key = "concurrent:" + taskId;
                    String value = "data_" + System.nanoTime();
                    
                    master.set(key, value);
                    successCount.incrementAndGet();
                    
                    // 立即从Slave读取
                    String slaveRead = slave.get(key);
                    if (slaveRead == null || !slaveRead.equals(value)) {
                        readFailCount.incrementAndGet();
                        System.out.printf("⚠️  任务%d: 读取不一致 (写入:%s, 读取:%s)%n", 
                            taskId, value, slaveRead);
                    }
                    
                } catch (Exception e) {
                    System.out.printf("❌ 任务%d异常: %s%n", taskId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        long testEnd = System.currentTimeMillis();
        
        executor.shutdown();
        
        System.out.printf("并发测试结果:%n");
        System.out.printf("- 总耗时: %d ms%n", testEnd - testStart);
        System.out.printf("- 写入成功: %d/100%n", successCount.get());
        System.out.printf("- 读取不一致: %d/100 (%.1f%%)%n", 
            readFailCount.get(), readFailCount.get() * 100.0 / 100);
        
        if (readFailCount.get() > 0) {
            System.out.println("🎯 观察到CAP权衡！高并发下出现了一致性问题");
        } else {
            System.out.println("💪 Redis表现优秀！即使高并发也保持了很好的一致性");
        }
    }
    
    static void testNetworkLatencyImpact(Jedis master, Jedis slave) throws Exception {
        System.out.println("模拟网络延迟影响...");
        System.out.println("💡 提示：在生产环境中，跨区域部署会有明显延迟");
        
        // 模拟慢网络：通过大量小写入来占用网络带宽
        System.out.println("制造网络拥塞...");
        
        ExecutorService noiseExecutor = Executors.newFixedThreadPool(5);
        
        // 启动网络噪音生成器
        for (int i = 0; i < 5; i++) {
            noiseExecutor.submit(() -> {
                Jedis noiseMaster = new Jedis("localhost", 6379);
                for (int j = 0; j < 1000; j++) {
                    try {
                        noiseMaster.set("noise:" + Thread.currentThread().getId() + ":" + j, 
                            "noise_data_" + System.nanoTime());
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (Exception e) {
                        break;
                    }
                }
                noiseMaster.close();
            });
        }
        
        TimeUnit.MILLISECONDS.sleep(100); // 让网络拥塞建立
        
        // 在拥塞环境下测试
        System.out.println("在网络拥塞环境下测试...");
        AtomicInteger delayedReads = new AtomicInteger(0);
        
        for (int i = 0; i < 20; i++) {
            String key = "latency_test:" + i;
            String value = "test_data_" + System.currentTimeMillis();
            
            master.set(key, value);
            String slaveRead = slave.get(key);
            
            if (slaveRead == null) {
                delayedReads.incrementAndGet();
                System.out.printf("❌ 第%d次测试：延迟同步%n", i + 1);
            }
            
            TimeUnit.MILLISECONDS.sleep(50);
        }
        
        noiseExecutor.shutdownNow();
        
        System.out.printf("网络拥塞测试结果: %d/20 次出现同步延迟 (%.1f%%)%n", 
            delayedReads.get(), delayedReads.get() * 100.0 / 20);
    }
    
    static void testFailoverScenario(Jedis master, Jedis slave) throws Exception {
        System.out.println("故障切换场景测试");
        
        // 预写入测试数据
        master.set("failover:balance", "1000");
        master.set("failover:user", "alice");
        TimeUnit.MILLISECONDS.sleep(100);
        
        System.out.println("正常状态验证:");
        System.out.println("Master余额: " + master.get("failover:balance"));
        System.out.println("Slave余额:  " + slave.get("failover:balance"));
        
        System.out.println("\n💡 模拟Master故障场景:");
        System.out.println("假设Master突然不可用，但Slave仍然可以提供读服务");
        System.out.println("这体现了Redis的AP特性：");
        System.out.println("- ✅ 可用性：Slave继续提供服务");
        System.out.println("- ❌ 一致性：可能读到过时数据");
        System.out.println("- ✅ 分区容错：网络分区时系统仍可工作");
        
        // 模拟从Slave读取
        try {
            String balance = slave.get("failover:balance");
            String user = slave.get("failover:user");
            System.out.printf("\n📖 应急读取服务：用户%s的余额是%s%n", user, balance);
            System.out.println("⚠️  注意：这个余额可能不是最新的！");
            System.out.println("💡 这就是AP系统的权衡：优先保证可用性");
        } catch (Exception e) {
            System.out.println("❌ 连Slave也不可用：" + e.getMessage());
        }
    }
}
