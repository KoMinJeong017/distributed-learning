// ExtremeCAPTest.java - 添加自动终止条件
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExtremeCAPTest {
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    
    // 自动终止条件配置
    private static final int MAX_CONSECUTIVE_FAILURES = 5;
    private static final int MAX_TOTAL_FAILURES = 20;
    private static final long MAX_EXECUTION_TIME_MS = 30000; // 30秒
    private static final AtomicBoolean GLOBAL_STOP_FLAG = new AtomicBoolean(false);
    
    public static void main(String[] args) throws Exception {
        System.out.println("⚡ 极端条件Redis CAP验证实验 (改进版)");
        System.out.println("新增：智能终止条件 + 资源保护机制");
        System.out.println("==========================================");
        
        long startTime = System.currentTimeMillis();
        
        try (Jedis master = new Jedis("localhost", 6379);
             Jedis slave = new Jedis("localhost", 6380)) {
            
            // 预检查连接
            if (!preflightCheck(master, slave)) {
                System.out.println("❌ 预检查失败，终止实验");
                return;
            }
            
            // 实验1：大数据量同步延迟测试
            System.out.println("📦 实验1：大数据量同步延迟测试");
            if (!testLargeDataReplication(master, slave)) {
                System.out.println("⚠️  大数据测试失败，跳过后续高强度测试");
                return;
            }
            
            // 实验2：高并发写入压力测试（改进版）
            System.out.println("\n🚀 实验2：智能高并发写入测试");
            testHighConcurrencyWritesWithTermination(master, slave);
            
            // 实验3：网络延迟模拟（改进版）
            if (!GLOBAL_STOP_FLAG.get()) {
                System.out.println("\n🌐 实验3：智能网络延迟影响测试");
                testNetworkLatencyWithTermination(master, slave);
            }
            
            System.out.println("\n✅ 极端测试完成（改进版）！");
            
        } catch (Exception e) {
            System.out.println("❌ 实验异常终止: " + e.getMessage());
        } finally {
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.printf("📊 总执行时间: %.2f 秒%n", totalTime / 1000.0);
        }
    }
    
    /**
     * 预检查：验证基础连接和环境
     */
    static boolean preflightCheck(Jedis master, Jedis slave) {
        try {
            System.out.println("🔍 执行预检查...");
            
            // 检查基础连接
            String masterPing = master.ping();
            String slavePing = slave.ping();
            
            if (!"PONG".equals(masterPing) || !"PONG".equals(slavePing)) {
                System.out.println("❌ 基础连接失败");
                return false;
            }
            
            // 检查基础同步
            String testKey = "preflight_test";
            String testValue = "test_" + System.currentTimeMillis();
            master.set(testKey, testValue);
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("❌ 预检查被中断");
                return false;
            }
            
            String slaveValue = slave.get(testKey);
            if (!testValue.equals(slaveValue)) {
                System.out.println("❌ 基础主从同步失败");
                return false;
            }
            
            // 清理测试数据
            master.del(testKey);
            
            System.out.println("✅ 预检查通过");
            return true;
            
        } catch (Exception e) {
            System.out.println("❌ 预检查异常: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 大数据测试 - 添加失败检测
     */
    static boolean testLargeDataReplication(Jedis master, Jedis slave) {
        try {
            System.out.println("写入大数据对象...");
            
            // 创建适中大小的数据（避免内存问题）
            StringBuilder largeData = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                largeData.append("商品描述数据测试序号：").append(i).append("\n");
            }
            String bigValue = largeData.toString();
            System.out.printf("数据大小: %.2f KB%n", bigValue.getBytes().length / 1024.0);
            
            // 写入测试
            long writeStart = System.currentTimeMillis();
            master.set("large_data:test", bigValue);
            long writeEnd = System.currentTimeMillis();
            
            // 读取测试
            long readStart = System.currentTimeMillis();
            String slaveValue = slave.get("large_data:test");
            long readEnd = System.currentTimeMillis();
            
            System.out.printf("Master写入耗时: %d ms%n", writeEnd - writeStart);
            System.out.printf("Slave读取耗时: %d ms%n", readEnd - readStart);
            
            boolean success = bigValue.equals(slaveValue);
            System.out.printf("✅ 大数据同步: %s%n", success ? "成功" : "失败");
            
            // 清理数据
            master.del("large_data:test");
            
            return success;
            
        } catch (Exception e) {
            System.out.println("❌ 大数据测试失败: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 改进的高并发测试 - 添加智能终止条件
     */
    static void testHighConcurrencyWritesWithTermination(Jedis master, Jedis slave) {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(50); // 降低并发数
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger consecutiveFailures = new AtomicInteger(0);
        
        System.out.println("启动50个并发写入线程（智能终止版）...");
        
        long testStart = System.currentTimeMillis();
        
        for (int i = 0; i < 50; i++) {
            final int taskId = i;
            executor.submit(() -> {
                try {
                    // 检查全局停止标志
                    if (GLOBAL_STOP_FLAG.get() || 
                        consecutiveFailures.get() >= MAX_CONSECUTIVE_FAILURES ||
                        failureCount.get() >= MAX_TOTAL_FAILURES) {
                        return;
                    }
                    
                    // 创建独立连接避免线程安全问题
                    try (Jedis taskMaster = new Jedis("localhost", 6379);
                         Jedis taskSlave = new Jedis("localhost", 6380)) {
                        
                        String key = "concurrent_v2:" + taskId;
                        String value = "data_" + System.nanoTime();
                        
                        // 写入测试
                        taskMaster.set(key, value);
                        successCount.incrementAndGet();
                        consecutiveFailures.set(0); // 重置连续失败计数
                        
                        // 读取验证
                        try {
                            TimeUnit.MILLISECONDS.sleep(10); // 小延迟
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        String slaveRead = taskSlave.get(key);
                        
                        if (slaveRead == null || !slaveRead.equals(value)) {
                            System.out.printf("⚠️  任务%d: 读取不一致%n", taskId);
                        }
                        
                    }
                    
                } catch (Exception e) {
                    int failures = failureCount.incrementAndGet();
                    int consecutive = consecutiveFailures.incrementAndGet();
                    
                    if (failures <= 3) { // 只显示前3个错误
                        System.out.printf("❌ 任务%d失败: %s%n", taskId, 
                            e.getClass().getSimpleName());
                    }
                    
                    // 检查终止条件
                    if (consecutive >= MAX_CONSECUTIVE_FAILURES) {
                        System.out.printf("🛑 连续失败%d次，触发自动终止%n", consecutive);
                        GLOBAL_STOP_FLAG.set(true);
                    }
                    
                    if (failures >= MAX_TOTAL_FAILURES) {
                        System.out.printf("🛑 总失败%d次，触发自动终止%n", failures);
                        GLOBAL_STOP_FLAG.set(true);
                    }
                    
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            // 等待完成或超时
            boolean finished = latch.await(MAX_EXECUTION_TIME_MS, TimeUnit.MILLISECONDS);
            if (!finished) {
                System.out.println("🛑 执行超时，自动终止");
                GLOBAL_STOP_FLAG.set(true);
            }
        } catch (InterruptedException e) {
            System.out.println("🛑 执行被中断");
            GLOBAL_STOP_FLAG.set(true);
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
        
        executor.shutdown();
        long testEnd = System.currentTimeMillis();
        
        // 结果统计
        System.out.printf("📊 并发测试结果:%n");
        System.out.printf("- 总耗时: %d ms%n", testEnd - testStart);
        System.out.printf("- 成功: %d, 失败: %d%n", successCount.get(), failureCount.get());
        System.out.printf("- 成功率: %.1f%%%n", 
            successCount.get() * 100.0 / (successCount.get() + failureCount.get()));
        
        if (failureCount.get() > 0) {
            System.out.println("🎯 观察到CAP权衡！系统在压力下选择了保护策略");
        }
    }
    
    /**
     * 改进的网络延迟测试 - 添加智能终止
     */
    static void testNetworkLatencyWithTermination(Jedis master, Jedis slave) {
        if (GLOBAL_STOP_FLAG.get()) {
            System.out.println("⚠️  全局停止标志已设置，跳过网络延迟测试");
            return;
        }
        
        System.out.println("模拟网络延迟影响（智能版）...");
        
        ExecutorService noiseExecutor = Executors.newFixedThreadPool(3);
        AtomicInteger noiseFailures = new AtomicInteger(0);
        AtomicBoolean noiseStopFlag = new AtomicBoolean(false);
        
        // 启动受控的网络噪音生成器
        for (int i = 0; i < 3; i++) {
            final int threadId = i;
            noiseExecutor.submit(() -> {
                try (Jedis noiseMaster = new Jedis("localhost", 6379)) {
                    
                    for (int j = 0; j < 100 && !noiseStopFlag.get() && !GLOBAL_STOP_FLAG.get(); j++) {
                        try {
                            noiseMaster.set("noise:" + threadId + ":" + j, 
                                "noise_" + System.nanoTime());
                            TimeUnit.MILLISECONDS.sleep(5);
                            
                        } catch (Exception e) {
                            int failures = noiseFailures.incrementAndGet();
                            
                            if (failures >= 5) {
                                System.out.printf("🛑 噪音生成器%d失败过多，自动停止%n", threadId);
                                noiseStopFlag.set(true);
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.printf("❌ 噪音生成器%d异常: %s%n", threadId, 
                        e.getClass().getSimpleName());
                }
            });
        }
        
        try {
            TimeUnit.MILLISECONDS.sleep(200); // 让噪音建立
            
            // 在拥塞环境下测试
            System.out.println("在网络拥塞环境下测试...");
            AtomicInteger delayedReads = new AtomicInteger(0);
            AtomicInteger testFailures = new AtomicInteger(0);
            
            for (int i = 0; i < 10 && !GLOBAL_STOP_FLAG.get(); i++) {
                try {
                    String key = "latency_test:" + i;
                    String value = "test_data_" + System.currentTimeMillis();
                    
                    master.set(key, value);
                    String slaveRead = slave.get(key);
                    
                    if (slaveRead == null) {
                        delayedReads.incrementAndGet();
                        System.out.printf("❌ 第%d次测试：延迟同步%n", i + 1);
                    }
                    
                    TimeUnit.MILLISECONDS.sleep(20);
                    
                } catch (Exception e) {
                    int failures = testFailures.incrementAndGet();
                    if (failures >= 3) {
                        System.out.println("🛑 网络测试失败过多，自动终止");
                        break;
                    }
                }
            }
            
            System.out.printf("📊 网络拥塞测试结果: %d/10 次延迟 (%.1f%%)%n", 
                delayedReads.get(), delayedReads.get() * 10.0);
                
        } catch (InterruptedException e) {
            System.out.println("🛑 网络延迟测试被中断");
            Thread.currentThread().interrupt(); // 恢复中断状态
        } finally {
            noiseStopFlag.set(true);
            noiseExecutor.shutdown();
        }
    }
}
