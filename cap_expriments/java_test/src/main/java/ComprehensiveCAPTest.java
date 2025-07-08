
// ComprehensiveCAPTest.java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ComprehensiveCAPTest {
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        System.out.println("🎯 Redis CAP理论综合验证实验");
        System.out.println("整合版：从基础到极端的全方位测试");
        System.out.println("==========================================");

        Jedis master = new Jedis("localhost", 6379);
        Jedis slave = new Jedis("localhost", 6380);

        System.out.println("📡 连接测试:");
        System.out.println("Master: " + master.ping());
        System.out.println("Slave:  " + slave.ping());

        // ============ 基础测试组 ============
        System.out.println("\n" + "=".repeat(50));
        System.out.println("📚 第一组：基础CAP特性验证");
        System.out.println("=".repeat(50));

        // 实验1：并发读写一致性测试（增强版）
        System.out.println("\n🔄 实验1：并发读写一致性测试");
        testConcurrentReadWrite(master, slave);

        // 实验2：电商秒杀场景模拟（增强版）
        System.out.println("\n⚡ 实验2：电商秒杀场景模拟");
        testSeckillScenario(master, slave);

        // 实验3：读写分离性能对比（增强版）
        System.out.println("\n📈 实验3：读写分离性能对比");
        testReadWriteSeparation(master, slave);

        // ============ 极端测试组 ============
        System.out.println("\n" + "=".repeat(50));
        System.out.println("⚡ 第二组：极端条件压力测试");
        System.out.println("=".repeat(50));

        // 实验4：大数据量同步延迟测试（极端版）
        System.out.println("\n📦 实验4：大数据量同步延迟测试");
        testLargeDataReplication(master, slave);

        // 实验5：高并发写入压力测试（极端版）
        System.out.println("\n🚀 实验5：高并发写入压力测试");
        testHighConcurrencyWrites(master, slave);

        // 实验6：网络延迟模拟（极端版）
        System.out.println("\n🌐 实验6：模拟网络延迟影响");
        testNetworkLatencyImpact(master, slave);

        // ============ 故障测试组 ============
        System.out.println("\n" + "=".repeat(50));
        System.out.println("💥 第三组：故障恢复能力测试");
        System.out.println("=".repeat(50));

        // 实验7：故障恢复测试（增强版）
        System.out.println("\n🛠️  实验7：故障恢复能力测试");
        testFailureRecovery(master, slave);

        // 实验8：故障切换场景（极端版）
        System.out.println("\n💥 实验8：故障切换场景测试");
        testFailoverScenario(master, slave);

        master.close();
        slave.close();

        // 总结报告
        System.out.println("\n" + "=".repeat(50));
        System.out.println("📊 CAP实验总结报告");
        System.out.println("=".repeat(50));
        printSummaryReport();
    }

    // ==================== 基础测试组方法 ====================

    static void testConcurrentReadWrite(Jedis master, Jedis slave) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        AtomicInteger writeCount = new AtomicInteger(0);
        AtomicInteger readFailCount = new AtomicInteger(0);

        // 模拟高并发场景：一个线程写，多个线程读
        Future<?> writer = executor.submit(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    String key = "concurrent:user:" + i;
                    String value = "user_data_" + System.currentTimeMillis();
                    master.set(key, value);
                    writeCount.incrementAndGet();
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
                        if (value == null) {
                            readFailCount.incrementAndGet();
                        }
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
        executor.awaitTermination(10, TimeUnit.SECONDS);

        System.out.printf("💡 并发测试结果: 写入%d次，读取失败%d次%n",
            writeCount.get(), readFailCount.get());
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
        AtomicInteger viewFailCount = new AtomicInteger(0);
        for (int i = 1; i <= 10; i++) {
            String stock = slave.get("seckill:iphone15:stock");
            if (stock == null) viewFailCount.incrementAndGet();
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
        System.out.printf("库存查看失败率: %.1f%% (%d/10)%n",
            viewFailCount.get() * 10.0, viewFailCount.get());

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

    // ==================== 极端测试组方法 ====================

    static void testLargeDataReplication(Jedis master, Jedis slave) throws Exception {
        System.out.println("写入大数据对象...");

        // 创建1MB的大数据
        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 50000; i++) { // 减少到50k避免内存问题
            largeData.append("商品描述数据测试大数据同步延迟序号：").append(i).append("\n");
        }
        String bigValue = largeData.toString();
        System.out.printf("数据大小: %.2f KB%n", bigValue.getBytes().length / 1024.0);

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
                        if (readFailCount.get() <= 5) { // 只显示前5个错误避免刷屏
                            System.out.printf("⚠️  任务%d: 读取不一致 (写入:%s, 读取:%s)%n",
                                taskId, value, slaveRead);
                        }
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

        ExecutorService noiseExecutor = Executors.newFixedThreadPool(3); // 减少线程数

        // 启动网络噪音生成器
        for (int i = 0; i < 3; i++) {
            final int threadId = i;
            noiseExecutor.submit(() -> {
                try (Jedis noiseMaster = new Jedis("localhost", 6379)) {
                    for (int j = 0; j < 500; j++) { // 减少噪音数量
                        try {
                            noiseMaster.set("noise:" + threadId + ":" + j,
                                "noise_data_" + System.nanoTime());
                            TimeUnit.MILLISECONDS.sleep(2);
                        } catch (Exception e) {
                            break;
                        }
                    }
                }
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

    // ==================== 故障测试组方法 ====================

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
        } catch (Exception e) {
            System.out.println("✅ 预期：Master不可用，写入失败: " + e.getClass().getSimpleName());
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

        // 等待一下让连接恢复
        TimeUnit.SECONDS.sleep(2);

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

    // ==================== 总结报告 ====================

    static void printSummaryReport() {
        System.out.println("🎯 CAP理论验证总结:");
        System.out.println();

        System.out.println("📚 我们验证了什么：");
        System.out.println("1. ✅ Redis的异步复制机制（主从延迟）");
        System.out.println("2. ✅ 高并发场景下的一致性挑战");
        System.out.println("3. ✅ 大数据同步的延迟影响");
        System.out.println("4. ✅ 网络条件对CAP权衡的影响");
        System.out.println("5. ✅ 故障情况下的可用性保证");

        System.out.println("\n💡 关键洞察：");
        System.out.println("- Redis是AP系统：优先保证可用性和分区容错");
        System.out.println("- 本地环境性能优秀，CAP权衡不明显");
        System.out.println("- 生产环境中网络延迟会放大CAP问题");
        System.out.println("- 不同业务场景需要不同的CAP策略");

        System.out.println("\n🚀 下一步学习方向：");
        System.out.println("1. 深入Redis集群和哨兵模式");
        System.out.println("2. 学习微服务架构设计原则");
        System.out.println("3. 了解其他存储系统的CAP选择");
        System.out.println("4. 开始电商项目的实际开发");

        System.out.println("\n✨ 恭喜！您已经深入理解了CAP理论！");
    }
}
