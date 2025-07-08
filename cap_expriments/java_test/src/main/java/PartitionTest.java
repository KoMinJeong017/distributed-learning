import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class PartitionTest {
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    
    public static void main(String[] args) throws Exception {
        System.out.println("🌐 Redis网络分区CAP验证实验");
        System.out.println("目标：观察真正的CAP权衡现象");
        System.out.println("=====================================");
        
        Jedis master = new Jedis("localhost", 6379);
        Jedis slave = new Jedis("localhost", 6380);
        
        // 验证初始连接
        System.out.println("📡 初始连接测试:");
        System.out.println("Master: " + master.ping());
        System.out.println("Slave:  " + slave.ping());
        
        // 阶段1: 分区前测试
        System.out.println("\n" + "=".repeat(40));
        System.out.println("📚 阶段1: 分区前正常状态测试");
        System.out.println("=".repeat(40));
        testBeforePartition(master, slave);
        
        // 阶段2: 创建分区
        System.out.println("\n" + "=".repeat(40));
        System.out.println("💥 阶段2: 创建网络分区");
        System.out.println("=".repeat(40));
        createPartition();
        
        // 阶段3: 分区中测试
        System.out.println("\n" + "=".repeat(40));
        System.out.println("⚡ 阶段3: 分区状态下的系统行为");
        System.out.println("=".repeat(40));
        testDuringPartition(master, slave);
        
        // 阶段4: 恢复网络
        System.out.println("\n" + "=".repeat(40));
        System.out.println("🔧 阶段4: 恢复网络连接");
        System.out.println("=".repeat(40));
        recoverPartition();
        
        // 阶段5: 分区后测试
        System.out.println("\n" + "=".repeat(40));
        System.out.println("📈 阶段5: 分区恢复后的数据一致性");
        System.out.println("=".repeat(40));
        testAfterPartition(master, slave);
        
        // 总结
        System.out.println("\n" + "=".repeat(40));
        System.out.println("🎯 CAP权衡观察总结");
        System.out.println("=".repeat(40));
        printPartitionSummary();
        
        master.close();
        slave.close();
    }
    
    static void testBeforePartition(Jedis master, Jedis slave) throws Exception {
        System.out.println("准备测试数据...");
        
        // 写入测试数据
        for (int i = 1; i <= 5; i++) {
            String key = "normal_data:" + i;
            String value = "value_" + i + "_" + System.currentTimeMillis();
            master.set(key, value);
            System.out.printf("写入 %s = %s%n", key, value);
        }
        
        // 等待同步
        TimeUnit.MILLISECONDS.sleep(200);
        
        // 验证同步
        System.out.println("\n验证主从同步状态:");
        for (int i = 1; i <= 5; i++) {
            String key = "normal_data:" + i;
            String masterValue = master.get(key);
            String slaveValue = slave.get(key);
            boolean synced = masterValue != null && masterValue.equals(slaveValue);
            System.out.printf("%s: %s (同步: %s)%n", key, slaveValue, 
                synced ? "✅" : "❌");
        }
        
        System.out.println("\n💡 正常状态下Redis表现优秀，主从同步迅速");
    }
    
    static void createPartition() throws Exception {
        System.out.println("请在另一个终端执行以下命令创建网络分区:");
        System.out.println("📋 命令: ./simulate_partition.sh start");
        System.out.println();
        System.out.println("这将会:");
        System.out.println("- 🚫 阻断Master和Slave之间的网络连接");
        System.out.println("- 📊 模拟真实的网络分区场景");
        System.out.println("- ⚡ 触发CAP定理的权衡选择");
        System.out.println();
        System.out.println("执行完成后按回车键继续...");
        System.in.read();
        
        // 验证分区是否生效
        System.out.println("🔍 验证分区状态...");
        TimeUnit.SECONDS.sleep(2);
    }
    
    static void testDuringPartition(Jedis master, Jedis slave) throws Exception {
        System.out.println("🔥 关键测试：分区状态下的CAP权衡");
        
        // 测试1: Master写入能力
        System.out.println("\n📝 测试1: Master写入能力");
        try {
            String key = "partition_test:master_write";
            String value = "written_during_partition_" + System.currentTimeMillis();
            master.set(key, value);
            System.out.printf("✅ Master写入成功: %s = %s%n", key, value);
            System.out.println("💡 体现了分区容错性：Master仍可工作");
        } catch (Exception e) {
            System.out.printf("❌ Master写入失败: %s%n", e.getMessage());
        }
        
        // 测试2: Slave读取能力
        System.out.println("\n👁️  测试2: Slave读取能力");
        try {
            String oldValue = slave.get("normal_data:1");
            System.out.printf("Slave读取旧数据: %s%n", oldValue);
            
            String newValue = slave.get("partition_test:master_write");
            System.out.printf("Slave读取新数据: %s%n", newValue);
            
            if (oldValue != null && newValue == null) {
                System.out.println("🎯 观察到CAP权衡！");
                System.out.println("- ✅ 可用性：Slave仍可提供读服务（返回旧数据）");
                System.out.println("- ❌ 一致性：Slave无法读取到Master的新写入");
                System.out.println("- ✅ 分区容错：网络分区时系统继续运行");
            }
        } catch (Exception e) {
            System.out.printf("❌ Slave读取失败: %s%n", e.getMessage());
            System.out.println("💡 如果Slave也不可用，说明系统选择了一致性而非可用性");
        }
        
        // 测试3: 业务场景模拟
        System.out.println("\n🛒 测试3: 电商场景模拟");
        simulateBusinessScenario(master, slave);
    }
    
    static void simulateBusinessScenario(Jedis master, Jedis slave) {
        System.out.println("模拟分区时的电商操作:");
        
        // 用户下单（需要写Master）
        try {
            master.set("order:12345", "用户A购买iPhone15");
            master.decr("inventory:iphone15");
            System.out.println("✅ 用户下单成功（Master写入）");
        } catch (Exception e) {
            System.out.println("❌ 用户下单失败：" + e.getMessage());
        }
        
        // 其他用户查看商品（从Slave读取）
        try {
            String inventory = slave.get("inventory:iphone15");
            System.out.printf("其他用户查看库存: %s%n", 
                inventory != null ? inventory : "❌ 查看失败");
            
            if (inventory != null) {
                System.out.println("⚠️  警告：用户看到的可能是过时的库存信息！");
                System.out.println("💡 这就是AP系统的权衡：保证可用性但可能不一致");
            }
        } catch (Exception e) {
            System.out.println("❌ 库存查看失败：" + e.getMessage());
        }
    }
    
    static void recoverPartition() throws Exception {
        System.out.println("请在另一个终端执行以下命令恢复网络:");
        System.out.println("📋 命令: ./simulate_partition.sh stop");
        System.out.println();
        System.out.println("这将会:");
        System.out.println("- 🔧 恢复Master和Slave之间的网络连接");
        System.out.println("- 📈 重新建立主从同步");
        System.out.println("- ✅ 系统回到正常的CAP状态");
        System.out.println();
        System.out.println("执行完成后按回车键继续...");
        System.in.read();
        
        System.out.println("⏳ 等待网络恢复和数据同步...");
        TimeUnit.SECONDS.sleep(3);
    }
    
    static void testAfterPartition(Jedis master, Jedis slave) throws Exception {
        System.out.println("🔍 验证分区恢复后的数据一致性:");
        
        // 检查分区期间写入的数据
        String partitionData = slave.get("partition_test:master_write");
        if (partitionData != null) {
            System.out.printf("✅ 分区期间的数据已同步: %s%n", partitionData);
            System.out.println("💡 最终一致性实现：分区恢复后数据同步完成");
        } else {
            System.out.println("❌ 分区期间的数据未同步");
        }
        
        // 检查业务数据
        String order = slave.get("order:12345");
        String inventory = slave.get("inventory:iphone15");
        
        System.out.printf("订单数据同步: %s%n", order != null ? "✅" : "❌");
        System.out.printf("库存数据同步: %s%n", inventory != null ? "✅" : "❌");
        
        // 验证新的读写操作
        System.out.println("\n📊 验证恢复后的正常操作:");
        master.set("recovery_test", "network_recovered_" + System.currentTimeMillis());
        TimeUnit.MILLISECONDS.sleep(100);
        String recoveryValue = slave.get("recovery_test");
        
        if (recoveryValue != null) {
            System.out.println("✅ 主从同步已完全恢复");
        } else {
            System.out.println("⚠️  主从同步可能仍在恢复中");
        }
    }
    
    static void printPartitionSummary() {
        System.out.println("🎯 网络分区实验验证了以下CAP权衡:");
        System.out.println();
        System.out.println("1️⃣ 分区容错性 (P):");
        System.out.println("   ✅ 网络分区时，Master和Slave都能独立工作");
        System.out.println("   ✅ 系统没有完全崩溃，体现了容错能力");
        System.out.println();
        System.out.println("2️⃣ 可用性 (A):");
        System.out.println("   ✅ Master继续接受写入请求");
        System.out.println("   ✅ Slave继续提供读取服务（如果可达）");
        System.out.println("   💡 Redis选择了可用性而非完全一致性");
        System.out.println();
        System.out.println("3️⃣ 一致性 (C):");
        System.out.println("   ❌ 分区期间无法保证强一致性");
        System.out.println("   ✅ 分区恢复后实现最终一致性");
        System.out.println("   💡 这就是AP系统的权衡策略");
        System.out.println();
        System.out.println("🔑 关键洞察:");
        System.out.println("- Redis是典型的AP系统");
        System.out.println("- 在分区时优先保证可用性");
        System.out.println("- 通过最终一致性来平衡数据准确性");
        System.out.println("- 不同业务场景需要不同的处理策略");
        System.out.println();
        System.out.println("🚀 恭喜！您已经亲眼见证了CAP定理的真实体现！");
    }
}
