#!/bin/bash

echo "🔧 Redis网络分区模拟工具"

case "$1" in
  "start")
    echo "💥 开始网络分区：阻断Master和Slave之间的连接"
    echo "方法1: 使用Docker网络隔离"
    
    # 获取容器网络信息
    MASTER_IP=$(docker inspect redis-master | grep '"IPAddress"' | tail -1 | cut -d'"' -f4)
    SLAVE_IP=$(docker inspect redis-slave | grep '"IPAddress"' | tail -1 | cut -d'"' -f4)
    
    echo "Master IP: $MASTER_IP"
    echo "Slave IP: $SLAVE_IP"
    
    # 方法1: 暂停Slave容器（模拟网络分区）
    echo "🚫 暂停Slave容器以模拟分区..."
    docker pause redis-slave
    
    # 方法2: 使用iptables阻断连接（需要root权限）
    # sudo iptables -A INPUT -s $MASTER_IP -j DROP
    # sudo iptables -A OUTPUT -d $SLAVE_IP -j DROP
    
    echo "✅ 网络分区已生效"
    echo "📊 验证分区状态："
    docker exec redis-master redis-cli ping || echo "❌ Master不可达"
    docker exec redis-slave redis-cli ping 2>/dev/null || echo "✅ Slave已分区"
    ;;
    
  "stop")
    echo "🔧 恢复网络连接"
    
    # 恢复Slave容器
    echo "▶️  恢复Slave容器..."
    docker unpause redis-slave
    
    # 恢复iptables（如果使用了方法2）
    # sudo iptables -D INPUT -s $MASTER_IP -j DROP 2>/dev/null
    # sudo iptables -D OUTPUT -d $SLAVE_IP -j DROP 2>/dev/null
    
    echo "✅ 网络连接已恢复"
    echo "📊 验证恢复状态："
    docker exec redis-master redis-cli ping
    docker exec redis-slave redis-cli ping
    
    # 等待主从同步恢复
    echo "⏳ 等待主从同步恢复..."
    sleep 3
    ;;
    
  "status")
    echo "📊 当前网络状态："
    echo "Master状态:"
    docker exec redis-master redis-cli ping 2>/dev/null || echo "❌ Master不可达"
    echo "Slave状态:"
    docker exec redis-slave redis-cli ping 2>/dev/null || echo "❌ Slave不可达"
    
    echo "主从复制状态:"
    docker exec redis-master redis-cli info replication | grep connected_slaves
    ;;
    
  *)
    echo "用法: $0 {start|stop|status}"
    echo "  start  - 开始网络分区"
    echo "  stop   - 恢复网络连接"
    echo "  status - 检查当前状态"
    ;;
esac
