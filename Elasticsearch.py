import requests
import time
import pandas as pd

# 定义配置文件
configurations = {
    "ES_Instance1": {
        "addresses": ["192.168.1.10", "192.168.1.11"],
        "port": 9200,
        "initial_node_count": 3,
        "initial_jvm_memory_ratio": 80,
        "initial_index_count": 10
    },
    "ES_Instance2": {
        "addresses": ["192.168.1.12", "192.168.1.13"],
        "port": 9200,
        "initial_node_count": 5,
        "initial_jvm_memory_ratio": 70,
        "initial_index_count": 20
    }
}

# 创建结果记录列表
results = []

# 遍历每个ES实例进行巡检
for instance_name, instance_info in configurations.items():
    addresses = instance_info["addresses"]
    port = instance_info["port"]
    initial_node_count = instance_info["initial_node_count"]
    initial_jvm_memory_ratio = instance_info["initial_jvm_memory_ratio"]
    initial_index_count = instance_info["initial_index_count"]

    # 初始化巡检结果字典
    inspection_result = {
        "ES实例": instance_name,
        "时间戳": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "集群健康状态": "",
        "节点数量": "",
        "JVM内存使用": "",
        "副本数量": "",
        "分片数量": ""
    }

    try:
        # 发送HTTP请求获取集群健康状态
        response = requests.get(f"http://{addresses[0]}:{port}/_cluster/health")
        cluster_health = response.json()["status"]
        inspection_result["集群健康状态"] = cluster_health

        # 发送HTTP请求获取节点数量
        response = requests.get(f"http://{addresses[0]}:{port}/_nodes/stats")
        node_count = len(response.json()["nodes"])
        if node_count == initial_node_count:
            inspection_result["节点数量"] = "正常"
        else:
            inspection_result["节点数量"] = f"异常 (离线节点: {initial_node_count - node_count})"

        # 发送HTTP请求获取JVM内存使用比例
        response = requests.get(f"http://{addresses[0]}:{port}/_nodes/stats/jvm")
        jvm_memory_ratio = response.json()["nodes"][0]["jvm"]["mem"]["heap_used_percent"]
        if jvm_memory_ratio <= initial_jvm_memory_ratio:
            inspection_result["JVM内存使用"] = "正常"
        else:
            inspection_result["JVM内存使用"] = f"异常 (使用比例: {jvm_memory_ratio}%)"

        # 发送HTTP请求获取副本数量
        response = requests.get(f"http://{addresses[0]}:{port}/_cat/indices?v")
        replica_count = 0
        replica_indices = []
        for line in response.text.splitlines():
            index_name = line.split()[2]
            replica = int(line.split()[4])
            replica_count += replica
            if replica == 0:
                replica_indices.append(index_name)
        if replica_count == 0:
            inspection_result["副本数量"] = "异常"
        elif replica_count < 3:
            inspection_result["副本数量"] = f"{replica_count} (索引: {', '.join(replica_indices)})"
        else:
            inspection_result["副本数量"] = f"{replica_count}"

        # 发送HTTP请求获取分片数量
        response = requests.get(f"http://{addresses[0]}:{port}/_cat/indices?v")
        shard_count = 0
        shard_per_node = {}
        for line in response.text.splitlines():
            index_name = line.split()[2]
            shard = int(line.split()[3])
            shard_count += shard
            if shard_per_node.get(shard) is None:
                shard_per_node[shard] = [index_name]
            else:
                shard_per_node[shard].append(index_name)

        if shard_count < initial_index_count:
            inspection_result["分片数量"] = "正常"
            for shard, indices in shard_per_node.items():
                inspection_result["分片数量"] += f" {shard} ({', '.join(indices)})"
        else:
            inspection_result["分片数量"] = "异常"
            for shard, indices in shard_per_node.items():
                inspection_result["分片数量"] += f" {shard} ({', '.join(indices)})"
        
        # 添加巡检结果到列表
        results.append(inspection_result)

    except requests.exceptions.Timeout:
        print(f"巡检超时: {instance_name}")

# 将巡检结果转换为DataFrame
df = pd.DataFrame(results)

# 将结果写入Excel文件
timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
excel_file_name = f"ES_Inspection_{timestamp}.xlsx"
with pd.ExcelWriter(excel_file_name) as writer:
    df.to_excel(writer, sheet_name="ES巡检结果", index=False)

print(f"巡检结果已保存到文件: {excel_file_name}")