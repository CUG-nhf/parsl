import sys
from pathlib import Path

repo_path = str(Path(__file__).parent / "parsl") 
sys.path.insert(0, repo_path) 

import parsl
from parsl.config import Config
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl import python_app

import logging
import time

# 验证导入路径是否正确
print(f"Parsl 正在从这里导入: {parsl.__file__}")
def set_logging():
    log_dir = Path(__file__).parent
    log_file_name = 'output.log'
    log_file_path = log_dir / log_file_name

    log_dir.mkdir(parents=True, exist_ok=True)

    # 安全删除旧日志文件
    if log_file_path.exists():
        try:
            log_file_path.unlink()
        except Exception as e:
            print(f"删除旧日志文件失败: {e}")
            
    # 创建独立 logger 实例
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # 防止重复添加 handler
    if not logger.handlers:
        # 创建文件处理器并设置格式
        file_handler = logging.FileHandler(log_file_path)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger

logger = set_logging()

# 定义并行函数
@python_app
def cpu_task():
    import socket
    return socket.gethostname()

def make_config():
    # 定义Parsl的配置
    return Config(
        executors=[
            HighThroughputExecutor(
                label="local_htex",
                cores_per_worker=1,
                max_workers_per_node=10,
                provider=LocalProvider(
                    min_blocks=1,
                    init_blocks=1,
                    max_blocks=1,
                    nodes_per_block=1,
                    parallelism=1,
                ),
            )
        ]
)

def simulate(config, task_number=100):
    dfk = None
    try:
        # 手动加载
        dfk = parsl.load(config)
        # warmup = cpu_task()
        # while len(dfk.executors['lsf_htex'].connected_managers) < max_nodes:
        #     time.sleep(1)
        # _ = warmup.result()
        
        logger.info(f"Submitted {task_number} tasks. Waiting for results...")
        
        start_time = time.time()
        futures = [cpu_task() for i in range(task_number)]
        results = [f.result() for f in futures]
        end_time = time.time()
        
    
        logger.info(f"Makespan: {end_time - start_time}, Throughput: { task_number // (end_time - start_time)} tasks/s \n")
        
        host_finish_tasks = {}
        for host in results:
            if host not in host_finish_tasks:
                host_finish_tasks[host] = 0
            host_finish_tasks[host] += 1
        for host, task_count in host_finish_tasks.items():
            logger.info(f"Host {host} finished {task_count} tasks")
        logger.info(f"Total nodes: {len(host_finish_tasks)}")
        
        return end_time - start_time

    except Exception as e:
        logger.error(f"Captured error: {e}", exc_info=True)
        return None
    finally:
        if dfk:
            parsl.dfk().cleanup()
    

TOTAL_TASK_NUMBER = 100

logger.info(f"Python Version: {sys.version}")
logger.info(f"Parsl Version: {parsl.__version__}")
config = make_config()
total_time = simulate(config, TOTAL_TASK_NUMBER)
