# 文件名: main.py
# ---------------------
# desc: 爬虫框架主入口 (调度器)
#       - 负责加载和合并配置
#       - 负责动态调度子项目
# ---------------------
import sys
import os
import yaml
import importlib
import logging


# --- 1. 配置加载器 ---

def load_config(project_name: str) -> dict:
    """
    加载并合并三层配置
    1. conf/config.yml (环境开关)
    2. conf/infrastructure.yml (密钥库)
    3. conf/projects/{project_name}.yml (业务逻辑)
    """
    try:
        # --- 加载文件 ---
        with open("config/config.yml", 'r', encoding='utf-8') as f:
            env_config = yaml.safe_load(f)

        with open("config/infrastructure.yml", 'r', encoding='utf-8') as f:
            infra_config = yaml.safe_load(f)

        project_config_path = f"../common-mod/config/projects/{project_name}.yml"
        if not os.path.exists(project_config_path):
            raise FileNotFoundError(f"项目配置文件未找到: {project_config_path}")

        with open(project_config_path, 'r', encoding='utf-8') as f:
            project_config = yaml.safe_load(f)

        # --- 合并配置 ---

        # 1. 确定当前环境
        current_env = env_config.get("environment", "dev")  # 默认为 dev
        logging.info(f"--- 当前运行环境: {current_env} ---")

        # 2. 从 env_config 获取环境特定设置
        env_settings = env_config.get("environments", {}).get(current_env, {})

        # 3. 解析连接 (e.g., "data_db" -> "prod_data_db")
        connection_map = env_settings.get("connections", {})
        final_connections = {}

        if "data_db" in connection_map:
            conn_key = connection_map["data_db"]
            final_connections["data_db"] = infra_config.get("databases", {}).get(conn_key)

        if "monitor_db" in connection_map:
            conn_key = connection_map["monitor_db"]
            final_connections["monitor_db"] = infra_config.get("databases", {}).get(conn_key)

        if "storage" in connection_map:
            conn_key = connection_map["storage"]
            final_connections["storage"] = infra_config.get("obs_storage", {}).get(conn_key)

        # 4. 组合最终配置
        final_config = {}
        final_config.update(project_config)  # 基础业务逻辑
        final_config["connections"] = final_connections  # 注入的具体连接
        final_config["env_settings"] = env_settings.get("env_settings", {})  # 注入的环境开关

        return final_config

    except FileNotFoundError as e:
        logging.error(f"配置加载失败: {e}")
        return None
    except Exception as e:
        logging.error(f"解析配置失败: {e}", exc_info=True)
        return None


# --- 2. 动态调度器 ---

# 项目/阶段 与 类的映射
# (未来有新项目, 在此注册即可)
SPIDER_REGISTRY = {
    "csrc_gov": {
        "list": "projects.csrc_gov.csrc_gov_list_spider.CsrcGovListSpider",
        "detail": "projects.csrc_gov.csrc_gov_detail_spider.CsrcGovDetailSpider",
        "attachment": "projects.csrc_gov.csrc_gov_attachment_spider.CsrcGovAttachmentSpider",
    },
    # "new_site": {
    #     "list": "projects.new_site.new_site_list_spider.NewSiteListSpider",
    #     "detail": "...",
    # }
}


def get_spider_class(project: str, stage: str) -> type | None:
    """
    动态导入爬虫类
    """
    try:
        module_path, class_name = SPIDER_REGISTRY[project][stage].rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except (KeyError, ImportError) as e:
        logging.error(f"无法加载爬虫: Project={project}, Stage={stage}. 错误: {e}")
        return None


# --- 3. 主入口 ---

if __name__ == '__main__':
    # 配置基础日志，以便在加载配置失败时也能记录
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # --- 1. 解析参数 ---
    # 用法: python main.py csrc_gov list
    # 用法: python main.py csrc_gov detail
    # 用法: python main.py csrc_gov attachment
    if len(sys.argv) < 3:
        print("用法: python main.py [项目名] [阶段名]")
        print("可用项目:", list(SPIDER_REGISTRY.keys()))
        sys.exit(1)

    project_name = sys.argv[1]
    stage_name = sys.argv[2]

    # --- 2. 加载配置 ---
    final_config = load_config(project_name)
    if not final_config:
        sys.exit(1)

    # --- 3. 加载爬虫类 ---
    SpiderClass = get_spider_class(project_name, stage_name)
    if not SpiderClass:
        sys.exit(1)

    # --- 4. 实例化并运行 ---
    try:
        spider_instance = SpiderClass(project_config=final_config)
        spider_instance.run()
    except Exception as e:
        logging.error(f"爬虫 {SpiderClass.task_name} 运行时发生顶层异常: {e}", exc_info=True)
        sys.exit(1)