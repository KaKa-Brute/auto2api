"""auto2api Python 版入口：加载 YAML 配置，启动优先级自动切换网关。

用法：
    python main.py -config config.yaml
与 Go 版完全独立，功能对齐。默认监听 :8080。
内置可视化管理台（/chat，免鉴权）：编辑配置、重启服务、查看日志。
"""
import argparse
import logging
import os
import socket
import subprocess
import sys
import time

import uvicorn

from auto2api import config as cfgmod
from auto2api.server import build_app, parse_addr

_log = logging.getLogger("auto2api")


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    """在绑定前等待端口释放（用于重启时新进程等待旧进程退出）。对齐 Go listenWithRetry。"""
    bind_host = "127.0.0.1" if host in ("0.0.0.0", "") else host
    deadline = time.time() + timeout
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((bind_host, port))
            s.close()
            return
        except OSError:
            s.close()
            if time.time() > deadline:
                return  # 超时也返回，交由 uvicorn 报错
            _log.info("port %d busy, waiting for previous instance to release...", port)
            time.sleep(0.3)


def spawn_self() -> None:
    """以相同解释器与参数启动一个新进程（继承环境与工作目录）。对齐 Go spawnSelf。

    新进程通过 wait_for_port 等待本进程释放端口后接管服务。
    """
    subprocess.Popen([sys.executable] + sys.argv,
                     cwd=os.getcwd(), env=os.environ.copy())


def main() -> None:
    parser = argparse.ArgumentParser(description="auto2api python gateway")
    parser.add_argument("-config", "--config", dest="config",
                        default="config.yaml", help="配置文件路径")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        cfg = cfgmod.load(args.config)
    except Exception as e:  # noqa: BLE001
        logging.error("load config: %s", e)
        sys.exit(1)

    host, port = parse_addr(cfg.server.addr)

    # 重启标志：管理台的 restart 回调置位后，主循环拉起新进程并优雅退出。
    state = {"restarting": False}

    # server 在下方创建后再被 restart 回调引用（闭包延迟绑定）。
    def restart() -> None:
        state["restarting"] = True
        server.should_exit = True  # 通知 uvicorn 优雅关闭

    app = build_app(cfg, config_path=args.config, restart=restart)

    uv_cfg = uvicorn.Config(app, host=host, port=port,
                            log_level="info", access_log=True)
    server = uvicorn.Server(uv_cfg)

    # 绑定前等待端口释放，便于重启时平滑接管。
    wait_for_port(host, port)

    server.run()

    # uvicorn 退出后：若是重启触发，则拉起携带相同参数的新进程接管服务。
    if state["restarting"]:
        _log.info("restart requested, spawning new process...")
        try:
            spawn_self()
            _log.info("old process stopped, new process is taking over")
        except Exception as e:  # noqa: BLE001
            _log.error("spawn new process failed: %s", e)
    else:
        _log.info("server stopped")


if __name__ == "__main__":
    main()
