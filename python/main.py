"""auto2api Python 版入口：加载 YAML 配置，启动优先级自动切换网关。

用法：
    python main.py -config config.yaml
与 Go 版完全独立，功能对齐。默认监听 :8080。
"""
import argparse
import logging
import sys

import uvicorn

from auto2api import config as cfgmod
from auto2api.server import build_app, parse_addr


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

    app = build_app(cfg)
    host, port = parse_addr(cfg.server.addr)
    uvicorn.run(app, host=host, port=port, log_level="info", access_log=True)


if __name__ == "__main__":
    main()
