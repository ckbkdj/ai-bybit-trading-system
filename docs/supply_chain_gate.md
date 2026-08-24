# Git、安全与供应链门禁

更新时间：2026-08-25
当前状态：`LOCKED_DEPENDENCIES_CONFIGURED / CURRENT_ATTESTATION_REQUIRED`

## 已完成

- Ubuntu Python 3.11 和 Windows Python 3.12 应用依赖从平台锁文件安装；Windows 锁启用 `--require-hashes`。
- `pip-audit` 从 `requirements/audit.lock` 安装，不再在工作流内临时拼接应用依赖。

- 本地版本历史保留旧代码和本轮修改，旧 v4.1 可追溯。
- `.env.local`、数据库、模型、日志、node_modules 和本机配置不进入 Git。
- 已跟踪源码的离线字面密钥扫描为 0；扫描只报告路径/行号/类型，绝不输出疑似 secret 值。
- npm `package-lock.json` 为 lockfile v3，已检查 resolved package 的 integrity 字段。
- release bundle 和 DB migration 均记录 SHA/checksum/code commit。
- 日志 redaction 覆盖 API key、secret、Bearer 和 Bybit signature。

## 阻断项

1. 仍需由当前 HEAD 的 CI 生成当天 `pip-audit` 证据；历史报告不能替代。
2. 尚未生成并签署生产 SBOM、容器/运行时 digest 和依赖许可证清单。

机器报告：`docs/evidence/supply_chain_audit_20260823.json`。

## 正确关闭方式

在隔离构建机上从平台锁安装并跑完整预测、训练 smoke、交易和 E2E 测试；Windows 必须校验锁内 hash，Ubuntu 必须校验全版本 pin，npm 保留现有 lock；执行漏洞/许可证扫描并存证；记录 Python/OS/CUDA/TA-Lib/TensorFlow/ONNX Runtime 版本；最后把所有 hash 写入 StrategyReleaseBundle。发现高危漏洞或 lock 漂移时不得晋升。

历史中曾出现的凭证即使当前工作树已清除，也应撤销并在安全机重新创建；Git 历史清洗属于破坏性操作，本轮没有擅自执行。
