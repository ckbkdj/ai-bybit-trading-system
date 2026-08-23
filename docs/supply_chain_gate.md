# Git、安全与供应链门禁

更新时间：2026-08-23
当前状态：`BLOCKED`

## 已完成

- 本地版本历史保留旧代码和本轮修改，旧 v4.1 可追溯。
- `.env.local`、数据库、模型、日志、node_modules 和本机配置不进入 Git。
- 已跟踪源码的离线字面密钥扫描为 0；扫描只报告路径/行号/类型，绝不输出疑似 secret 值。
- npm `package-lock.json` 为 lockfile v3，已检查 resolved package 的 integrity 字段。
- release bundle 和 DB migration 均记录 SHA/checksum/code commit。
- 日志 redaction 覆盖 API key、secret、Bearer 和 Bybit signature。

## 阻断项

1. Python requirements 仍有非精确版本，不能重建完全一致的生产环境。
2. 当前测试环境缺少部分生产依赖，不能把本机 `pip freeze` 伪装成已验证 lock。
3. 没有附带当天的 `pip-audit`/`npm audit` 或等价漏洞扫描签名报告。
4. 尚未生成并签署生产 SBOM、容器/运行时 digest 和依赖许可证清单。

机器报告：`docs/evidence/supply_chain_audit_20260823.json`。

## 正确关闭方式

在隔离构建机上从声明文件解析一套兼容依赖，跑完整预测、训练 smoke、交易和 E2E 测试；生成带 hash 的 Python lock、保留 npm lock；执行漏洞/许可证扫描并存证；记录 Python/OS/CUDA/TA-Lib/TensorFlow/ONNX Runtime 版本；最后把所有 hash 写入 StrategyReleaseBundle。发现高危漏洞或 lock 漂移时不得晋升。

历史中曾出现的凭证即使当前工作树已清除，也应撤销并在安全机重新创建；Git 历史清洗属于破坏性操作，本轮没有擅自执行。
