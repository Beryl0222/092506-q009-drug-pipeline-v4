# 创新药合作里程碑

这是一个面向“创新药共同开发”运营协作的服务端项目。服务把领域记录、负责人和当前状态保存到本地 SQLite，应用层通过进程内请求适配器提供健康检查和基础登记能力，业务流程在这个边界上运行。

## 目录

- `src/drug_pipeline/domain.py` 定义记录对象和时间处理。
- `src/drug_pipeline/store.py` 负责 SQLite 连接、表结构和记录读写。
- `src/drug_pipeline/service.py` 提供应用服务入口。
- `src/drug_pipeline/api.py` 将 JSON 请求转换为服务调用。
- `tests/` 保存领域边界的可重复测试。

## 运行

运行测试：`PYTHONPATH=src python3 -m unittest discover -s tests`

检查源码：`python3 -m compileall src`

项目只使用 Python 标准库，测试和运行不需要启动其他服务。
