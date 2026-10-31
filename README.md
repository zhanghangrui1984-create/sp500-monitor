# 标普500正式模型 v17 每日报告

2026-10-31采集修正：FCS每个完整周按实际有效配对天数求均值（至少1天）；空周仅在距来源周末14个日历日内沿用，第15天起未知，旧值仅供核查，不能据此确认金融离场。CFNAI按官方发布日历沿用，不再按月初80天清空；Yahoo日线末行延迟时核验常规收盘报价补齐，JSON及数据完整性失败重试。采集版本`live-data-20261031-fcs14d`，详见下方因子文档。手动工作流默认验证不发邮件，定时日报继续发送。

运行正式模型的三个入场、独立金融离场和三情景统一离场，每天北京时间06:30由原GitHub Actions工作流启动，计算美股最近已完成交易日的因子并发送HTML邮件。周末仍发报告并注明上一交易日。

- [全部因子获取、计算、发布时间和缺数处理](docs/因子获取与计算_v17.md)
- [正式模型配置](monitor_v17/model.json)
- [历史逐日一致性验证](tests/fixtures/parity_summary.json)
- [每日运行记录](https://github.com/zhanghangrui1984-create/sp500-monitor/actions)

配置使用原仓库Secrets：`FRED_API_KEY`、`EMAIL_ADDRESS`、`EMAIL_PASSWORD`。默认发给原发件邮箱；可选变量`EMAIL_RECEIVER`。代码不保存密钥。此前旧版本曾把凭据写入公开Git历史，需在相应服务中轮换旧凭据；本次不重写历史。

```sh
pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m monitor_v17.main --no-email
python -m monitor_v17.main
```

报告只提供模型信号，不维护用户实际持仓或提交交易。旧十触发器版本仅作历史存档，不能作为当前生产入口。
