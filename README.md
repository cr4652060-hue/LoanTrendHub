# LoanTrendHub

## 运行方式（外置 MySQL 配置）

1. 在 jar 同目录放置 `application.yml`（MySQL 配置）。
2. 启动命令：

```bash
java -jar loan-trend-hub-1.0.0.jar --spring.config.additional-location=file:./ --spring.config.name=application
```

启动日志会打印当前 `datasource url` 与 `driver`，用于确认未回落到其它数据库配置。

## 分支字典清洗脚本

当历史脏网点导致分支数异常（例如 70 个）时，执行：

```bash
mysql -h127.0.0.1 -uroot -p loantrendhub < scripts/reset-branch-dict.sql
```

该脚本会清空并重灌 35 个标准网点与别名映射（含“东张哨”常见变体）。