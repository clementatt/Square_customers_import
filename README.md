# Square 客户数据管理工具

[English Version](README_EN.md)

这是一个用于管理 Square 客户数据的 Python 工具集，包含客户数据导入功能。

## 功能特点

- **客户数据导入**：支持批量导入客户数据到 Square 系统
  - 支持 CSV 和 Excel (xlsx/xls) 格式的数据文件
  - 支持客户群组管理，便于分类和批量操作
  - 自动检测和处理重复客户数据
  - 同一周内手机号查重功能，避免重复导入
- **进度显示**：使用进度条实时显示操作进度
  - 显示总体进度和当前批次进度
  - 实时更新成功/失败数量统计
- **日志记录**：详细记录所有操作过程和结果

## 环境要求

- Python 3.6+
- Square API 访问令牌

## 安装

1. 克隆或下载本项目到本地
2. 进入项目目录，安装依赖：

```bash
pip install -r requirements.txt
```

## 配置

1. 复制配置文件模板：
```bash
cp .env.example .env
```

2. 编辑 `.env` 文件，配置你的 Square API 访问令牌：
```env
SQUARE_ACCESS_TOKEN=your_access_token_here
SQUARE_ENVIRONMENT=sandbox  # 或 production
```

## 使用方法

### 导入客户数据

支持从 CSV 或 Excel 文件导入客户数据：

```python
from square_customer_import import SquareCustomerImport

# 初始化导入工具
importer = SquareCustomerImport()

# 导入CSV格式的客户数据
importer.import_customers('customers.csv')

# 或导入Excel格式的客户数据
importer.import_customers('customers.xlsx')
```

#### 数据文件格式要求

- CSV/Excel 文件必须包含以下列：
  - `Customer name`：客户姓名（格式：姓/名 或 完整名字）
  - `Customer email`：客户邮箱
  - `Customer phone number`：客户电话（可选，自动添加国际区号）
  - `Pick-up time (local)`：送达时间（格式：YYYY-MM-DD HH:MM:SS），用于按周分组客户

### 客户群组管理

系统会根据客户数据中的`Pick-up time (local)`字段自动计算周数，并按周数创建不同的客户组：

```python
# 导入客户数据，系统会自动按周数分组
importer.import_customers('customers.csv')
```

每个客户组的命名格式为：`YYYY年第W周_客户组`，其中YYYY为当前年份，W为ISO周数（1-53）。

## 进度显示

工具在执行批量操作时会显示详细的进度信息：

- 总体进度条：显示整体任务完成百分比
- 批次进度条：显示当前批次的处理进度
- 实时统计：显示成功/失败的数量统计
- 日志输出：同步显示详细的操作日志

## 注意事项

- 在生产环境中使用前，请先在沙箱环境中测试
- 确保备份重要数据
- 不要将 `.env` 文件提交到版本控制系统
- 大批量导入时建议分批进行，每批建议不超过1000条数据

## 许可证

[MIT License](LICENSE)