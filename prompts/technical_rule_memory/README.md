# 技术规则分类模型记忆

## 目标

这组文档用于把 `问题对应技术规则示例(1).xlsx` 中的人为判断经验，沉淀为可复用的“模型记忆块”。

适用场景：

- 对单条审计问题做技术规则分类
- 作为提示词中的长期记忆，而不是把整张示例表反复塞给模型
- 给规则引擎、候选分类召回、few-shot 提示提供统一口径

## 使用方式

优先加载顺序建议如下：

1. [00_overview.md](./00_overview.md)
2. [01_core_categories.md](./01_core_categories.md)
3. [02_boundary_rules.md](./02_boundary_rules.md)
4. [03_multilabel_and_fallback.md](./03_multilabel_and_fallback.md)
5. [04_usage_strategy.md](./04_usage_strategy.md)

如果只想最小化加载成本，至少加载：

- `00_overview.md`
- `01_core_categories.md`

## 结构说明

- `00_overview.md`
  任务目标、总体原则、推荐主分类池
- `01_core_categories.md`
  核心分类的定义、触发特征、判断模式
- `02_boundary_rules.md`
  易混分类的边界判断规则
- `03_multilabel_and_fallback.md`
  多标签策略、兜底规则、输出规范
- `04_usage_strategy.md`
  如何把这些记忆块用于模型分类与候选召回

## 总结

这组文档沉淀的是：

- 判断特征
- 分类边界
- 输出规则

不是对原示例表的机械复制。
