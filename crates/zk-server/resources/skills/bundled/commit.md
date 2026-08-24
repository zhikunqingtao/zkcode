# /commit — 智能提交

## 目标
分析暂存区的变更，创建结构良好的 git commit。

## 步骤
1. 运行 `git diff --cached --stat` 查看哪些文件发生了变更
2. 运行 `git diff --cached` 查看实际变更内容
3. 分析变更并确定：
   - 类型：feat/fix/refactor/docs/test/chore
   - 范围：受影响的模块或组件
   - 摘要：简洁描述（最多 72 个字符）
4. 按 Conventional Commits 格式生成提交信息
5. 向用户展示信息以确认
6. 执行 `git commit -m "<message>"`

## 规则
- 第一行必须 <= 72 个字符
- 使用祈使语气（"Add feature" 而非 "Added feature"）
- 如果变更涉及多个关注点，建议拆分为多个提交
- 如有重大变更（breaking change），必须包含相应的 footer
