-- mfdh PostgreSQL 初始化脚本（M0）
-- 会话 / 修复 / 审批 / 知识库 / webhook 表将在 M3 里程碑创建。
-- 当前仅创建扩展，便于后续使用 UUID 主键。
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
