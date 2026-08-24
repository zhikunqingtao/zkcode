//! 配置域仓储——[`Db`](crate::Db) 的 config 单行 KV 表读写（多文件 impl 之一）。
//!
//! 语义来源（旧仓库只读，2026-08-16 冻结）：`ConfigService.loadConfigJson` /
//! `saveConfigJson`（global.db 的 `global_config` 表，S7b 裁定并入 D6 单库的
//! `config` 表）。value 为不透明 JSON 文本——序列化形状由 zk-server 的
//! `UserConfig` DTO 负责，本层不做解析（旧系统同样以 String 存取）。

use rusqlite::{OptionalExtension, params};

use crate::error::DbError;
use crate::time::{format_rfc3339_micros, now_millis};

impl crate::Db {
    /// 读取配置值（`GET /api/config` 数据源；对齐旧 `loadConfigJson`）。
    ///
    /// 无对应行时返回 `Ok(None)`（调用方以代码内置默认值兜底，
    /// 对齐旧 `defaultUserConfig` 语义）。
    ///
    /// # Errors
    /// 底层 `SQLite` 查询失败时返回。
    pub async fn get_config_value(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_owned();
        self.with_reader(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT value FROM config WHERE key = ?1",
                    params![key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?)
        })
        .await
    }

    /// 写入（upsert）配置值（`PUT /api/config` 落库；对齐旧 `saveConfigJson`
    /// 的 UPDATE + INSERT 双步，`SQLite` 侧以单条 `ON CONFLICT` 等价表达）。
    ///
    /// # Errors
    /// 底层 `SQLite` 写入失败时返回。
    pub async fn put_config_value(&self, key: &str, value: &str) -> Result<(), DbError> {
        let key = key.to_owned();
        let value = value.to_owned();
        self.with_writer(move |conn| {
            let updated_at = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO config (key, value, updated_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key) DO UPDATE SET value = ?2, updated_at = ?3",
                params![key, value, updated_at],
            )?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    /// 读缺省 → 写入 → 读回 → 覆盖写（upsert 幂等）全链路。
    #[tokio::test]
    async fn config_kv_roundtrip_and_upsert() {
        let db = crate::Db::open_in_memory().expect("boot");
        assert_eq!(db.get_config_value("user_config").await.expect("get"), None);
        db.put_config_value("user_config", r#"{"theme":"dark"}"#)
            .await
            .expect("insert");
        assert_eq!(
            db.get_config_value("user_config").await.expect("get"),
            Some(r#"{"theme":"dark"}"#.to_owned())
        );
        db.put_config_value("user_config", r#"{"theme":"light"}"#)
            .await
            .expect("upsert");
        assert_eq!(
            db.get_config_value("user_config").await.expect("get"),
            Some(r#"{"theme":"light"}"#.to_owned())
        );
    }

    /// 文件库跨连接持久：写入 → drop → 重开同一文件仍可读回（重启保留语义）。
    #[tokio::test]
    async fn config_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("zk-db-config-{}", uuid::Uuid::new_v4()));
        let path = dir.join("data.db");
        {
            let db = crate::Db::open(&path).expect("open file db");
            db.put_config_value("user_config", r#"{"locale":"en"}"#)
                .await
                .expect("write");
        }
        let db = crate::Db::open(&path).expect("reopen file db");
        assert_eq!(
            db.get_config_value("user_config").await.expect("get"),
            Some(r#"{"locale":"en"}"#.to_owned())
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
