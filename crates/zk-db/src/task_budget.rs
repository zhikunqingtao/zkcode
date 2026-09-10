//! Durable root-task budgets and attached-child reservations.
//!
//! Budget counters live on the root `tasks` row and every attached child has one
//! immutable allocation row.  All mutations happen inside the same `SQLite` write
//! transaction as task submission or terminal result commit; an in-memory tracker is
//! never authoritative.

#![allow(
    missing_docs,
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};

use crate::Db;
use crate::error::DbError;
use crate::task_runtime::CasOutcome;
use crate::time::{format_rfc3339_micros, now_millis};

/// Direct children may reserve at most one fifth of each finite root budget.
pub const DIRECT_CHILD_BUDGET_PERCENT: i64 = 20;
/// At least one fifth of each finite root budget is unavailable to child reservations.
pub const ROOT_BUDGET_RESERVE_PERCENT: i64 = 20;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TaskBudgetLimits {
    pub token_limit: Option<i64>,
    pub cost_limit_nanos_usd: Option<i64>,
    pub deadline_at_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BudgetReservationStatus {
    Active,
    Settled,
    Incomplete,
}

impl BudgetReservationStatus {
    fn parse(value: &str) -> Result<Self, DbError> {
        match value {
            "active" => Ok(Self::Active),
            "settled" => Ok(Self::Settled),
            "incomplete" => Ok(Self::Incomplete),
            other => Err(DbError::Invalid(format!(
                "BUDGET_RESERVATION_STATUS_CORRUPT:{other}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskBudgetReservationRecord {
    pub child_task_id: String,
    pub root_task_id: String,
    pub reserved_tokens: Option<i64>,
    pub reserved_cost_nanos_usd: Option<i64>,
    pub used_tokens: Option<i64>,
    pub used_cost_nanos_usd: Option<i64>,
    pub usage_complete: bool,
    pub status: BudgetReservationStatus,
    pub version: i64,
    pub created_at: String,
    pub settled_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskBudgetSnapshot {
    pub task_id: String,
    pub root_task_id: String,
    pub token_limit: Option<i64>,
    pub cost_limit_nanos_usd: Option<i64>,
    pub deadline_at_ms: Option<i64>,
    pub reserved_tokens: i64,
    pub reserved_cost_nanos_usd: i64,
    pub consumed_tokens: i64,
    pub consumed_cost_nanos_usd: i64,
    pub available_tokens: Option<i64>,
    pub available_cost_nanos_usd: Option<i64>,
    pub usage_complete: bool,
    pub budget_version: i64,
    pub reservation: Option<TaskBudgetReservationRecord>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ExecutionBudgetEnvelope {
    budget: Option<TaskBudgetLimits>,
}

pub(crate) fn parse_execution_budget(json: &str) -> Result<TaskBudgetLimits, DbError> {
    let envelope: ExecutionBudgetEnvelope = serde_json::from_str(json)?;
    let limits = envelope.budget.unwrap_or_default();
    validate_limits(&limits)?;
    Ok(limits)
}

pub(crate) fn validate_limits(limits: &TaskBudgetLimits) -> Result<(), DbError> {
    if limits.token_limit.is_some_and(|value| value <= 0) {
        return Err(DbError::Invalid("TOKEN_BUDGET_INVALID".to_owned()));
    }
    if limits.cost_limit_nanos_usd.is_some_and(|value| value <= 0) {
        return Err(DbError::Invalid("COST_BUDGET_INVALID".to_owned()));
    }
    if limits.deadline_at_ms.is_some_and(|value| value <= 0) {
        return Err(DbError::Invalid("TASK_DEADLINE_INVALID".to_owned()));
    }
    Ok(())
}

fn ceil_percent(value: i64, percent: i64) -> i64 {
    let whole = (value / 100) * percent;
    let remainder = (value % 100) * percent;
    whole + (remainder + 99) / 100
}

fn floor_percent(value: i64, percent: i64) -> i64 {
    (value / 100) * percent + ((value % 100) * percent) / 100
}

fn grant_dimension(
    name: &str,
    root_limit: Option<i64>,
    consumed: i64,
    already_reserved: i64,
    requested: Option<i64>,
) -> Result<Option<i64>, DbError> {
    let Some(root_limit) = root_limit else {
        return Ok(requested);
    };
    let per_child_cap = floor_percent(root_limit, DIRECT_CHILD_BUDGET_PERCENT);
    let parent_reserve = ceil_percent(root_limit, ROOT_BUDGET_RESERVE_PERCENT);
    let child_pool_cap = root_limit.saturating_sub(parent_reserve);
    let spend_available = root_limit
        .saturating_sub(consumed)
        .saturating_sub(already_reserved);
    let pool_available = child_pool_cap.saturating_sub(already_reserved);
    let available = spend_available.min(pool_available).max(0);
    let desired = requested.unwrap_or_else(|| per_child_cap.min(available));
    if desired <= 0 {
        return Err(DbError::Invalid(format!("{name}_BUDGET_UNAVAILABLE")));
    }
    if desired > per_child_cap {
        return Err(DbError::Invalid(format!(
            "{name}_CHILD_BUDGET_EXCEEDS_20_PERCENT"
        )));
    }
    if desired > available {
        return Err(DbError::Invalid(format!("{name}_BUDGET_UNAVAILABLE")));
    }
    Ok(Some(desired))
}

/// Inserts the child allocation and atomically charges the root account. The child task
/// must already have been inserted in the caller's still-uncommitted transaction.
pub(crate) fn reserve_child_budget_in_current_write(
    conn: &Connection,
    root_task_id: &str,
    child_task_id: &str,
    requested: &TaskBudgetLimits,
    now: &str,
) -> Result<(), DbError> {
    let root: Option<(
        Option<i64>,
        Option<i64>,
        Option<i64>,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        bool,
        bool,
    )> = conn
        .query_row(
            "SELECT token_budget_limit,cost_budget_nanos_usd,deadline_at_ms,
                    budget_reserved_tokens,budget_reserved_cost_nanos_usd,
                    budget_consumed_tokens,budget_consumed_cost_nanos_usd,budget_version,
                    COALESCE((SELECT total_tokens FROM run_envelopes
                              WHERE id=tasks.current_run_id),0),
                    COALESCE((SELECT cost_nanos_usd FROM run_envelopes
                              WHERE id=tasks.current_run_id),0),
                    COALESCE((SELECT SUM(reserved_input_tokens+reserved_output_tokens)
                              FROM llm_calls WHERE run_id=tasks.current_run_id
                                AND status='started'),0),
                    COALESCE((SELECT SUM(reserved_cost_nanos_usd)
                              FROM llm_calls WHERE run_id=tasks.current_run_id
                                AND status='started'),0),
                    usage_complete,
                    COALESCE((SELECT usage_complete FROM run_envelopes
                              WHERE id=tasks.current_run_id AND task_id=tasks.id),0)
             FROM tasks WHERE id=?1 AND parent_task_id IS NULL AND root_task_id=id",
            params![root_task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get::<_, i64>(12)? != 0,
                    row.get::<_, i64>(13)? != 0,
                ))
            },
        )
        .optional()?;
    let Some((
        root_token_limit,
        root_cost_limit,
        root_deadline,
        reserved_tokens,
        reserved_cost,
        consumed_tokens,
        consumed_cost,
        budget_version,
        current_run_tokens,
        current_run_cost,
        active_call_tokens,
        active_call_cost,
        root_usage_complete,
        current_run_usage_complete,
    )) = root
    else {
        return Err(DbError::Invalid("ROOT_BUDGET_ACCOUNT_NOT_FOUND".to_owned()));
    };

    // A child reservation is a new spend decision against the root account. If
    // either the logical Task aggregate or its authoritative current Run has
    // incomplete usage, the available balance cannot be proven. Fail before
    // touching the root counters; the caller's encompassing submission
    // transaction then rolls back the already-staged child row as well.
    let root_run_id: String = conn.query_row(
        "SELECT current_run_id FROM tasks WHERE id=?1",
        [root_task_id],
        |row| row.get(0),
    )?;
    if (!root_usage_complete || !current_run_usage_complete)
        && !crate::runtime_ledger::timeout_usage_exception(conn, &root_run_id)?
    {
        return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
    }

    // Token and cost ceilings are optional, but an absent deadline can leave
    // cleanup responsibility alive forever. Fail before the child is observable.
    if root_deadline.is_none() {
        return Err(DbError::Invalid("ROOT_BUDGET_NOT_CONFIGURED".to_owned()));
    }

    let current_ms = now_millis();
    if root_deadline.is_some_and(|deadline| deadline <= current_ms) {
        return Err(DbError::Invalid("TASK_DEADLINE_EXCEEDED".to_owned()));
    }
    if let (Some(requested_deadline), Some(root_deadline)) =
        (requested.deadline_at_ms, root_deadline)
        && requested_deadline > root_deadline
    {
        return Err(DbError::Invalid(
            "CHILD_DEADLINE_EXCEEDS_ROOT_DEADLINE".to_owned(),
        ));
    }
    if requested
        .deadline_at_ms
        .is_some_and(|deadline| deadline <= current_ms)
    {
        return Err(DbError::Invalid("TASK_DEADLINE_EXCEEDED".to_owned()));
    }

    let token_limit = grant_dimension(
        "TOKEN",
        root_token_limit,
        consumed_tokens
            .saturating_add(current_run_tokens)
            .saturating_add(active_call_tokens),
        reserved_tokens,
        requested.token_limit,
    )?;
    let cost_limit_nanos_usd = grant_dimension(
        "COST",
        root_cost_limit,
        consumed_cost
            .saturating_add(current_run_cost)
            .saturating_add(active_call_cost),
        reserved_cost,
        requested.cost_limit_nanos_usd,
    )?;
    let deadline_at_ms = match (requested.deadline_at_ms, root_deadline) {
        (Some(requested), Some(root)) => Some(requested.min(root)),
        (requested @ Some(_), None) => requested,
        (None, root) => root,
    };
    let token_charge = token_limit.unwrap_or(0);
    let cost_charge = cost_limit_nanos_usd.unwrap_or(0);

    let changed = conn.execute(
        "UPDATE tasks SET
            budget_reserved_tokens=budget_reserved_tokens+?1,
            budget_reserved_cost_nanos_usd=budget_reserved_cost_nanos_usd+?2,
            budget_version=budget_version+1,updated_at=?3
         WHERE id=?4 AND budget_version=?5
           AND (token_budget_limit IS NULL OR
                budget_consumed_tokens+budget_reserved_tokens+?1
                +COALESCE((SELECT total_tokens FROM run_envelopes
                           WHERE id=tasks.current_run_id),0)
                +COALESCE((SELECT SUM(reserved_input_tokens+reserved_output_tokens)
                           FROM llm_calls WHERE run_id=tasks.current_run_id
                             AND status='started'),0) <= token_budget_limit)
           AND (cost_budget_nanos_usd IS NULL OR
                budget_consumed_cost_nanos_usd+budget_reserved_cost_nanos_usd+?2
                    +COALESCE((SELECT cost_nanos_usd FROM run_envelopes
                               WHERE id=tasks.current_run_id),0)
                    +COALESCE((SELECT SUM(reserved_cost_nanos_usd)
                               FROM llm_calls WHERE run_id=tasks.current_run_id
                                 AND status='started'),0)
                    <= cost_budget_nanos_usd)",
        params![token_charge, cost_charge, now, root_task_id, budget_version],
    )?;
    if changed != 1 {
        let usage_complete: Option<(bool, bool)> = conn
            .query_row(
                "SELECT usage_complete,
                        COALESCE((SELECT usage_complete FROM run_envelopes
                                  WHERE id=tasks.current_run_id AND task_id=tasks.id),0)
                 FROM tasks WHERE id=?1 AND parent_task_id IS NULL AND root_task_id=id",
                params![root_task_id],
                |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
            )
            .optional()?;
        if usage_complete.is_some_and(|(task, run)| !task || !run) {
            return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
        }
        return Err(DbError::Invalid("BUDGET_VERSION_CONFLICT".to_owned()));
    }
    conn.execute(
        "UPDATE tasks SET token_budget_limit=?1,cost_budget_nanos_usd=?2,
            deadline_at_ms=?3,updated_at=?4
         WHERE id=?5 AND parent_task_id IS NOT NULL",
        params![
            token_limit,
            cost_limit_nanos_usd,
            deadline_at_ms,
            now,
            child_task_id
        ],
    )?;
    conn.execute(
        "INSERT INTO task_budget_reservations
            (child_task_id,root_task_id,reserved_tokens,reserved_cost_nanos_usd,
             usage_complete,status,version,created_at)
         VALUES(?1,?2,?3,?4,0,'active',0,?5)",
        params![
            child_task_id,
            root_task_id,
            token_limit,
            cost_limit_nanos_usd,
            now
        ],
    )?;
    Ok(())
}

/// Settles one task's authoritative Run usage. Child allocations are released only when
/// `run_envelopes.usage_complete=1` and usage fits the immutable child grant. Missing
/// usage leaves the allocation charged and poisons the child/root usage projections.
/// A provider-side overrun is different: the authoritative usage is still complete, so
/// the reservation becomes `incomplete` for operator audit without poisoning usage
/// integrity. Its original grant remains reserved, and any known excess is charged to
/// the root account so that no part of the overrun can be reused by later admissions.
/// If that excess crosses the root hard limit, the charge saturates the remaining budget;
/// the exact uncapped usage remains available in the Run and reservation audit rows.
pub(crate) fn settle_task_budget_in_current_write(
    conn: &Connection,
    task_id: &str,
    run_id: &str,
    now: &str,
) -> Result<(), DbError> {
    let task: (Option<String>, String, i64, i64) = conn.query_row(
        "SELECT parent_task_id,root_task_id,budget_consumed_tokens,
                budget_consumed_cost_nanos_usd FROM tasks WHERE id=?1",
        params![task_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let usage: (i64, i64, bool) = conn.query_row(
        "SELECT total_tokens,cost_nanos_usd,usage_complete
         FROM run_envelopes WHERE id=?1 AND task_id=?2",
        params![run_id, task_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)? != 0)),
    )?;
    let (used_tokens, used_cost, usage_complete) = usage;

    if task.0.is_none() {
        if usage_complete {
            let changed = conn.execute(
                "UPDATE tasks SET budget_consumed_tokens=budget_consumed_tokens+?1,
                    budget_consumed_cost_nanos_usd=budget_consumed_cost_nanos_usd+?2,
                    budget_version=budget_version+1,updated_at=?3
                 WHERE id=?4
                   AND (token_budget_limit IS NULL OR
                        budget_consumed_tokens+budget_reserved_tokens+?1 <= token_budget_limit)
                   AND (cost_budget_nanos_usd IS NULL OR
                        budget_consumed_cost_nanos_usd+budget_reserved_cost_nanos_usd+?2
                            <= cost_budget_nanos_usd)",
                params![used_tokens, used_cost, now, task_id],
            )?;
            if changed != 1 {
                return Err(DbError::Invalid("ROOT_BUDGET_EXHAUSTED".to_owned()));
            }
        } else {
            conn.execute(
                "UPDATE tasks SET usage_complete=0,budget_version=budget_version+1,
                    updated_at=?1 WHERE id=?2",
                params![now, task_id],
            )?;
        }
        return Ok(());
    }

    let reservation: (Option<i64>, Option<i64>, String, i64) = conn
        .query_row(
            "SELECT reserved_tokens,reserved_cost_nanos_usd,status,version
             FROM task_budget_reservations WHERE child_task_id=?1 AND root_task_id=?2",
            params![task_id, task.1],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?
        .ok_or_else(|| DbError::Invalid("CHILD_BUDGET_RESERVATION_NOT_FOUND".to_owned()))?;
    if reservation.2 != "active" {
        return Err(DbError::Invalid(
            "CHILD_BUDGET_RESERVATION_ALREADY_TERMINAL".to_owned(),
        ));
    }
    let reserved_tokens = reservation.0.unwrap_or(0);
    let reserved_cost = reservation.1.unwrap_or(0);
    // A recovered Task carries usage from interrupted attempts on its durable
    // Task row. The final settlement charges the root exactly once for the
    // complete logical Task rather than only for its last physical Run.
    let total_used_tokens = task.2.saturating_add(used_tokens);
    let total_used_cost = task.3.saturating_add(used_cost);
    if !usage_complete {
        let updated = conn.execute(
            "UPDATE task_budget_reservations SET used_tokens=?1,used_cost_nanos_usd=?2,
                status='incomplete',usage_complete=0,settled_at=?3,version=version+1
             WHERE child_task_id=?4 AND status='active' AND version=?5",
            params![
                Option::<i64>::None,
                Option::<i64>::None,
                now,
                task_id,
                reservation.3
            ],
        )?;
        if updated != 1 {
            return Err(DbError::Invalid("BUDGET_VERSION_CONFLICT".to_owned()));
        }
        conn.execute(
            "UPDATE tasks SET usage_complete=0,budget_version=budget_version+1,
                updated_at=?1 WHERE id IN (?2,?3)",
            params![now, task_id, task.1],
        )?;
        return Ok(());
    }

    let allocation_exceeded = reservation.0.is_some_and(|limit| total_used_tokens > limit)
        || reservation.1.is_some_and(|limit| total_used_cost > limit);
    if allocation_exceeded {
        let updated = conn.execute(
            "UPDATE task_budget_reservations SET used_tokens=?1,used_cost_nanos_usd=?2,
                status='incomplete',usage_complete=1,settled_at=?3,version=version+1
             WHERE child_task_id=?4 AND status='active' AND version=?5",
            params![
                total_used_tokens,
                total_used_cost,
                now,
                task_id,
                reservation.3
            ],
        )?;
        if updated != 1 {
            return Err(DbError::Invalid("BUDGET_VERSION_CONFLICT".to_owned()));
        }

        let root: (
            Option<i64>,
            Option<i64>,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
        ) = conn.query_row(
            "SELECT root.token_budget_limit,root.cost_budget_nanos_usd,
                        root.budget_reserved_tokens,root.budget_reserved_cost_nanos_usd,
                        root.budget_consumed_tokens,root.budget_consumed_cost_nanos_usd,
                        COALESCE(run.total_tokens,0),COALESCE(run.cost_nanos_usd,0),
                        COALESCE((SELECT SUM(call.reserved_input_tokens+
                                                   call.reserved_output_tokens)
                                  FROM llm_calls call
                                  WHERE call.run_id=root.current_run_id
                                    AND call.status='started'),0),
                        COALESCE((SELECT SUM(call.reserved_cost_nanos_usd)
                                  FROM llm_calls call
                                  WHERE call.run_id=root.current_run_id
                                    AND call.status='started'),0)
                 FROM tasks root
                 LEFT JOIN run_envelopes run
                   ON run.id=root.current_run_id AND run.task_id=root.id
                 WHERE root.id=?1 AND root.parent_task_id IS NULL
                   AND root.root_task_id=root.id",
            params![task.1],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                ))
            },
        )?;
        let token_excess = total_used_tokens.saturating_sub(reserved_tokens).max(0);
        let cost_excess = total_used_cost.saturating_sub(reserved_cost).max(0);
        let chargeable_token_excess = root.0.map_or(token_excess, |limit| {
            limit
                .saturating_sub(root.2)
                .saturating_sub(root.4)
                .saturating_sub(root.6)
                .saturating_sub(root.8)
                .max(0)
                .min(token_excess)
        });
        let chargeable_cost_excess = root.1.map_or(cost_excess, |limit| {
            limit
                .saturating_sub(root.3)
                .saturating_sub(root.5)
                .saturating_sub(root.7)
                .saturating_sub(root.9)
                .max(0)
                .min(cost_excess)
        });
        let root_updated = conn.execute(
            "UPDATE tasks SET
                budget_consumed_tokens=budget_consumed_tokens+?1,
                budget_consumed_cost_nanos_usd=budget_consumed_cost_nanos_usd+?2,
                budget_version=budget_version+1,updated_at=?3
             WHERE id=?4 AND parent_task_id IS NULL AND root_task_id=id",
            params![chargeable_token_excess, chargeable_cost_excess, now, task.1],
        )?;
        if root_updated != 1 {
            return Err(DbError::Invalid("ROOT_BUDGET_ACCOUNT_NOT_FOUND".to_owned()));
        }
        conn.execute(
            "UPDATE tasks SET budget_version=budget_version+1,updated_at=?1 WHERE id=?2",
            params![now, task_id],
        )?;
        return Ok(());
    }
    let root_updated = conn.execute(
        "UPDATE tasks SET
            budget_reserved_tokens=budget_reserved_tokens-?1,
            budget_reserved_cost_nanos_usd=budget_reserved_cost_nanos_usd-?2,
            budget_consumed_tokens=budget_consumed_tokens+?3,
            budget_consumed_cost_nanos_usd=budget_consumed_cost_nanos_usd+?4,
            budget_version=budget_version+1,updated_at=?5
         WHERE id=?6 AND budget_reserved_tokens>=?1
           AND budget_reserved_cost_nanos_usd>=?2",
        params![
            reserved_tokens,
            reserved_cost,
            total_used_tokens,
            total_used_cost,
            now,
            task.1
        ],
    )?;
    if root_updated != 1 {
        return Err(DbError::Invalid("ROOT_BUDGET_ACCOUNT_CORRUPT".to_owned()));
    }
    let reservation_updated = conn.execute(
        "UPDATE task_budget_reservations SET used_tokens=?1,used_cost_nanos_usd=?2,
            usage_complete=1,status='settled',settled_at=?3,version=version+1
         WHERE child_task_id=?4 AND status='active' AND version=?5",
        params![
            total_used_tokens,
            total_used_cost,
            now,
            task_id,
            reservation.3
        ],
    )?;
    if reservation_updated != 1 {
        return Err(DbError::Invalid("BUDGET_VERSION_CONFLICT".to_owned()));
    }
    conn.execute(
        "UPDATE tasks SET budget_consumed_tokens=?1,
            budget_consumed_cost_nanos_usd=?2,budget_version=budget_version+1,
            updated_at=?3 WHERE id=?4",
        params![total_used_tokens, total_used_cost, now, task_id],
    )?;
    Ok(())
}

fn map_reservation(row: &Row<'_>) -> rusqlite::Result<TaskBudgetReservationRecord> {
    let status: String = row.get(7)?;
    Ok(TaskBudgetReservationRecord {
        child_task_id: row.get(0)?,
        root_task_id: row.get(1)?,
        reserved_tokens: row.get(2)?,
        reserved_cost_nanos_usd: row.get(3)?,
        used_tokens: row.get(4)?,
        used_cost_nanos_usd: row.get(5)?,
        usage_complete: row.get::<_, i64>(6)? != 0,
        status: BudgetReservationStatus::parse(&status).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        version: row.get(8)?,
        created_at: row.get(9)?,
        settled_at: row.get(10)?,
    })
}

impl Db {
    /// CAS-configures a root account. Shrinking below consumed+reserved or below the
    /// permanent 20% parent reserve invariant fails closed.
    pub async fn configure_root_task_budget_cas(
        &self,
        root_task_id: &str,
        expected_budget_version: i64,
        limits: &TaskBudgetLimits,
    ) -> Result<CasOutcome, DbError> {
        validate_limits(limits)?;
        let root_task_id = root_task_id.to_owned();
        let limits = limits.clone();
        self.with_writer(move |conn| {
            let current: Option<(i64, i64, i64, i64)> = conn
                .query_row(
                    "SELECT budget_version,budget_reserved_tokens,
                            budget_reserved_cost_nanos_usd,status IN
                                ('succeeded','partial','failed','cancelled')
                     FROM tasks WHERE id=?1 AND parent_task_id IS NULL AND root_task_id=id",
                    params![root_task_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let Some((version, reserved_tokens, reserved_cost, terminal)) = current else {
                return Ok(CasOutcome::NotFound);
            };
            if terminal != 0 {
                return Ok(CasOutcome::InvalidTransition);
            }
            if version != expected_budget_version {
                return Ok(CasOutcome::VersionConflict);
            }
            if limits.token_limit.is_some_and(|limit| {
                reserved_tokens
                    > limit.saturating_sub(ceil_percent(limit, ROOT_BUDGET_RESERVE_PERCENT))
            }) || limits.cost_limit_nanos_usd.is_some_and(|limit| {
                reserved_cost
                    > limit.saturating_sub(ceil_percent(limit, ROOT_BUDGET_RESERVE_PERCENT))
            }) {
                return Err(DbError::Invalid("ROOT_PARENT_RESERVE_VIOLATION".to_owned()));
            }
            let now = format_rfc3339_micros(now_millis());
            let updated = conn.execute(
                "UPDATE tasks SET token_budget_limit=?1,cost_budget_nanos_usd=?2,
                    deadline_at_ms=?3,budget_version=budget_version+1,updated_at=?4
                 WHERE id=?5 AND budget_version=?6
                   AND (?1 IS NULL OR
                        budget_consumed_tokens+budget_reserved_tokens<=?1)
                   AND (?2 IS NULL OR
                        budget_consumed_cost_nanos_usd+budget_reserved_cost_nanos_usd<=?2)",
                params![
                    limits.token_limit,
                    limits.cost_limit_nanos_usd,
                    limits.deadline_at_ms,
                    now,
                    root_task_id,
                    expected_budget_version
                ],
            )?;
            Ok(if updated == 1 {
                CasOutcome::Applied
            } else {
                CasOutcome::InvalidTransition
            })
        })
        .await
    }

    pub async fn read_task_budget(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskBudgetSnapshot>, DbError> {
        let task_id = task_id.to_owned();
        self.with_reader(move |conn| {
            let task: Option<(
                String,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                i64,
                i64,
                i64,
                i64,
                bool,
                i64,
            )> = conn
                .query_row(
                    "SELECT target.root_task_id,root.token_budget_limit,
                            root.cost_budget_nanos_usd,root.deadline_at_ms,
                            root.budget_reserved_tokens,
                            root.budget_reserved_cost_nanos_usd,
                            root.budget_consumed_tokens,
                            root.budget_consumed_cost_nanos_usd,
                            root.usage_complete,root.budget_version
                     FROM tasks target JOIN tasks root ON root.id=target.root_task_id
                     WHERE target.id=?1",
                    params![task_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                            row.get::<_, i64>(8)? != 0,
                            row.get(9)?,
                        ))
                    },
                )
                .optional()?;
            let Some(task) = task else {
                return Ok(None);
            };
            let reservation = conn
                .query_row(
                    "SELECT child_task_id,root_task_id,reserved_tokens,
                            reserved_cost_nanos_usd,used_tokens,used_cost_nanos_usd,
                            usage_complete,status,version,created_at,settled_at
                     FROM task_budget_reservations WHERE child_task_id=?1",
                    params![task_id],
                    map_reservation,
                )
                .optional()?;
            Ok(Some(TaskBudgetSnapshot {
                task_id,
                root_task_id: task.0,
                token_limit: task.1,
                cost_limit_nanos_usd: task.2,
                deadline_at_ms: task.3,
                reserved_tokens: task.4,
                reserved_cost_nanos_usd: task.5,
                consumed_tokens: task.6,
                consumed_cost_nanos_usd: task.7,
                available_tokens: task
                    .1
                    .map(|limit| limit.saturating_sub(task.4).saturating_sub(task.6)),
                available_cost_nanos_usd: task
                    .2
                    .map(|limit| limit.saturating_sub(task.5).saturating_sub(task.7)),
                usage_complete: task.8,
                budget_version: task.9,
                reservation,
            }))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CreateTaskWithRun, CreateTaskWithRunOutcome};

    fn id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn root_request(session_id: &str) -> CreateTaskWithRun {
        CreateTaskWithRun {
            task_id: id(),
            run_id: id(),
            root_session_id: session_id.to_owned(),
            transcript_session_id: session_id.to_owned(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "root budget owner".to_owned(),
            prompt: Some("root prompt".to_owned()),
            task_type: "agent".to_owned(),
            model: "test-model".to_owned(),
            working_dir: "/tmp/zk-task-budget".to_owned(),
            execution_config_json: serde_json::json!({
                "budget": {
                    "tokenLimit": 10_000,
                    "costLimitNanosUsd": 10_000_000,
                    "deadlineAtMs": now_millis() + 600_000,
                }
            })
            .to_string(),
            startup_epoch: 1,
        }
    }

    fn child_request(session_id: &str, parent: &CreateTaskWithRunOutcome) -> CreateTaskWithRun {
        CreateTaskWithRun {
            task_id: id(),
            run_id: id(),
            root_session_id: session_id.to_owned(),
            transcript_session_id: id(),
            parent_task_id: Some(parent.task.id.clone()),
            parent_run_id: Some(parent.run_id.clone()),
            creator_tool_use_id: Some(id()),
            ordinal: 0,
            description: "child budget consumer".to_owned(),
            prompt: Some("child prompt".to_owned()),
            task_type: "agent".to_owned(),
            model: "test-model".to_owned(),
            working_dir: "/tmp/zk-task-budget".to_owned(),
            execution_config_json: r#"{"isolation":"readOnly"}"#.to_owned(),
            startup_epoch: 1,
        }
    }

    async fn set_usage_completeness(
        db: &Db,
        root_task_id: &str,
        root_run_id: &str,
        task_complete: bool,
        run_complete: bool,
    ) {
        let root_task_id = root_task_id.to_owned();
        let root_run_id = root_run_id.to_owned();
        db.with_writer(move |conn| {
            assert_eq!(
                conn.execute(
                    "UPDATE tasks SET usage_complete=?1 WHERE id=?2",
                    params![task_complete, root_task_id],
                )?,
                1
            );
            assert_eq!(
                conn.execute(
                    "UPDATE run_envelopes SET usage_complete=?1 WHERE id=?2",
                    params![run_complete, root_run_id],
                )?,
                1
            );
            Ok(())
        })
        .await
        .expect("usage setup");
    }

    async fn assert_child_submission_rejected_without_side_effects(
        db: &Db,
        root: &CreateTaskWithRunOutcome,
        child: &CreateTaskWithRun,
    ) {
        let budget_before = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget before")
            .expect("root budget row");
        let error = db
            .create_task_with_run(child)
            .await
            .expect_err("incomplete usage must reject child submission");
        match error {
            DbError::Invalid(code) => assert_eq!(code, "BUDGET_USAGE_INCOMPLETE"),
            other => panic!("unexpected child submission error: {other}"),
        }

        assert!(
            db.find_runtime_task_by_id(&child.task_id)
                .await
                .expect("child lookup")
                .is_none(),
            "the staged child Task must roll back"
        );
        let child_task_id = child.task_id.clone();
        let child_run_id = child.run_id.clone();
        let child_session_id = child.transcript_session_id.clone();
        let residue = db
            .with_reader(move |conn| {
                let reservations: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM task_budget_reservations WHERE child_task_id=?1",
                    params![child_task_id],
                    |row| row.get(0),
                )?;
                let runs: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM run_envelopes WHERE id=?1",
                    params![child_run_id],
                    |row| row.get(0),
                )?;
                let sessions: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM sessions WHERE id=?1",
                    params![child_session_id],
                    |row| row.get(0),
                )?;
                Ok((reservations, runs, sessions))
            })
            .await
            .expect("submission residue");
        assert_eq!(residue, (0, 0, 0));

        let budget_after = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget after")
            .expect("root budget row");
        assert_eq!(
            budget_after, budget_before,
            "root budget must not be charged"
        );
    }

    #[tokio::test]
    async fn child_submission_rejects_incomplete_root_task_without_side_effects() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/zk-task-budget-root-incomplete")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        set_usage_completeness(&db, &root.task.id, &root.run_id, false, true).await;
        let child = child_request(&session.id, &root);

        assert_child_submission_rejected_without_side_effects(&db, &root, &child).await;
    }

    #[tokio::test]
    async fn child_submission_rejects_incomplete_current_run_without_side_effects() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/zk-task-budget-run-incomplete")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        set_usage_completeness(&db, &root.task.id, &root.run_id, true, false).await;
        let child = child_request(&session.id, &root);

        assert_child_submission_rejected_without_side_effects(&db, &root, &child).await;
    }
}
