//! Live `PostgreSQL` 18 positive and known-negative candidate-parser smoke test.

use pgshard_planner::{StatementKind, parse_one};

#[tokio::test]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing to PostgreSQL 18"]
async fn admitted_dml_parses_on_postgres18() {
    let database_url = std::env::var_os("PGSHARD_TEST_DATABASE_URL")
        .expect("PGSHARD_TEST_DATABASE_URL")
        .into_string()
        .expect("PGSHARD_TEST_DATABASE_URL must be UTF-8");
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to PostgreSQL 18");
    let connection_task = tokio::spawn(connection);

    let version = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .expect("read server version")
        .get::<_, i32>(0);
    assert!(
        (180_000..190_000).contains(&version),
        "test requires PostgreSQL 18, received server_version_num={version}"
    );

    client
        .batch_execute(
            "CREATE TEMP TABLE planner_target (
                tenant_id bigint PRIMARY KEY,
                value bigint NOT NULL,
                \"array\" bigint NOT NULL DEFAULT 0
            )",
        )
        .await
        .expect("create planner target");

    for (sql, expected) in [
        (
            "SELECT value FROM planner_target WHERE tenant_id = 1",
            StatementKind::Query,
        ),
        (
            "INSERT INTO planner_target (tenant_id, value) VALUES (1, 2)",
            StatementKind::Insert,
        ),
        (
            "UPDATE planner_target SET value = 2 WHERE tenant_id = 1",
            StatementKind::Update,
        ),
        (
            "DELETE FROM planner_target WHERE tenant_id = 1",
            StatementKind::Delete,
        ),
        (
            "MERGE INTO planner_target AS target
             USING (VALUES (1::bigint, 2::bigint)) AS source (tenant_id, value)
             ON target.tenant_id = source.tenant_id
             WHEN MATCHED THEN UPDATE SET value = source.value
             WHEN NOT MATCHED BY SOURCE THEN DELETE
             WHEN NOT MATCHED THEN
               INSERT (tenant_id, value) VALUES (source.tenant_id, source.value)",
            StatementKind::Merge,
        ),
    ] {
        assert_eq!(parse_one(sql).expect("planner parse").kind(), expected);
        client.prepare(sql).await.expect("PostgreSQL 18 parse");
    }

    let comparisons = vec!["target.array < 1"; 51].join(", ");
    let comparison_sql = format!("SELECT {comparisons} FROM planner_target AS target");
    assert_eq!(
        parse_one(&comparison_sql)
            .expect("independent comparison parse")
            .kind(),
        StatementKind::Query
    );
    client
        .prepare(&comparison_sql)
        .await
        .expect("PostgreSQL 18 independent comparisons");

    for non_postgres_sql in [
        "SELECT TOP 1 * FROM planner_target",
        "INSERT OVERWRITE planner_target VALUES (1, 2)",
        "DELETE FROM planner_target ORDER BY tenant_id LIMIT 1",
    ] {
        parse_one(non_postgres_sql).expect("candidate parser acceptance");
        assert!(
            client.prepare(non_postgres_sql).await.is_err(),
            "PostgreSQL unexpectedly accepted candidate-only syntax"
        );
    }

    drop(client);
    connection_task
        .await
        .expect("join PostgreSQL connection task")
        .expect("PostgreSQL connection");
}
