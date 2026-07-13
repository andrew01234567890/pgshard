//! Live `PostgreSQL` 18 positive and known-negative candidate-parser smoke test.

use pgshard_catalog::{
    CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId, RegisteredTable,
    RoutingHashConfig, ShardKeyType, ShardRoute, TableName,
};
use pgshard_planner::{
    CatalogOnlySearchPath, PhysicalSchemaError, PhysicalShardKeyObservation, PhysicalShardKeyProof,
    RouteTemplateError, StatementKind, parse_one,
};
use pgshard_types::{KEYSPACE_END, KeyRange, RoutingHashV1, ShardId};
use uuid::Uuid;

fn route_snapshot(schema: &str) -> (CatalogSnapshot, DatabaseId) {
    route_snapshot_for(schema, "planner_target")
}

fn route_snapshot_for(schema: &str, table: &str) -> (CatalogSnapshot, DatabaseId) {
    let database_id = DatabaseId::new(Uuid::from_u128(2)).expect("database ID");
    let registered_table = RegisteredTable::new(
        TableName::new(schema, table).expect("temporary table name"),
        "tenant_id",
        ShardKeyType::Int64,
        RoutingHashV1::VERSION,
    )
    .expect("registered table");
    let database = DatabaseCatalog::new(
        database_id,
        "app",
        DatabaseEpochs::new(1, 1, 1).expect("database epochs"),
        vec![ShardRoute::new(
            ShardId(0),
            KeyRange::new(0, KEYSPACE_END).expect("complete range"),
        )],
        vec![registered_table],
    )
    .expect("database catalog");
    let snapshot = CatalogSnapshot::new(
        ClusterId::new(Uuid::from_u128(1)).expect("cluster ID"),
        1,
        RoutingHashConfig::new(1, 42).expect("routing hash"),
        vec![database],
    )
    .expect("catalog snapshot");
    (snapshot, database_id)
}

async fn physical_observation(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> PhysicalShardKeyObservation {
    let row = client
        .query_one(
            "SELECT attributes.atttypid::bigint, attributes.attcollation::bigint, \
                    pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding()) \
               FROM pg_catalog.pg_attribute AS attributes \
               JOIN pg_catalog.pg_class AS relations \
                 ON relations.oid = attributes.attrelid \
               JOIN pg_catalog.pg_namespace AS namespaces \
                 ON namespaces.oid = relations.relnamespace \
              WHERE namespaces.nspname = $1 \
                AND relations.relname = $2 \
                AND attributes.attname = 'tenant_id' \
                AND attributes.attnum > 0 \
                AND NOT attributes.attisdropped",
            &[&schema, &table],
        )
        .await
        .expect("read physical shard-key catalog row");
    let type_oid = u32::try_from(row.get::<_, i64>(0)).expect("type OID fits u32");
    let collation_oid = u32::try_from(row.get::<_, i64>(1)).expect("collation OID fits u32");
    PhysicalShardKeyObservation::new(ShardId(0), 1, type_oid, collation_oid, row.get(2))
}

async fn check_parameter_route(client: &tokio_postgres::Client) {
    let temporary_schema: String = client
        .query_one(
            "SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema()",
            &[],
        )
        .await
        .expect("read temporary schema")
        .get(0);
    let (snapshot, database_id) = route_snapshot(&temporary_schema);
    let routed_sql =
        format!("SELECT * FROM \"{temporary_schema}\".\"planner_target\" WHERE tenant_id = $1");
    let reported_search_path: String = client
        .query_one(
            "SELECT pg_catalog.set_config('search_path', '', false)",
            &[],
        )
        .await
        .expect("pin empty search path")
        .get(0);
    let search_path =
        CatalogOnlySearchPath::require_empty(&reported_search_path).expect("empty search path");
    let statement = client
        .prepare(&routed_sql)
        .await
        .expect("PostgreSQL 18 routed parse");
    let parameter_type_oids = statement
        .params()
        .iter()
        .map(tokio_postgres::types::Type::oid)
        .collect::<Vec<_>>();
    let template = parse_one(&routed_sql)
        .expect("route SQL parse")
        .parameter_route_template(&snapshot, database_id)
        .expect("route template");
    let physical_schema = PhysicalShardKeyProof::verify(
        &snapshot,
        database_id,
        template.table_name(),
        &[physical_observation(client, &temporary_schema, "planner_target").await],
    )
    .expect("physical int8 schema on every active shard");
    let resolved = template
        .resolve_parameter_types(search_path, &physical_schema, &parameter_type_oids)
        .expect("PostgreSQL parameter resolution");
    assert_eq!(resolved.template().parameter_number().get(), 1);
    assert_eq!(resolved.template().shard_key_type(), ShardKeyType::Int64);
    assert_eq!(resolved.parameter_type_oid(), 20);

    let double_equality =
        format!("SELECT * FROM \"{temporary_schema}\".\"planner_target\" WHERE tenant_id == $1");
    assert_eq!(
        parse_one(&double_equality)
            .expect("candidate parser accepts double equality")
            .parameter_route_template(&snapshot, database_id),
        Err(RouteTemplateError::UnsupportedShape),
    );
    assert!(
        client.prepare(&double_equality).await.is_err(),
        "default PostgreSQL catalog unexpectedly resolved double equality"
    );

    check_operator_search_path_reanalysis(client, &routed_sql).await;
    check_coercible_column_rejected(client, &temporary_schema).await;
}

async fn check_coercible_column_rejected(client: &tokio_postgres::Client, schema: &str) {
    client
        .batch_execute(
            "CREATE TEMP TABLE planner_coercion_target (tenant_id double precision PRIMARY KEY); \
             INSERT INTO planner_coercion_target VALUES (9007199254740992::double precision)",
        )
        .await
        .expect("create coercible-column fixture");
    let sql =
        format!("SELECT * FROM \"{schema}\".\"planner_coercion_target\" WHERE tenant_id = $1");
    let statement = client
        .prepare_typed(&sql, &[tokio_postgres::types::Type::INT8])
        .await
        .expect("PostgreSQL accepts explicit bigint against float8 column");
    assert_eq!(
        statement
            .params()
            .iter()
            .map(tokio_postgres::types::Type::oid)
            .collect::<Vec<_>>(),
        vec![20],
        "ParameterDescription must expose only the explicit bigint parameter"
    );
    assert_eq!(
        client
            .query(&statement, &[&9_007_199_254_740_993_i64])
            .await
            .expect("execute coercible-column fixture")
            .len(),
        1,
        "PostgreSQL must demonstrate bigint-to-float8 equality rounding"
    );

    let (snapshot, database_id) = route_snapshot_for(schema, "planner_coercion_target");
    let table_name = TableName::new(schema, "planner_coercion_target").expect("table name");
    assert_eq!(
        PhysicalShardKeyProof::verify(
            &snapshot,
            database_id,
            &table_name,
            &[physical_observation(client, schema, "planner_coercion_target").await],
        ),
        Err(PhysicalSchemaError::TypeMismatch {
            shard_id: ShardId(0),
            expected_oid: 20,
            actual_oid: 701,
        }),
        "a matching parameter OID must not hide a coercible physical column"
    );
}

async fn set_search_path(client: &tokio_postgres::Client, path: &str) {
    let path = path.to_owned();
    let reported: String = client
        .query_one(
            "SELECT pg_catalog.set_config('search_path', $1, false)",
            &[&path],
        )
        .await
        .expect("set and read search path")
        .get(0);
    assert_eq!(reported, path, "PostgreSQL reported another search path");
}

async fn route_row_count(
    client: &tokio_postgres::Client,
    statement: &tokio_postgres::Statement,
) -> usize {
    client
        .query(statement, &[&42_i64])
        .await
        .expect("execute operator route fixture")
        .len()
}

async fn check_operator_search_path_reanalysis(client: &tokio_postgres::Client, routed_sql: &str) {
    // Keep the persistent-schema fixture inside one transaction. Any assertion
    // failure closes the test connection and PostgreSQL rolls the fixture back.
    client
        .batch_execute("BEGIN")
        .await
        .expect("begin operator fixture transaction");
    client
        .execute(
            "INSERT INTO planner_target (tenant_id, value) VALUES (42, 1)",
            &[],
        )
        .await
        .expect("insert route target");
    let backend_pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("read backend PID")
        .get(0);
    let attack_schema = format!("planner_operator_attack_{backend_pid}");
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {attack_schema};
             CREATE FUNCTION {attack_schema}.always_false(bigint, bigint)
             RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS 'SELECT false';
             CREATE OPERATOR {attack_schema}.= (
                 LEFTARG = bigint,
                 RIGHTARG = bigint,
                 FUNCTION = {attack_schema}.always_false
             );"
        ))
        .await
        .expect("install test operator");
    set_search_path(client, &format!("{attack_schema}, pg_catalog")).await;

    let shadowed_statement = client
        .prepare(routed_sql)
        .await
        .expect("prepare with shadowing operator");
    assert_eq!(
        route_row_count(client, &shadowed_statement).await,
        0,
        "test operator must shadow built-in equality under an unsafe search path"
    );

    set_search_path(client, "").await;
    let builtin_statement = client
        .prepare(routed_sql)
        .await
        .expect("prepare with catalog-only operators");
    assert_eq!(route_row_count(client, &builtin_statement).await, 1);

    set_search_path(client, &format!("{attack_schema}, pg_catalog")).await;
    assert_eq!(
        route_row_count(client, &builtin_statement).await,
        0,
        "PostgreSQL must expose why search_path cannot change after Parse"
    );

    set_search_path(client, "").await;
    assert_eq!(
        route_row_count(client, &builtin_statement).await,
        1,
        "cached route statements must execute only under the empty path"
    );
    client
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back operator fixture");
    let fixture_removed: bool = client
        .query_one(
            "SELECT pg_catalog.to_regnamespace($1) IS NULL",
            &[&attack_schema],
        )
        .await
        .expect("verify operator fixture rollback")
        .get(0);
    assert!(fixture_removed, "operator fixture schema survived rollback");
}

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

    check_parameter_route(&client).await;

    drop(client);
    connection_task
        .await
        .expect("join PostgreSQL connection task")
        .expect("PostgreSQL connection");
}
