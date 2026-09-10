-- Views a router can route.
--
-- The planner has no view expansion, so a view used to be an undeclared
-- relation that fell to the database default placement: a view over a
-- sharded table was created on every shard and then read from ONE of them,
-- with no error. It is refused outright today. This table is what lets it be
-- ROUTED instead: the base relation it projects, and the map from each of
-- its output columns to the base column behind it.
--
-- The map is the load-bearing part. With it the planner recognises a shard
-- key that the view exposes under another name -- WHERE tenant = $1 over a
-- view whose "tenant" is the base table's tenant_id -- and routes the
-- statement to one shard while still sending SQL that targets the VIEW, so
-- PostgreSQL keeps evaluating the view's own defaults, privileges and RLS
-- rather than pgshard reimplementing them.
--
-- Only the shape a versioned-schema migration tool produces is recorded:
-- ONE base relation, direct column projections. Anything else is 'opaque',
-- which routes nowhere and is refused rather than guessed at.
--
-- This is system-owned. A view is recorded by the applier from the statement
-- the client already ran; it is not a desired-state row anybody declares.

SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.views (
    database     text        NOT NULL REFERENCES pgshard.databases (name) ON DELETE CASCADE,
    schema_name  text        NOT NULL,
    view_name    text        NOT NULL,
    -- The single relation the view projects. Empty for an opaque view.
    base_schema  text        NOT NULL DEFAULT '',
    base_name    text        NOT NULL DEFAULT '',
    -- shape 'simple' is one base relation and direct column projections, so
    -- the column map below is complete and the view can be routed by it.
    -- 'opaque' is everything else: recorded so the router knows the relation
    -- IS a view and refuses it, rather than mistaking it for a table.
    shape        text        NOT NULL CHECK (shape IN ('simple', 'opaque')),
    -- output column -> base column. Empty for an opaque view.
    columns      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema_name, view_name),
    CONSTRAINT simple_views_name_their_base
        CHECK (shape <> 'simple' OR (base_name <> '' AND columns <> '{}'::jsonb))
);

GRANT SELECT ON pgshard.views TO pgshard_reader;

RESET ROLE;
