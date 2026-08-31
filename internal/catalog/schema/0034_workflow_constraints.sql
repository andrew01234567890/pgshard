-- Workflows carried a kind and a state as free text, and the table had no
-- index but its primary key. Every reader filters on kind and state and
-- orders by created_at -- the reconciler, the copy poller, the operator's
-- probes and the admin page -- against a table production never prunes, so
-- each of them was a growing sequential scan.
--
-- The value lists are the ones internal/controller declares. A kind or a
-- state outside them is a typo or a version of the controller the catalog
-- has not been told about, and both are better refused by the INSERT than
-- discovered by a reader that silently matches nothing.

ALTER TABLE pgshard.workflows
    ADD CONSTRAINT workflows_kind_is_known
    CHECK (kind IN ('reshard', 'table_placement', 'upgrade'));

ALTER TABLE pgshard.workflows
    ADD CONSTRAINT workflows_state_is_known
    CHECK (state IN ('pending', 'provisioning', 'running', 'paused',
                     'completed', 'failed', 'cancelled'));

-- The shape every hot path asks for: the live workflows of one kind, oldest
-- first. Terminal rows are the ones that accumulate, and leaving them out
-- keeps the index the size of the work in flight rather than of the history.
CREATE INDEX workflows_live_by_kind
    ON pgshard.workflows (kind, created_at)
    WHERE state NOT IN ('completed', 'failed', 'cancelled');

-- The admin page and the operator ask for recent workflows of every kind.
CREATE INDEX workflows_by_created_at ON pgshard.workflows (created_at DESC);
