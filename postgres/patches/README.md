# PostgreSQL patch series

Each major version has its own directory (`18/`, `19/`, ...) containing a `series` file
and one file per patch. During the image build every non-blank, non-comment line of
`series` is applied in order from the PostgreSQL source root with `patch -p1`.

## Policy

- Patches are a last resort. A patch is added only after an accepted design decision
  documents why the behaviour cannot be achieved with unmodified PostgreSQL
  (extensions, hooks, background workers, configuration, or the wire protocol).
- Upstream first: every patch must be proposed to pgsql-hackers before or at the same
  time it is added here, and its status is tracked until it is merged or rejected.
- Patches are per major version. A change needed on several majors is a separate file
  in each major's directory, rebased for that branch.
- Every patch file starts with a header describing its purpose, the design decision that
  requires it, and its upstream status (thread link, commitfest entry, or rejection).
- When upstream ships the change, the patch is removed from the series for that major.
