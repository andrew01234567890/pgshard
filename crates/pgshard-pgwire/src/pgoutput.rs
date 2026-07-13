//! Bounded zero-copy decoding of `PostgreSQL` 18 logical replication controls.

use std::fmt;

use thiserror::Error;

use crate::{BackendFrame, BackendTag, ClientEncoding, MAX_LARGE_MESSAGE_LENGTH};

const PGOUTPUT_GID_MAX_LENGTH: usize = 199;

/// Maximum complete `pgoutput` message accepted by this decoder.
pub const MAX_PGOUTPUT_MESSAGE_LENGTH: usize = MAX_LARGE_MESSAGE_LENGTH;

/// A `PostgreSQL` 18 `pgoutput` protocol version.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u32)]
pub enum PgOutputVersion {
    /// Base transaction and row-change protocol.
    V1 = 1,
    /// Adds streaming of in-progress transactions.
    V2 = 2,
    /// Adds two-phase transaction decoding.
    V3 = 3,
    /// Adds information needed for parallel stream application.
    V4 = 4,
}

impl PgOutputVersion {
    /// Returns the integer sent as the `proto_version` option.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self as u32
    }
}

impl TryFrom<u32> for PgOutputVersion {
    type Error = PgOutputConfigurationError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            _ => Err(PgOutputConfigurationError::UnsupportedVersion(value)),
        }
    }
}

/// Requested `pgoutput` streaming mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PgOutputStreaming {
    /// Buffer each transaction until its terminal outcome.
    Off,
    /// Stream chunks of in-progress transactions.
    On,
    /// Include protocol-v4 abort information for parallel application.
    Parallel,
}

/// Validated `PostgreSQL` 18 `pgoutput` protocol options.
///
/// This proves only that the option combination is supported. The connection
/// owner must bind it to the exact `START_REPLICATION` command that requested
/// those options, the authoritative persistent `two_phase` state of that
/// command's replication slot, and use it only after the command enters
/// COPY-BOTH mode. `PostgreSQL` uses the logical OR of the requested and
/// persistent slot states; requesting `false` does not disable an enabled slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOutputConfiguration {
    version: PgOutputVersion,
    streaming: PgOutputStreaming,
    requested_two_phase: bool,
    slot_two_phase: bool,
}

impl PgOutputConfiguration {
    /// Validates the negotiated protocol version and feature combination.
    ///
    /// # Errors
    ///
    /// `requested_two_phase` is the accepted `START_REPLICATION` option.
    /// `slot_two_phase` must be the authoritative state of the exact selected
    /// slot. Rejects streaming below protocol v2, parallel streaming below v4,
    /// and a new two-phase request below v3. A previously enabled slot remains
    /// effective even with an older requested protocol.
    pub fn new(
        version: PgOutputVersion,
        streaming: PgOutputStreaming,
        requested_two_phase: bool,
        slot_two_phase: bool,
    ) -> Result<Self, PgOutputConfigurationError> {
        let minimum_streaming_version = match streaming {
            PgOutputStreaming::Off => None,
            PgOutputStreaming::On => Some(PgOutputVersion::V2),
            PgOutputStreaming::Parallel => Some(PgOutputVersion::V4),
        };
        if let Some(minimum) = minimum_streaming_version
            && version < minimum
        {
            return Err(PgOutputConfigurationError::StreamingRequiresVersion {
                streaming,
                minimum,
                actual: version,
            });
        }
        if requested_two_phase && version < PgOutputVersion::V3 {
            return Err(PgOutputConfigurationError::RequestedTwoPhaseRequiresVersion(version));
        }
        Ok(Self {
            version,
            streaming,
            requested_two_phase,
            slot_two_phase,
        })
    }

    /// Returns the negotiated `pgoutput` protocol version.
    #[must_use]
    pub const fn version(self) -> PgOutputVersion {
        self.version
    }

    /// Returns the negotiated transaction-streaming mode.
    #[must_use]
    pub const fn streaming(self) -> PgOutputStreaming {
        self.streaming
    }

    /// Returns whether the accepted command requested two-phase decoding.
    #[must_use]
    pub const fn requested_two_phase(self) -> bool {
        self.requested_two_phase
    }

    /// Returns the authoritative persistent two-phase state of the slot.
    #[must_use]
    pub const fn slot_two_phase(self) -> bool {
        self.slot_two_phase
    }

    /// Returns `PostgreSQL`'s effective two-phase decoding state.
    ///
    /// A slot remains enabled across later starts that request `two_phase`
    /// false, until an explicit slot alteration disables it.
    #[must_use]
    pub const fn two_phase(self) -> bool {
        self.requested_two_phase || self.slot_two_phase
    }

    const fn streaming_enabled(self) -> bool {
        !matches!(self.streaming, PgOutputStreaming::Off)
    }

    const fn parallel_streaming(self) -> bool {
        matches!(self.streaming, PgOutputStreaming::Parallel)
    }
}

/// Invalid `PostgreSQL` 18 `pgoutput` option combination.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PgOutputConfigurationError {
    /// `PostgreSQL` 18 supports only protocol versions one through four.
    #[error("unsupported PostgreSQL 18 pgoutput protocol version {0}")]
    UnsupportedVersion(u32),
    /// The selected streaming mode requires a newer protocol version.
    #[error("pgoutput streaming mode {streaming:?} requires {minimum:?}; received {actual:?}")]
    StreamingRequiresVersion {
        /// Requested streaming mode.
        streaming: PgOutputStreaming,
        /// Earliest protocol supporting that mode.
        minimum: PgOutputVersion,
        /// Requested protocol version.
        actual: PgOutputVersion,
    },
    /// A command requested two-phase decoding below protocol v3.
    #[error("requesting pgoutput two-phase decoding requires protocol v3; received {0:?}")]
    RequestedTwoPhaseRequiresVersion(PgOutputVersion),
}

/// Proof that both sides of a replication connection use canonical `UTF8`.
///
/// `PostgreSQL` bounds prepared-transaction identifiers before converting from
/// `server_encoding` to `client_encoding`. Requiring both authoritative
/// `ParameterStatus` values to be `UTF8` makes that server-side 199-byte bound
/// valid on the wire as well as matching pgshard's database-encoding contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOutputEncoding {
    _private: (),
}

impl PgOutputEncoding {
    /// Combines the existing client-encoding proof with server state.
    ///
    /// `server_encoding` must be the authoritative `ParameterStatus` value from
    /// the same replication connection.
    ///
    /// # Errors
    ///
    /// Returns an error unless the server reports canonical `UTF8`.
    pub fn require_utf8(
        _client_encoding: ClientEncoding,
        server_encoding: &str,
    ) -> Result<Self, PgOutputEncodingError> {
        if server_encoding == "UTF8" {
            Ok(Self { _private: () })
        } else {
            Err(PgOutputEncodingError)
        }
    }
}

/// A replication connection does not satisfy pgshard's UTF-8 contract.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("pgoutput decoding requires canonical server_encoding and client_encoding UTF8")]
pub struct PgOutputEncodingError;

/// One WAL-data envelope carried by backend `CopyData`.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct XLogData<'a> {
    wal_start: u64,
    wal_end: u64,
    server_time: i64,
    data: &'a [u8],
}

impl fmt::Debug for XLogData<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("XLogData")
            .field("wal_start", &self.wal_start)
            .field("wal_end", &self.wal_end)
            .field("server_time", &self.server_time)
            .field("data_length", &self.data.len())
            .finish()
    }
}

impl<'a> XLogData<'a> {
    /// Returns the first WAL position represented by this envelope.
    #[must_use]
    pub const fn wal_start(self) -> u64 {
        self.wal_start
    }

    /// Returns the sender's current end-of-WAL position.
    #[must_use]
    pub const fn wal_end(self) -> u64 {
        self.wal_end
    }

    /// Returns the sender clock in microseconds since `PostgreSQL`'s epoch.
    #[must_use]
    pub const fn server_time(self) -> i64 {
        self.server_time
    }

    /// Returns the borrowed WAL or logical-decoding payload.
    #[must_use]
    pub const fn data(self) -> &'a [u8] {
        self.data
    }
}

/// One primary keepalive carried by backend `CopyData`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrimaryKeepalive {
    wal_end: u64,
    server_time: i64,
    reply_requested: bool,
}

impl PrimaryKeepalive {
    /// Returns the sender's current end-of-WAL position.
    #[must_use]
    pub const fn wal_end(self) -> u64 {
        self.wal_end
    }

    /// Returns the sender clock in microseconds since `PostgreSQL`'s epoch.
    #[must_use]
    pub const fn server_time(self) -> i64 {
        self.server_time
    }

    /// Returns whether the sender requests an immediate status reply.
    #[must_use]
    pub const fn reply_requested(self) -> bool {
        self.reply_requested
    }
}

/// Server-to-client replication payload carried by backend `CopyData`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationCopyData<'a> {
    /// WAL or logical-decoding data.
    XLogData(XLogData<'a>),
    /// Sender keepalive and optional immediate-reply request.
    PrimaryKeepalive(PrimaryKeepalive),
}

/// Decodes one replication payload from a backend `CopyData` frame.
///
/// # Errors
///
/// Rejects another backend tag, an unknown replication payload tag, truncated
/// fixed metadata, a reply-request byte other than zero or one, or trailing
/// keepalive bytes.
pub fn decode_replication_copy_data(
    frame: BackendFrame<'_>,
) -> Result<ReplicationCopyData<'_>, PgOutputError> {
    if frame.tag() != BackendTag::CopyData {
        return Err(PgOutputError::WrongBackendTag(frame.tag()));
    }
    let Some((&tag, body)) = frame.body().split_first() else {
        return Err(PgOutputError::Truncated("replication payload tag"));
    };
    let mut cursor = Cursor::new(body);
    match tag {
        b'w' => {
            let wal_start = cursor.u64("XLogData WAL start")?;
            let wal_end = cursor.u64("XLogData WAL end")?;
            let server_time = cursor.i64("XLogData server time")?;
            Ok(ReplicationCopyData::XLogData(XLogData {
                wal_start,
                wal_end,
                server_time,
                data: cursor.remaining(),
            }))
        }
        b'k' => {
            let wal_end = cursor.u64("keepalive WAL end")?;
            let server_time = cursor.i64("keepalive server time")?;
            let reply_requested = cursor.boolean("keepalive reply request")?;
            cursor.finish()?;
            Ok(ReplicationCopyData::PrimaryKeepalive(PrimaryKeepalive {
                wal_end,
                server_time,
                reply_requested,
            }))
        }
        _ => Err(PgOutputError::UnknownReplicationPayload(tag)),
    }
}

/// Transaction metadata from a `pgoutput` Begin message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOutputBegin {
    final_lsn: u64,
    commit_time: i64,
    xid: u32,
}

impl PgOutputBegin {
    /// Returns the transaction's final LSN.
    #[must_use]
    pub const fn final_lsn(self) -> u64 {
        self.final_lsn
    }

    /// Returns the commit time in microseconds since `PostgreSQL`'s epoch.
    #[must_use]
    pub const fn commit_time(self) -> i64 {
        self.commit_time
    }

    /// Returns the publisher transaction ID.
    #[must_use]
    pub const fn xid(self) -> u32 {
        self.xid
    }
}

/// Terminal metadata shared by Commit and Stream Commit messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOutputCommit {
    commit_lsn: u64,
    end_lsn: u64,
    commit_time: i64,
}

impl PgOutputCommit {
    /// Returns the commit record's LSN.
    #[must_use]
    pub const fn commit_lsn(self) -> u64 {
        self.commit_lsn
    }

    /// Returns the first LSN after the transaction.
    #[must_use]
    pub const fn end_lsn(self) -> u64 {
        self.end_lsn
    }

    /// Returns the commit time in microseconds since `PostgreSQL`'s epoch.
    #[must_use]
    pub const fn commit_time(self) -> i64 {
        self.commit_time
    }
}

/// Replication-origin metadata for the following transaction changes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PgOutputOrigin<'a> {
    origin_lsn: u64,
    name: &'a str,
}

impl fmt::Debug for PgOutputOrigin<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgOutputOrigin")
            .field("origin_lsn", &self.origin_lsn)
            .field("name_length", &self.name.len())
            .finish()
    }
}

impl<'a> PgOutputOrigin<'a> {
    /// Returns the origin-side LSN.
    #[must_use]
    pub const fn origin_lsn(self) -> u64 {
        self.origin_lsn
    }

    /// Returns the borrowed UTF-8 replication-origin name.
    #[must_use]
    pub const fn name(self) -> &'a str {
        self.name
    }
}

/// Metadata shared by prepared-transaction control messages.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PgOutputPrepared<'a> {
    lsn: u64,
    end_lsn: u64,
    timestamp: i64,
    xid: u32,
    gid: &'a str,
}

impl fmt::Debug for PgOutputPrepared<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgOutputPrepared")
            .field("lsn", &self.lsn)
            .field("end_lsn", &self.end_lsn)
            .field("timestamp", &self.timestamp)
            .field("xid", &self.xid)
            .field("gid_length", &self.gid.len())
            .finish()
    }
}

impl<'a> PgOutputPrepared<'a> {
    /// Returns the prepare or commit LSN named by the enclosing message.
    #[must_use]
    pub const fn lsn(self) -> u64 {
        self.lsn
    }

    /// Returns the first LSN after the prepared transaction or outcome.
    #[must_use]
    pub const fn end_lsn(self) -> u64 {
        self.end_lsn
    }

    /// Returns the prepare or commit time in `PostgreSQL` microseconds.
    #[must_use]
    pub const fn timestamp(self) -> i64 {
        self.timestamp
    }

    /// Returns the publisher transaction ID.
    #[must_use]
    pub const fn xid(self) -> u32 {
        self.xid
    }

    /// Returns the borrowed prepared-transaction identifier.
    #[must_use]
    pub const fn gid(self) -> &'a str {
        self.gid
    }
}

/// Metadata from a Rollback Prepared message.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PgOutputRollbackPrepared<'a> {
    prepare_end_lsn: u64,
    rollback_end_lsn: u64,
    prepare_time: i64,
    rollback_time: i64,
    xid: u32,
    gid: &'a str,
}

impl fmt::Debug for PgOutputRollbackPrepared<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgOutputRollbackPrepared")
            .field("prepare_end_lsn", &self.prepare_end_lsn)
            .field("rollback_end_lsn", &self.rollback_end_lsn)
            .field("prepare_time", &self.prepare_time)
            .field("rollback_time", &self.rollback_time)
            .field("xid", &self.xid)
            .field("gid_length", &self.gid.len())
            .finish()
    }
}

impl<'a> PgOutputRollbackPrepared<'a> {
    /// Returns the end LSN recorded when the transaction prepared.
    #[must_use]
    pub const fn prepare_end_lsn(self) -> u64 {
        self.prepare_end_lsn
    }

    /// Returns the end LSN of the rollback outcome.
    #[must_use]
    pub const fn rollback_end_lsn(self) -> u64 {
        self.rollback_end_lsn
    }

    /// Returns the prepare time in `PostgreSQL` microseconds.
    #[must_use]
    pub const fn prepare_time(self) -> i64 {
        self.prepare_time
    }

    /// Returns the rollback time in `PostgreSQL` microseconds.
    #[must_use]
    pub const fn rollback_time(self) -> i64 {
        self.rollback_time
    }

    /// Returns the publisher transaction ID.
    #[must_use]
    pub const fn xid(self) -> u32 {
        self.xid
    }

    /// Returns the borrowed prepared-transaction identifier.
    #[must_use]
    pub const fn gid(self) -> &'a str {
        self.gid
    }
}

/// Metadata from a Stream Abort message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgOutputStreamAbort {
    xid: u32,
    subxid: u32,
    abort_lsn: Option<u64>,
    abort_time: Option<i64>,
}

impl PgOutputStreamAbort {
    /// Returns the top-level publisher transaction ID.
    #[must_use]
    pub const fn xid(self) -> u32 {
        self.xid
    }

    /// Returns the aborted subtransaction ID.
    #[must_use]
    pub const fn subxid(self) -> u32 {
        self.subxid
    }

    /// Returns the abort LSN supplied only by parallel streaming.
    #[must_use]
    pub const fn abort_lsn(self) -> Option<u64> {
        self.abort_lsn
    }

    /// Returns the abort time supplied only by parallel streaming.
    #[must_use]
    pub const fn abort_time(self) -> Option<i64> {
        self.abort_time
    }
}

/// One decoded `PostgreSQL` 18 `pgoutput` transaction/control message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PgOutputControlMessage<'a> {
    /// Starts one buffered transaction.
    Begin(PgOutputBegin),
    /// Commits one buffered transaction.
    Commit(PgOutputCommit),
    /// Identifies the replication origin for following changes.
    Origin(PgOutputOrigin<'a>),
    /// Starts one streamed transaction segment.
    StreamStart {
        /// Publisher transaction ID.
        xid: u32,
        /// Whether this is the transaction's first segment.
        first_segment: bool,
    },
    /// Ends the current streamed segment without deciding its transaction.
    StreamStop,
    /// Commits a previously streamed transaction.
    StreamCommit {
        /// Publisher transaction ID.
        xid: u32,
        /// Commit position and time.
        commit: PgOutputCommit,
    },
    /// Aborts a streamed transaction or subtransaction.
    StreamAbort(PgOutputStreamAbort),
    /// Starts a buffered prepared transaction.
    BeginPrepare(PgOutputPrepared<'a>),
    /// Prepares a buffered two-phase transaction.
    Prepare(PgOutputPrepared<'a>),
    /// Commits an earlier prepared transaction.
    CommitPrepared(PgOutputPrepared<'a>),
    /// Rolls back an earlier prepared transaction.
    RollbackPrepared(PgOutputRollbackPrepared<'a>),
    /// Prepares a previously streamed two-phase transaction.
    StreamPrepare(PgOutputPrepared<'a>),
}

/// Decodes one complete `pgoutput` transaction/control message.
///
/// The message must be the complete logical payload from one replication
/// `XLogData` envelope. This decoder validates message-local layout and enabled
/// protocol features only. Transaction order, LSN monotonicity, relation state,
/// durable acknowledgements, and replay belong to the future stream state
/// machine.
///
/// The combined client/server encoding proof must come from the same
/// replication connection before any protocol string is interpreted as UTF-8.
/// The configuration must likewise be bound to that connection's accepted
/// `START_REPLICATION` command and authoritative persistent slot state.
///
/// # Errors
///
/// Rejects an oversized, empty, row/schema, unknown, truncated, disabled,
/// malformed, non-UTF-8, or trailing-byte message.
pub fn decode_pgoutput_control(
    input: &[u8],
    configuration: PgOutputConfiguration,
    _encoding: PgOutputEncoding,
) -> Result<PgOutputControlMessage<'_>, PgOutputError> {
    if input.len() > MAX_PGOUTPUT_MESSAGE_LENGTH {
        return Err(PgOutputError::MessageTooLarge(input.len()));
    }
    let Some((&tag, body)) = input.split_first() else {
        return Err(PgOutputError::Truncated("pgoutput message tag"));
    };
    let mut cursor = Cursor::new(body);
    let message = match tag {
        b'B' => PgOutputControlMessage::Begin(decode_begin(&mut cursor)?),
        b'C' => PgOutputControlMessage::Commit(decode_commit(&mut cursor, "commit")?),
        b'O' => {
            let origin_lsn = cursor.u64("origin LSN")?;
            let name = cursor.cstring_utf8("replication origin")?;
            PgOutputControlMessage::Origin(PgOutputOrigin { origin_lsn, name })
        }
        b'S' => {
            require_streaming(configuration, tag)?;
            let xid = cursor.u32("stream transaction ID")?;
            let first_segment = cursor.boolean("first stream segment")?;
            PgOutputControlMessage::StreamStart { xid, first_segment }
        }
        b'E' => {
            require_streaming(configuration, tag)?;
            PgOutputControlMessage::StreamStop
        }
        b'c' => {
            require_streaming(configuration, tag)?;
            let xid = cursor.u32("stream transaction ID")?;
            let commit = decode_commit(&mut cursor, "stream commit")?;
            PgOutputControlMessage::StreamCommit { xid, commit }
        }
        b'A' => {
            require_streaming(configuration, tag)?;
            let xid = cursor.u32("stream transaction ID")?;
            let subxid = cursor.u32("stream subtransaction ID")?;
            let (abort_lsn, abort_time) = if configuration.parallel_streaming() {
                (
                    Some(cursor.u64("stream abort LSN")?),
                    Some(cursor.i64("stream abort time")?),
                )
            } else {
                (None, None)
            };
            PgOutputControlMessage::StreamAbort(PgOutputStreamAbort {
                xid,
                subxid,
                abort_lsn,
                abort_time,
            })
        }
        b'b' => {
            require_two_phase(configuration, tag)?;
            PgOutputControlMessage::BeginPrepare(decode_prepared(&mut cursor, false, false)?)
        }
        b'P' => {
            require_two_phase(configuration, tag)?;
            PgOutputControlMessage::Prepare(decode_prepared(&mut cursor, true, true)?)
        }
        b'K' => {
            require_two_phase(configuration, tag)?;
            PgOutputControlMessage::CommitPrepared(decode_prepared(&mut cursor, true, false)?)
        }
        b'r' => {
            require_two_phase(configuration, tag)?;
            PgOutputControlMessage::RollbackPrepared(decode_rollback_prepared(&mut cursor)?)
        }
        b'p' => {
            require_streaming(configuration, tag)?;
            require_two_phase(configuration, tag)?;
            PgOutputControlMessage::StreamPrepare(decode_prepared(&mut cursor, true, true)?)
        }
        b'I' | b'U' | b'D' | b'T' | b'R' | b'Y' | b'M' => {
            return Err(PgOutputError::NonControlMessage(tag));
        }
        _ => return Err(PgOutputError::UnknownPgOutputMessage(tag)),
    };
    cursor.finish()?;
    Ok(message)
}

fn decode_begin(cursor: &mut Cursor<'_>) -> Result<PgOutputBegin, PgOutputError> {
    let final_lsn = cursor.u64("transaction final LSN")?;
    if final_lsn == 0 {
        return Err(PgOutputError::InvalidLsn("transaction final LSN"));
    }
    Ok(PgOutputBegin {
        final_lsn,
        commit_time: cursor.i64("transaction commit time")?,
        xid: cursor.u32("transaction ID")?,
    })
}

fn decode_commit(
    cursor: &mut Cursor<'_>,
    message: &'static str,
) -> Result<PgOutputCommit, PgOutputError> {
    let flags = cursor.byte("commit flags")?;
    if flags != 0 {
        return Err(PgOutputError::InvalidFlags { message, flags });
    }
    Ok(PgOutputCommit {
        commit_lsn: cursor.u64("commit LSN")?,
        end_lsn: cursor.u64("commit end LSN")?,
        commit_time: cursor.i64("commit time")?,
    })
}

fn decode_prepared<'a>(
    cursor: &mut Cursor<'a>,
    has_flags: bool,
    require_xid: bool,
) -> Result<PgOutputPrepared<'a>, PgOutputError> {
    if has_flags {
        let flags = cursor.byte("prepared transaction flags")?;
        if flags != 0 {
            return Err(PgOutputError::InvalidFlags {
                message: "prepared transaction",
                flags,
            });
        }
    }
    let lsn = cursor.u64("prepared transaction LSN")?;
    if lsn == 0 {
        return Err(PgOutputError::InvalidLsn("prepared transaction LSN"));
    }
    let end_lsn = cursor.u64("prepared transaction end LSN")?;
    if end_lsn == 0 {
        return Err(PgOutputError::InvalidLsn("prepared transaction end LSN"));
    }
    let timestamp = cursor.i64("prepared transaction time")?;
    let xid = cursor.u32("prepared transaction ID")?;
    if require_xid && xid == 0 {
        return Err(PgOutputError::InvalidTransactionId(
            "prepared transaction ID",
        ));
    }
    let gid = cursor.gid()?;
    Ok(PgOutputPrepared {
        lsn,
        end_lsn,
        timestamp,
        xid,
        gid,
    })
}

fn decode_rollback_prepared<'a>(
    cursor: &mut Cursor<'a>,
) -> Result<PgOutputRollbackPrepared<'a>, PgOutputError> {
    let flags = cursor.byte("rollback prepared flags")?;
    if flags != 0 {
        return Err(PgOutputError::InvalidFlags {
            message: "rollback prepared",
            flags,
        });
    }
    let prepare_end_lsn = cursor.u64("rollback prepared prepare end LSN")?;
    if prepare_end_lsn == 0 {
        return Err(PgOutputError::InvalidLsn(
            "rollback prepared prepare end LSN",
        ));
    }
    let rollback_end_lsn = cursor.u64("rollback prepared rollback end LSN")?;
    if rollback_end_lsn == 0 {
        return Err(PgOutputError::InvalidLsn(
            "rollback prepared rollback end LSN",
        ));
    }
    Ok(PgOutputRollbackPrepared {
        prepare_end_lsn,
        rollback_end_lsn,
        prepare_time: cursor.i64("rollback prepared prepare time")?,
        rollback_time: cursor.i64("rollback prepared rollback time")?,
        xid: cursor.u32("rollback prepared transaction ID")?,
        gid: cursor.gid()?,
    })
}

fn require_streaming(configuration: PgOutputConfiguration, tag: u8) -> Result<(), PgOutputError> {
    if configuration.streaming_enabled() {
        Ok(())
    } else {
        Err(PgOutputError::StreamingMessageDisabled(tag))
    }
}

fn require_two_phase(configuration: PgOutputConfiguration, tag: u8) -> Result<(), PgOutputError> {
    if configuration.two_phase() {
        Ok(())
    } else {
        Err(PgOutputError::TwoPhaseMessageDisabled(tag))
    }
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn remaining(&self) -> &'a [u8] {
        self.remaining
    }

    fn byte(&mut self, field: &'static str) -> Result<u8, PgOutputError> {
        Ok(self.take(1, field)?[0])
    }

    fn boolean(&mut self, field: &'static str) -> Result<bool, PgOutputError> {
        let value = self.byte(field)?;
        match value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PgOutputError::InvalidBoolean { field, value }),
        }
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, PgOutputError> {
        let bytes = self.take(4, field)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, PgOutputError> {
        let bytes = self.take(8, field)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn i64(&mut self, field: &'static str) -> Result<i64, PgOutputError> {
        let bytes = self.take(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn cstring_utf8(&mut self, field: &'static str) -> Result<&'a str, PgOutputError> {
        let Some(end) = self.remaining.iter().position(|byte| *byte == 0) else {
            return Err(PgOutputError::MissingTerminator(field));
        };
        let value = std::str::from_utf8(&self.remaining[..end])
            .map_err(|_| PgOutputError::InvalidUtf8(field))?;
        self.remaining = &self.remaining[end + 1..];
        Ok(value)
    }

    fn gid(&mut self) -> Result<&'a str, PgOutputError> {
        let gid = self.cstring_utf8("prepared transaction GID")?;
        if gid.len() > PGOUTPUT_GID_MAX_LENGTH {
            return Err(PgOutputError::InvalidGidLength(gid.len()));
        }
        Ok(gid)
    }

    fn take(&mut self, length: usize, field: &'static str) -> Result<&'a [u8], PgOutputError> {
        let value = self
            .remaining
            .get(..length)
            .ok_or(PgOutputError::Truncated(field))?;
        self.remaining = &self.remaining[length..];
        Ok(value)
    }

    fn finish(self) -> Result<(), PgOutputError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(PgOutputError::TrailingData(self.remaining.len()))
        }
    }
}

/// Logical replication envelope or `pgoutput` control decoding failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PgOutputError {
    /// The envelope decoder was called for another backend tag.
    #[error("expected backend CopyData, received {0:?}")]
    WrongBackendTag(BackendTag),
    /// A server-to-client replication payload tag is unknown.
    #[error("unknown replication payload tag {0}")]
    UnknownReplicationPayload(u8),
    /// A complete logical message exceeds the hard pooler ceiling.
    #[error("pgoutput message length {0} exceeds the hard pooler ceiling")]
    MessageTooLarge(usize),
    /// A known row, schema, type, truncate, or custom-message tag needs its
    /// dedicated decoder.
    #[error("pgoutput message tag {0} is not a transaction/control message")]
    NonControlMessage(u8),
    /// The logical message tag is not defined by `PostgreSQL` 18 `pgoutput`.
    #[error("unknown PostgreSQL 18 pgoutput message tag {0}")]
    UnknownPgOutputMessage(u8),
    /// A stream message arrived although streaming was disabled.
    #[error("pgoutput stream message tag {0} arrived while streaming was disabled")]
    StreamingMessageDisabled(u8),
    /// A prepared-transaction message arrived although two-phase was disabled.
    #[error("pgoutput two-phase message tag {0} arrived while two-phase was disabled")]
    TwoPhaseMessageDisabled(u8),
    /// A fixed-width field is missing bytes.
    #[error("{0} is truncated")]
    Truncated(&'static str),
    /// A reserved flags byte is nonzero.
    #[error("unrecognized flags {flags} in {message} message")]
    InvalidFlags {
        /// Message family carrying the flags.
        message: &'static str,
        /// Rejected flags byte.
        flags: u8,
    },
    /// A protocol boolean is neither zero nor one.
    #[error("{field} has invalid boolean value {value}")]
    InvalidBoolean {
        /// Boolean field name.
        field: &'static str,
        /// Rejected byte.
        value: u8,
    },
    /// A required WAL position is `PostgreSQL`'s invalid zero LSN.
    #[error("{0} is not set")]
    InvalidLsn(&'static str),
    /// A required transaction identifier is `PostgreSQL`'s invalid zero XID.
    #[error("{0} is invalid")]
    InvalidTransactionId(&'static str),
    /// A protocol string is missing its zero terminator.
    #[error("{0} is missing its zero terminator")]
    MissingTerminator(&'static str),
    /// A protocol string is not valid under the proven UTF-8 connection encodings.
    #[error("{0} is not valid UTF-8")]
    InvalidUtf8(&'static str),
    /// A prepared-transaction identifier exceeds `PostgreSQL`'s 199-byte bound.
    #[error("prepared transaction GID length {0} exceeds 199 bytes")]
    InvalidGidLength(usize),
    /// Valid fields did not consume the exact message.
    #[error("message has {0} trailing bytes")]
    TrailingData(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_LARGE_MESSAGE_LENGTH, Decode, decode_backend};

    fn utf8() -> PgOutputEncoding {
        PgOutputEncoding::require_utf8(
            ClientEncoding::require_utf8("UTF8").expect("canonical client UTF8"),
            "UTF8",
        )
        .expect("canonical server UTF8")
    }

    fn configuration(
        version: PgOutputVersion,
        streaming: PgOutputStreaming,
        two_phase: bool,
    ) -> PgOutputConfiguration {
        PgOutputConfiguration::new(version, streaming, two_phase, false)
            .expect("valid pgoutput test configuration")
    }

    fn copy_data(body: &[u8]) -> Vec<u8> {
        backend(b'd', body)
    }

    fn backend(tag: u8, body: &[u8]) -> Vec<u8> {
        let length = u32::try_from(4 + body.len()).expect("backend test length");
        let mut packet = Vec::with_capacity(length as usize + 1);
        packet.push(tag);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn backend_frame(input: &[u8]) -> BackendFrame<'_> {
        let Decode::Complete { frame, consumed } =
            decode_backend(input, DEFAULT_LARGE_MESSAGE_LENGTH).expect("backend frame")
        else {
            panic!("complete backend packet was incomplete");
        };
        assert_eq!(consumed, input.len());
        frame
    }

    fn push_u32(bytes: &mut Vec<u8>, value: u32) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn push_u64(bytes: &mut Vec<u8>, value: u64) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn push_i64(bytes: &mut Vec<u8>, value: i64) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn begin() -> Vec<u8> {
        let mut message = vec![b'B'];
        push_u64(&mut message, 11);
        push_i64(&mut message, -22);
        push_u32(&mut message, 33);
        message
    }

    fn commit(tag: u8, xid: Option<u32>) -> Vec<u8> {
        let mut message = vec![tag];
        if let Some(xid) = xid {
            push_u32(&mut message, xid);
        }
        message.push(0);
        push_u64(&mut message, 44);
        push_u64(&mut message, 55);
        push_i64(&mut message, -66);
        message
    }

    fn prepared(tag: u8, flags: bool, gid: &[u8]) -> Vec<u8> {
        let mut message = vec![tag];
        if flags {
            message.push(0);
        }
        push_u64(&mut message, 77);
        push_u64(&mut message, 88);
        push_i64(&mut message, -99);
        push_u32(&mut message, 111);
        message.extend_from_slice(gid);
        message.push(0);
        message
    }

    fn rollback_prepared(gid: &[u8]) -> Vec<u8> {
        let mut message = vec![b'r', 0];
        push_u64(&mut message, 121);
        push_u64(&mut message, 122);
        push_i64(&mut message, -123);
        push_i64(&mut message, -124);
        push_u32(&mut message, 125);
        message.extend_from_slice(gid);
        message.push(0);
        message
    }

    #[test]
    fn configuration_enforces_postgres18_feature_versions() {
        for (raw, expected) in [
            (1, PgOutputVersion::V1),
            (2, PgOutputVersion::V2),
            (3, PgOutputVersion::V3),
            (4, PgOutputVersion::V4),
        ] {
            assert_eq!(PgOutputVersion::try_from(raw), Ok(expected));
            assert_eq!(expected.as_u32(), raw);
        }
        for raw in [0, 5, u32::MAX] {
            assert_eq!(
                PgOutputVersion::try_from(raw),
                Err(PgOutputConfigurationError::UnsupportedVersion(raw))
            );
        }

        for (version, streaming, two_phase) in [
            (PgOutputVersion::V1, PgOutputStreaming::Off, false),
            (PgOutputVersion::V2, PgOutputStreaming::On, false),
            (PgOutputVersion::V3, PgOutputStreaming::On, true),
            (PgOutputVersion::V4, PgOutputStreaming::Parallel, true),
        ] {
            let config = configuration(version, streaming, two_phase);
            assert_eq!(config.version(), version);
            assert_eq!(config.streaming(), streaming);
            assert_eq!(config.requested_two_phase(), two_phase);
            assert!(!config.slot_two_phase());
            assert_eq!(config.two_phase(), two_phase);
        }

        assert!(matches!(
            PgOutputConfiguration::new(PgOutputVersion::V1, PgOutputStreaming::On, false, false),
            Err(PgOutputConfigurationError::StreamingRequiresVersion {
                minimum: PgOutputVersion::V2,
                actual: PgOutputVersion::V1,
                ..
            })
        ));
        assert!(matches!(
            PgOutputConfiguration::new(
                PgOutputVersion::V3,
                PgOutputStreaming::Parallel,
                false,
                false
            ),
            Err(PgOutputConfigurationError::StreamingRequiresVersion {
                minimum: PgOutputVersion::V4,
                actual: PgOutputVersion::V3,
                ..
            })
        ));
        assert_eq!(
            PgOutputConfiguration::new(PgOutputVersion::V2, PgOutputStreaming::Off, true, false),
            Err(PgOutputConfigurationError::RequestedTwoPhaseRequiresVersion(PgOutputVersion::V2))
        );

        let first_start =
            PgOutputConfiguration::new(PgOutputVersion::V3, PgOutputStreaming::Off, true, false)
                .expect("first two-phase request");
        assert!(first_start.requested_two_phase());
        assert!(!first_start.slot_two_phase());
        assert!(first_start.two_phase());

        let restarted =
            PgOutputConfiguration::new(PgOutputVersion::V1, PgOutputStreaming::Off, false, true)
                .expect("persistently enabled slot under a later false request");
        assert!(!restarted.requested_two_phase());
        assert!(restarted.slot_two_phase());
        assert!(restarted.two_phase());
        assert!(matches!(
            decode_pgoutput_control(&prepared(b'P', true, b"gid"), restarted, utf8()),
            Ok(PgOutputControlMessage::Prepare(_))
        ));

        let client = ClientEncoding::require_utf8("UTF8").expect("client UTF8");
        assert_eq!(
            PgOutputEncoding::require_utf8(client, "LATIN1"),
            Err(PgOutputEncodingError)
        );
        assert_eq!(
            PgOutputEncoding::require_utf8(client, "UTF8"),
            Ok(PgOutputEncoding { _private: () })
        );
    }

    #[test]
    fn replication_copy_data_is_exact_and_zero_copy() {
        let mut xlog_body = vec![b'w'];
        push_u64(&mut xlog_body, 101);
        push_u64(&mut xlog_body, 202);
        push_i64(&mut xlog_body, -303);
        xlog_body.extend_from_slice(b"private-wal-data");
        let xlog_packet = copy_data(&xlog_body);
        let ReplicationCopyData::XLogData(xlog) =
            decode_replication_copy_data(backend_frame(&xlog_packet)).expect("XLogData")
        else {
            panic!("decoded XLogData as keepalive");
        };
        assert_eq!(xlog.wal_start(), 101);
        assert_eq!(xlog.wal_end(), 202);
        assert_eq!(xlog.server_time(), -303);
        assert_eq!(xlog.data(), b"private-wal-data");
        assert_eq!(xlog.data().as_ptr(), xlog_packet[5 + 1 + 24..].as_ptr());
        assert!(!format!("{xlog:?}").contains("private"));

        for (reply_byte, expected) in [(0, false), (1, true)] {
            let mut keepalive_body = vec![b'k'];
            push_u64(&mut keepalive_body, 404);
            push_i64(&mut keepalive_body, -505);
            keepalive_body.push(reply_byte);
            let packet = copy_data(&keepalive_body);
            let ReplicationCopyData::PrimaryKeepalive(keepalive) =
                decode_replication_copy_data(backend_frame(&packet)).expect("keepalive")
            else {
                panic!("decoded keepalive as XLogData");
            };
            assert_eq!(keepalive.wal_end(), 404);
            assert_eq!(keepalive.server_time(), -505);
            assert_eq!(keepalive.reply_requested(), expected);
        }
    }

    #[test]
    fn malformed_replication_copy_data_fails_closed() {
        let query_packet = backend(b'C', b"SELECT 1\0");
        assert_eq!(
            decode_replication_copy_data(backend_frame(&query_packet)),
            Err(PgOutputError::WrongBackendTag(BackendTag::CommandComplete))
        );

        for body in [b"".as_slice(), b"x"] {
            let packet = copy_data(body);
            assert!(decode_replication_copy_data(backend_frame(&packet)).is_err());
        }

        let mut xlog_data = vec![b'w'];
        push_u64(&mut xlog_data, 1);
        push_u64(&mut xlog_data, 2);
        push_i64(&mut xlog_data, 3);
        let mut keepalive = vec![b'k'];
        push_u64(&mut keepalive, 4);
        push_i64(&mut keepalive, 5);
        keepalive.push(0);
        for body in [&xlog_data, &keepalive] {
            for length in 0..body.len() {
                let packet = copy_data(&body[..length]);
                assert!(
                    decode_replication_copy_data(backend_frame(&packet)).is_err(),
                    "fixed replication metadata prefix {length} was accepted"
                );
            }
            let packet = copy_data(body);
            assert!(decode_replication_copy_data(backend_frame(&packet)).is_ok());
        }

        let mut invalid_boolean = vec![b'k'];
        push_u64(&mut invalid_boolean, 1);
        push_i64(&mut invalid_boolean, 2);
        invalid_boolean.push(2);
        let packet = copy_data(&invalid_boolean);
        assert_eq!(
            decode_replication_copy_data(backend_frame(&packet)),
            Err(PgOutputError::InvalidBoolean {
                field: "keepalive reply request",
                value: 2,
            })
        );

        invalid_boolean[17] = 1;
        invalid_boolean.push(0);
        let packet = copy_data(&invalid_boolean);
        assert_eq!(
            decode_replication_copy_data(backend_frame(&packet)),
            Err(PgOutputError::TrailingData(1))
        );
    }

    #[test]
    fn buffered_transaction_controls_decode_exactly() {
        let config = configuration(PgOutputVersion::V1, PgOutputStreaming::Off, false);
        let begin_packet = begin();
        let begin = decode_pgoutput_control(&begin_packet, config, utf8()).expect("Begin");
        let PgOutputControlMessage::Begin(begin) = begin else {
            panic!("wrong Begin variant");
        };
        assert_eq!(begin.final_lsn(), 11);
        assert_eq!(begin.commit_time(), -22);
        assert_eq!(begin.xid(), 33);

        let commit_packet = commit(b'C', None);
        let PgOutputControlMessage::Commit(commit) =
            decode_pgoutput_control(&commit_packet, config, utf8()).expect("Commit")
        else {
            panic!("wrong Commit variant");
        };
        assert_eq!(commit.commit_lsn(), 44);
        assert_eq!(commit.end_lsn(), 55);
        assert_eq!(commit.commit_time(), -66);

        let mut origin_packet = vec![b'O'];
        push_u64(&mut origin_packet, 99);
        origin_packet.extend_from_slice(b"private-origin\0");
        let PgOutputControlMessage::Origin(origin) =
            decode_pgoutput_control(&origin_packet, config, utf8()).expect("Origin")
        else {
            panic!("wrong Origin variant");
        };
        assert_eq!(origin.origin_lsn(), 99);
        assert_eq!(origin.name(), "private-origin");
        assert_eq!(origin.name().as_ptr(), origin_packet[9..].as_ptr());
        assert!(!format!("{origin:?}").contains("private"));
    }

    #[test]
    fn streaming_controls_require_the_negotiated_mode() {
        let off = configuration(PgOutputVersion::V4, PgOutputStreaming::Off, true);
        for tag in *b"SEcAp" {
            assert_eq!(
                decode_pgoutput_control(&[tag], off, utf8()),
                Err(PgOutputError::StreamingMessageDisabled(tag))
            );
        }

        let on = configuration(PgOutputVersion::V2, PgOutputStreaming::On, false);
        let start = [b'S', 0, 0, 0, 7, 1];
        assert_eq!(
            decode_pgoutput_control(&start, on, utf8()),
            Ok(PgOutputControlMessage::StreamStart {
                xid: 7,
                first_segment: true,
            })
        );
        assert_eq!(
            decode_pgoutput_control(b"E", on, utf8()),
            Ok(PgOutputControlMessage::StreamStop)
        );

        let stream_commit = commit(b'c', Some(8));
        let PgOutputControlMessage::StreamCommit { xid, commit } =
            decode_pgoutput_control(&stream_commit, on, utf8()).expect("Stream Commit")
        else {
            panic!("wrong Stream Commit variant");
        };
        assert_eq!(xid, 8);
        assert_eq!(commit.commit_lsn(), 44);

        let mut abort = vec![b'A'];
        push_u32(&mut abort, 9);
        push_u32(&mut abort, 10);
        let PgOutputControlMessage::StreamAbort(abort_message) =
            decode_pgoutput_control(&abort, on, utf8()).expect("Stream Abort")
        else {
            panic!("wrong Stream Abort variant");
        };
        assert_eq!(abort_message.xid(), 9);
        assert_eq!(abort_message.subxid(), 10);
        assert_eq!(abort_message.abort_lsn(), None);
        assert_eq!(abort_message.abort_time(), None);

        let parallel = configuration(PgOutputVersion::V4, PgOutputStreaming::Parallel, false);
        push_u64(&mut abort, 11);
        push_i64(&mut abort, -12);
        let PgOutputControlMessage::StreamAbort(abort_message) =
            decode_pgoutput_control(&abort, parallel, utf8()).expect("parallel Stream Abort")
        else {
            panic!("wrong parallel Stream Abort variant");
        };
        assert_eq!(abort_message.abort_lsn(), Some(11));
        assert_eq!(abort_message.abort_time(), Some(-12));
        assert_eq!(
            decode_pgoutput_control(&abort, on, utf8()),
            Err(PgOutputError::TrailingData(16))
        );
    }

    #[test]
    fn two_phase_controls_are_zero_copy_and_feature_gated() {
        let disabled = configuration(PgOutputVersion::V4, PgOutputStreaming::On, false);
        for tag in *b"bPKr" {
            assert_eq!(
                decode_pgoutput_control(&[tag], disabled, utf8()),
                Err(PgOutputError::TwoPhaseMessageDisabled(tag))
            );
        }

        let config = configuration(PgOutputVersion::V4, PgOutputStreaming::On, true);
        for (tag, flags) in [(b'b', false), (b'P', true), (b'K', true), (b'p', true)] {
            let packet = prepared(tag, flags, b"private-gid");
            let message = decode_pgoutput_control(&packet, config, utf8())
                .expect("prepared transaction control");
            let (PgOutputControlMessage::BeginPrepare(prepared)
            | PgOutputControlMessage::Prepare(prepared)
            | PgOutputControlMessage::CommitPrepared(prepared)
            | PgOutputControlMessage::StreamPrepare(prepared)) = message
            else {
                panic!("wrong prepared transaction variant");
            };
            assert_eq!(prepared.lsn(), 77);
            assert_eq!(prepared.end_lsn(), 88);
            assert_eq!(prepared.timestamp(), -99);
            assert_eq!(prepared.xid(), 111);
            assert_eq!(prepared.gid(), "private-gid");
            let gid_offset = 1 + usize::from(flags) + 8 + 8 + 8 + 4;
            assert_eq!(prepared.gid().as_ptr(), packet[gid_offset..].as_ptr());
            assert!(!format!("{message:?}").contains("private"));
        }

        let rollback_packet = rollback_prepared(b"private-rollback-gid");
        let PgOutputControlMessage::RollbackPrepared(rollback) =
            decode_pgoutput_control(&rollback_packet, config, utf8()).expect("Rollback Prepared")
        else {
            panic!("wrong Rollback Prepared variant");
        };
        assert_eq!(rollback.prepare_end_lsn(), 121);
        assert_eq!(rollback.rollback_end_lsn(), 122);
        assert_eq!(rollback.prepare_time(), -123);
        assert_eq!(rollback.rollback_time(), -124);
        assert_eq!(rollback.xid(), 125);
        assert_eq!(rollback.gid(), "private-rollback-gid");
        assert!(!format!("{rollback:?}").contains("private"));
    }

    #[test]
    fn malformed_controls_reject_every_boundary() {
        let base = configuration(PgOutputVersion::V1, PgOutputStreaming::Off, false);
        let streaming = configuration(PgOutputVersion::V2, PgOutputStreaming::On, false);
        let two_phase = configuration(PgOutputVersion::V4, PgOutputStreaming::On, true);
        let parallel = configuration(PgOutputVersion::V4, PgOutputStreaming::Parallel, false);
        let mut stream_abort = vec![b'A'];
        push_u32(&mut stream_abort, 1);
        push_u32(&mut stream_abort, 2);
        let mut parallel_abort = stream_abort.clone();
        push_u64(&mut parallel_abort, 3);
        push_i64(&mut parallel_abort, -4);
        let packets = [
            (begin(), base),
            (commit(b'C', None), base),
            (vec![b'O', 0, 0, 0, 0, 0, 0, 0, 1, b'o', 0], base),
            (vec![b'S', 0, 0, 0, 1, 0], streaming),
            (commit(b'c', Some(1)), streaming),
            (stream_abort, streaming),
            (parallel_abort, parallel),
            (prepared(b'b', false, b"gid"), two_phase),
            (prepared(b'P', true, b"gid"), two_phase),
            (prepared(b'K', true, b"gid"), two_phase),
            (rollback_prepared(b"gid"), two_phase),
            (prepared(b'p', true, b"gid"), two_phase),
        ];
        for (packet, config) in packets {
            for length in 0..packet.len() {
                assert!(
                    decode_pgoutput_control(&packet[..length], config, utf8()).is_err(),
                    "prefix {length} of tag {:?} was accepted",
                    packet.first()
                );
            }
        }

        let mut bad_flags = commit(b'C', None);
        bad_flags[1] = 1;
        assert!(matches!(
            decode_pgoutput_control(&bad_flags, base, utf8()),
            Err(PgOutputError::InvalidFlags { flags: 1, .. })
        ));
        for mut packet in [
            prepared(b'P', true, b"gid"),
            prepared(b'K', true, b"gid"),
            rollback_prepared(b"gid"),
            prepared(b'p', true, b"gid"),
        ] {
            packet[1] = 1;
            assert!(matches!(
                decode_pgoutput_control(&packet, two_phase, utf8()),
                Err(PgOutputError::InvalidFlags { flags: 1, .. })
            ));
        }

        let invalid_boolean = [b'S', 0, 0, 0, 1, 2];
        assert!(matches!(
            decode_pgoutput_control(&invalid_boolean, streaming, utf8()),
            Err(PgOutputError::InvalidBoolean { value: 2, .. })
        ));

        let mut zero_lsn = begin();
        zero_lsn[1..9].fill(0);
        assert_eq!(
            decode_pgoutput_control(&zero_lsn, base, utf8()),
            Err(PgOutputError::InvalidLsn("transaction final LSN"))
        );

        for tag in *b"Pp" {
            let mut zero_xid = prepared(tag, true, b"gid");
            zero_xid[26..30].fill(0);
            assert_eq!(
                decode_pgoutput_control(&zero_xid, two_phase, utf8()),
                Err(PgOutputError::InvalidTransactionId(
                    "prepared transaction ID"
                ))
            );
        }

        let mut trailing = begin();
        trailing.push(0);
        assert_eq!(
            decode_pgoutput_control(&trailing, base, utf8()),
            Err(PgOutputError::TrailingData(1))
        );

        let mut invalid_origin = vec![b'O'];
        push_u64(&mut invalid_origin, 1);
        invalid_origin.extend_from_slice(b"\xff\0");
        assert_eq!(
            decode_pgoutput_control(&invalid_origin, base, utf8()),
            Err(PgOutputError::InvalidUtf8("replication origin"))
        );
    }

    #[test]
    fn prepared_identifiers_are_bounded_utf8_and_redacted() {
        let config = configuration(PgOutputVersion::V3, PgOutputStreaming::Off, true);
        let maximum = vec![b'g'; PGOUTPUT_GID_MAX_LENGTH];
        let packet = prepared(b'P', true, &maximum);
        let PgOutputControlMessage::Prepare(value) =
            decode_pgoutput_control(&packet, config, utf8()).expect("maximum GID")
        else {
            panic!("wrong Prepare variant");
        };
        assert_eq!(value.gid().len(), PGOUTPUT_GID_MAX_LENGTH);

        let overlong = vec![b'g'; PGOUTPUT_GID_MAX_LENGTH + 1];
        let packet = prepared(b'P', true, &overlong);
        assert_eq!(
            decode_pgoutput_control(&packet, config, utf8()),
            Err(PgOutputError::InvalidGidLength(PGOUTPUT_GID_MAX_LENGTH + 1))
        );

        let mut invalid_utf8 = prepared(b'P', true, b"gid");
        let gid_offset = 1 + 1 + 8 + 8 + 8 + 4;
        invalid_utf8[gid_offset] = 0xff;
        assert_eq!(
            decode_pgoutput_control(&invalid_utf8, config, utf8()),
            Err(PgOutputError::InvalidUtf8("prepared transaction GID"))
        );

        let mut unterminated = prepared(b'P', true, b"private-gid");
        unterminated.pop();
        let error =
            decode_pgoutput_control(&unterminated, config, utf8()).expect_err("unterminated GID");
        assert_eq!(
            error,
            PgOutputError::MissingTerminator("prepared transaction GID")
        );
        assert!(!format!("{error:?}").contains("private"));
    }

    #[test]
    fn unknown_and_noncontrol_messages_are_distinct() {
        let config = configuration(PgOutputVersion::V4, PgOutputStreaming::Parallel, true);
        for tag in *b"IUDTRYM" {
            assert_eq!(
                decode_pgoutput_control(&[tag], config, utf8()),
                Err(PgOutputError::NonControlMessage(tag))
            );
        }
        for tag in [0, b'Z', 0xff] {
            assert_eq!(
                decode_pgoutput_control(&[tag], config, utf8()),
                Err(PgOutputError::UnknownPgOutputMessage(tag))
            );
        }
    }
}
