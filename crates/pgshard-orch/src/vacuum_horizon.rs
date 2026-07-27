//! Continuous visibility for the vacuum horizon a physical slot pins.
//!
//! A physical replication slot fed by `hot_standby_feedback` stores the
//! standby's reported `xmin` in the slot itself, not merely in the walsender's
//! `PGPROC`, and `PostgreSQL` releases it only on an explicit drop or an
//! invalidation. Horizon invalidation cannot reach it, because that cause is
//! gated on the slot being logical. A standby that is down, paused, or
//! partitioned therefore holds the primary's data horizon still while the
//! transaction counter keeps moving, and nothing in the public catalog says so
//! until vacuum has already stopped making progress.
//!
//! Two separate questions decide whether that has happened, and a single
//! sample answers only the first. How far the pin has fallen behind is the
//! stored `xmin`'s age. Whether the pin has stopped moving is not: a walsender
//! detaches for a moment on every standby restart, network blip, and walsender
//! crash, and the `xmin` left behind can already be old, because a standby
//! catching up from a backlog was measured reporting age 52,842,368 while
//! streaming and perfectly healthy. Classifying that first detached sample
//! from its age alone reports a frozen horizon on a routine restart.
//!
//! [`VacuumHorizonWatch`] therefore folds successive samples of one slot. Only
//! the walsender writes a physical slot's `xmin`, so a detached slot whose
//! stored `xmin` is unchanged has not moved, and the transaction IDs the
//! primary consumed between the first detached sample and the current one
//! measure the detachment rather than the pin. The watch reads primary-local
//! slot samples and never mutates a slot, a setting, or feedback.

use std::num::NonZeroU32;

use crate::{
    slot_observer::{
        LocalPhysicalReplicationSlotObservation, LocalPostgresTransactionId, PinnedDataHorizon,
    },
    standby_slots::{ReplicationSlotName, SlotActivity, SlotInvalidation},
};

/// Transaction-ID age at which `PostgreSQL` forces anti-wraparound autovacuum.
///
/// This is the `autovacuum_freeze_max_age` default. Reaching it while a slot
/// pins the horizon is already a failure: every table qualifies for an
/// anti-wraparound vacuum that cannot advance `relfrozenxid` past the pin, so
/// the cluster burns I/O without moving the limit.
pub const ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE: u32 = 200_000_000;

/// Transaction-ID age at which a pinned data horizon becomes reportable.
///
/// One quarter of [`ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE`], which leaves three
/// quarters of that budget to drop the slot or re-seed the standby before
/// `PostgreSQL` starts the futile anti-wraparound work.
///
/// This bounds how far behind the pin is, and nothing else. A healthy standby
/// can carry an `xmin` this old while streaming, so age alone never justifies
/// a report; see [`FROZEN_DATA_HORIZON_CORROBORATION_XIDS`].
pub const FROZEN_DATA_HORIZON_XID_AGE: u32 = ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE / 4;

/// Transaction IDs a detachment must outlast before the horizon counts frozen.
///
/// The walsender is the only writer of a physical slot's `xmin`, so between two
/// detached samples that report the same `xmin` the pin provably did not move,
/// and the transaction IDs consumed in between are the length of the
/// detachment. Requiring that length separates a standby restart, which
/// reattaches and resumes reporting, from a horizon that has genuinely stopped.
///
/// One twentieth of [`ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE`]. Counting the wait
/// in transaction IDs rather than seconds bounds what corroboration costs in
/// the currency wraparound is measured in: it spends at most this much of the
/// remediation budget however fast the primary commits, and a primary that
/// commits nothing spends none of it and is in no danger of wrapping around.
pub const FROZEN_DATA_HORIZON_CORROBORATION_XIDS: u32 = ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE / 20;

/// Why a physical slot's stored data horizon is or is not reportable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VacuumHorizonState {
    /// The slot stores no `xmin` and pins no data horizon.
    Unpinned,
    /// `PostgreSQL` already stopped honouring this slot's horizon.
    ///
    /// An invalidated slot keeps reporting its last `xmin` in the public view
    /// even though the required-xmin computation skips it, so the stored value
    /// is stale evidence rather than a live pin.
    Invalidated {
        /// `PostgreSQL`'s reason for invalidating the slot.
        cause: SlotInvalidation,
    },
    /// A walsender owns the slot and can still advance the horizon.
    ///
    /// Feedback arrives no less often than `wal_receiver_status_interval`, so
    /// an owned slot is lagging rather than frozen however old its `xmin` is.
    /// A long-running standby query legitimately holds an old horizon here.
    Attached {
        /// Backend currently owning the slot.
        pid: NonZeroU32,
        /// `PostgreSQL`'s `age(xmin)` for the stored horizon.
        age: u32,
    },
    /// No walsender owns the slot, so nothing can advance the stored horizon.
    ///
    /// The age says how far behind the pin is; `detached_xids` says how long
    /// the slot has been unowned, counted in transaction IDs the primary
    /// consumed since the detachment was first sampled. Both bounds have to be
    /// met before the horizon is reportable, because a momentary detach carries
    /// whatever age the pin already had.
    Detached {
        /// `PostgreSQL`'s `age(xmin)` for the stored horizon.
        age: u32,
        /// Age at which a corroborated detachment becomes reportable.
        threshold: u32,
        /// Transaction IDs consumed since this detachment was first sampled.
        detached_xids: u32,
        /// Detachment length required before the horizon counts as frozen.
        corroboration_xids: u32,
    },
}

/// One evaluation of the data horizon a named physical slot pins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VacuumHorizonCondition {
    slot: ReplicationSlotName,
    state: VacuumHorizonState,
}

impl VacuumHorizonCondition {
    /// Returns the slot this condition describes.
    #[must_use]
    pub const fn slot(&self) -> &ReplicationSlotName {
        &self.slot
    }

    /// Returns the classified horizon state.
    #[must_use]
    pub const fn state(&self) -> VacuumHorizonState {
        self.state
    }

    /// Returns whether the horizon is frozen past both reportable bounds.
    #[must_use]
    pub const fn is_frozen(&self) -> bool {
        matches!(
            self.state,
            VacuumHorizonState::Detached {
                age,
                threshold,
                detached_xids,
                corroboration_xids,
            } if age >= threshold && detached_xids >= corroboration_xids
        )
    }

    /// Returns a stable machine-readable reason for the current state.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self.state {
            VacuumHorizonState::Unpinned => "HorizonUnpinned",
            VacuumHorizonState::Invalidated { .. } => "SlotInvalidated",
            VacuumHorizonState::Attached { .. } => "HorizonReportedByWalSender",
            VacuumHorizonState::Detached {
                age, threshold: t, ..
            } if age < t => "HorizonUnreportedWithinBudget",
            VacuumHorizonState::Detached {
                detached_xids,
                corroboration_xids,
                ..
            } if detached_xids < corroboration_xids => "HorizonDetachmentUncorroborated",
            VacuumHorizonState::Detached { .. } => "VacuumHorizonFrozen",
        }
    }

    /// Returns `PostgreSQL`'s `age(xmin)` for the stored horizon, if any.
    #[must_use]
    pub const fn pinned_age(&self) -> Option<u32> {
        match self.state {
            VacuumHorizonState::Unpinned | VacuumHorizonState::Invalidated { .. } => None,
            VacuumHorizonState::Attached { age, .. } | VacuumHorizonState::Detached { age, .. } => {
                Some(age)
            }
        }
    }

    /// Returns the transaction IDs left before forced anti-wraparound vacuum.
    ///
    /// Zero means `PostgreSQL` is already running anti-wraparound vacuums that
    /// this slot prevents from advancing the frozen limit.
    #[must_use]
    pub const fn remediation_budget_xids(&self) -> Option<u32> {
        match self.pinned_age() {
            Some(age) => Some(ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE.saturating_sub(age)),
            None => None,
        }
    }

    /// Returns an operator-facing description of the current state.
    #[must_use]
    pub fn message(&self) -> String {
        let slot = self.slot.as_str();
        match self.state {
            VacuumHorizonState::Unpinned => {
                format!("physical slot {slot} pins no data horizon")
            }
            VacuumHorizonState::Invalidated { cause } => format!(
                "physical slot {slot} is invalidated ({}); its reported xmin no longer \
                 constrains vacuum",
                invalidation_name(cause)
            ),
            VacuumHorizonState::Attached { pid, age } => format!(
                "physical slot {slot} pins data horizon age {age} and is owned by walsender \
                 pid {pid}, which can still advance it"
            ),
            VacuumHorizonState::Detached { age, threshold, .. } if age < threshold => format!(
                "physical slot {slot} has no walsender and pins data horizon age {age}, below \
                 the reportable age {threshold}"
            ),
            VacuumHorizonState::Detached {
                age,
                detached_xids,
                corroboration_xids,
                ..
            } if detached_xids < corroboration_xids => format!(
                "physical slot {slot} has no walsender and pins data horizon age {age}, but only \
                 {detached_xids} of the {corroboration_xids} transaction IDs that tell a \
                 walsender restart from a frozen horizon have passed since it detached"
            ),
            VacuumHorizonState::Detached {
                age,
                threshold,
                detached_xids,
                ..
            } => format!(
                "physical slot {slot} kept data horizon age {age} unchanged across \
                 {detached_xids} transaction IDs with no walsender (reportable at age \
                 {threshold}); {} transaction IDs remain before anti-wraparound autovacuum \
                 cannot advance the frozen limit; drop the slot or restore the standby",
                ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE.saturating_sub(age)
            ),
        }
    }
}

/// Successive samples of one physical slot, folded into a horizon condition.
///
/// A consumer holds one watch per slot and feeds it every sample it takes of
/// that slot, in order. The watch carries only what the next classification
/// needs: which slot was detached, the `xmin` it was detached on, and the age
/// that `xmin` had when the detachment was first seen. Every attached,
/// unpinned, or invalidated sample clears that record, as does an `xmin` or a
/// slot name that differs from the one being tracked, so a detachment has to be
/// continuous to accumulate.
///
/// A watch that starts on an already-detached slot cannot know when the
/// detachment began and counts from its own first sample, which delays the
/// report by at most [`FROZEN_DATA_HORIZON_CORROBORATION_XIDS`] transaction
/// IDs. Dropping samples has the same effect. Both err towards silence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VacuumHorizonWatch {
    threshold: u32,
    corroboration_xids: u32,
    detachment: Option<ObservedDetachment>,
}

impl Default for VacuumHorizonWatch {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumHorizonWatch {
    /// Creates a watch at the shipped reportable age and corroboration length.
    #[must_use]
    pub const fn new() -> Self {
        Self::with_limits(
            FROZEN_DATA_HORIZON_XID_AGE,
            FROZEN_DATA_HORIZON_CORROBORATION_XIDS,
        )
    }

    /// Creates a watch at a caller-chosen reportable age and corroboration
    /// length.
    #[must_use]
    pub const fn with_limits(threshold: u32, corroboration_xids: u32) -> Self {
        Self {
            threshold,
            corroboration_xids,
            detachment: None,
        }
    }

    /// Returns the age at which a corroborated detachment becomes reportable.
    #[must_use]
    pub const fn threshold(&self) -> u32 {
        self.threshold
    }

    /// Returns the detachment length required before reporting.
    #[must_use]
    pub const fn corroboration_xids(&self) -> u32 {
        self.corroboration_xids
    }

    /// Folds one primary-local slot sample into the horizon condition.
    pub fn observe(
        &mut self,
        slot: &LocalPhysicalReplicationSlotObservation,
    ) -> VacuumHorizonCondition {
        VacuumHorizonCondition {
            slot: slot.name().clone(),
            state: self.classify(slot),
        }
    }

    fn classify(&mut self, slot: &LocalPhysicalReplicationSlotObservation) -> VacuumHorizonState {
        if let Some(cause) = slot.invalidation() {
            self.detachment = None;
            return VacuumHorizonState::Invalidated { cause };
        }
        let Some(horizon) = slot.data_horizon() else {
            self.detachment = None;
            return VacuumHorizonState::Unpinned;
        };
        let age = horizon.age();
        if let SlotActivity::Active(pid) = slot.activity() {
            self.detachment = None;
            return VacuumHorizonState::Attached { pid, age };
        }
        VacuumHorizonState::Detached {
            age,
            threshold: self.threshold,
            detached_xids: age.saturating_sub(self.detached_since(slot.name(), horizon)),
            corroboration_xids: self.corroboration_xids,
        }
    }

    fn detached_since(&mut self, slot: &ReplicationSlotName, horizon: PinnedDataHorizon) -> u32 {
        match &self.detachment {
            Some(prior) if prior.slot == *slot && prior.xmin == horizon.xmin() => prior.first_age,
            _ => {
                self.detachment = Some(ObservedDetachment {
                    slot: slot.clone(),
                    xmin: horizon.xmin(),
                    first_age: horizon.age(),
                });
                horizon.age()
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedDetachment {
    slot: ReplicationSlotName,
    xmin: LocalPostgresTransactionId,
    first_age: u32,
}

const fn invalidation_name(cause: SlotInvalidation) -> &'static str {
    match cause {
        SlotInvalidation::WalRemoved => "wal_removed",
        SlotInvalidation::RowsRemoved => "rows_removed",
        SlotInvalidation::WalLevelInsufficient => "wal_level_insufficient",
        SlotInvalidation::IdleTimeout => "idle_timeout",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{slot_observer::physical_slot_fixture, standby_slots::SlotPersistence};

    const SLOT: &str = "pgshard_member_0001";

    fn named_slot(
        name: &str,
        activity: SlotActivity,
        horizon: Option<(u32, u32)>,
        invalidation: Option<SlotInvalidation>,
    ) -> LocalPhysicalReplicationSlotObservation {
        physical_slot_fixture(
            ReplicationSlotName::new(name).expect("slot name"),
            SlotPersistence::Unproven,
            activity,
            horizon.map(|(xmin, age)| (NonZeroU32::new(xmin).expect("xmin"), age)),
            invalidation,
        )
    }

    fn slot(
        activity: SlotActivity,
        horizon: Option<(u32, u32)>,
        invalidation: Option<SlotInvalidation>,
    ) -> LocalPhysicalReplicationSlotObservation {
        named_slot(SLOT, activity, horizon, invalidation)
    }

    fn pid(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("pid")
    }

    #[test]
    fn reportable_age_is_one_quarter_of_forced_anti_wraparound_vacuum() {
        assert_eq!(ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE, 200_000_000);
        assert_eq!(FROZEN_DATA_HORIZON_XID_AGE, 50_000_000);
        assert_eq!(
            ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE - FROZEN_DATA_HORIZON_XID_AGE,
            150_000_000
        );
    }

    #[test]
    fn corroboration_spends_one_twentieth_of_the_anti_wraparound_budget() {
        assert_eq!(FROZEN_DATA_HORIZON_CORROBORATION_XIDS, 10_000_000);
        assert_eq!(
            VacuumHorizonWatch::new().corroboration_xids(),
            FROZEN_DATA_HORIZON_CORROBORATION_XIDS
        );
        assert_eq!(
            VacuumHorizonWatch::new().threshold(),
            FROZEN_DATA_HORIZON_XID_AGE
        );
        assert_eq!(VacuumHorizonWatch::default(), VacuumHorizonWatch::new());
    }

    #[test]
    fn a_detachment_sustained_past_both_bounds_is_frozen() {
        let mut watch = VacuumHorizonWatch::new();
        let first = watch.observe(&slot(SlotActivity::Inactive, Some((754, 60_200_013)), None));
        assert!(!first.is_frozen());

        let condition = watch.observe(&slot(
            SlotActivity::Inactive,
            Some((754, 60_200_013 + FROZEN_DATA_HORIZON_CORROBORATION_XIDS)),
            None,
        ));
        assert!(condition.is_frozen());
        assert_eq!(condition.reason(), "VacuumHorizonFrozen");
        assert_eq!(
            condition.pinned_age(),
            Some(70_200_013),
            "the age is the pin's, not the detachment's"
        );
        assert_eq!(condition.remediation_budget_xids(), Some(129_799_987));
        assert_eq!(
            condition.state(),
            VacuumHorizonState::Detached {
                age: 70_200_013,
                threshold: FROZEN_DATA_HORIZON_XID_AGE,
                detached_xids: FROZEN_DATA_HORIZON_CORROBORATION_XIDS,
                corroboration_xids: FROZEN_DATA_HORIZON_CORROBORATION_XIDS,
            }
        );
        assert!(condition.message().contains("no walsender"));
        assert!(condition.message().contains("129799987"));
    }

    #[test]
    fn the_first_detached_sample_of_an_already_old_pin_is_never_frozen() {
        let mut watch = VacuumHorizonWatch::new();
        let condition = watch.observe(&slot(
            SlotActivity::Inactive,
            Some((754, ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE)),
            None,
        ));
        assert!(!condition.is_frozen());
        assert_eq!(condition.reason(), "HorizonDetachmentUncorroborated");
        assert_eq!(
            condition.state(),
            VacuumHorizonState::Detached {
                age: ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE,
                threshold: FROZEN_DATA_HORIZON_XID_AGE,
                detached_xids: 0,
                corroboration_xids: FROZEN_DATA_HORIZON_CORROBORATION_XIDS,
            }
        );
        assert!(condition.message().contains("0 of the 10000000"));
    }

    #[test]
    fn a_walsender_restart_across_an_already_old_pin_never_fires() {
        let mut watch = VacuumHorizonWatch::new();
        let old = 52_842_368;
        assert!(old > FROZEN_DATA_HORIZON_XID_AGE);

        for blip in 0..8_u32 {
            // A blip detaches the walsender for far less than the
            // corroboration length, so the aged pin it leaves behind stays
            // unreported, and reattachment forgets the detachment entirely.
            let burned = blip * 400_000;
            let attached = watch.observe(&slot(
                SlotActivity::Active(pid(2_443_170 + blip)),
                Some((754, old + burned)),
                None,
            ));
            assert!(!attached.is_frozen());
            assert_eq!(attached.reason(), "HorizonReportedByWalSender");

            let detached = watch.observe(&slot(
                SlotActivity::Inactive,
                Some((754, old + burned + 300_000)),
                None,
            ));
            assert!(!detached.is_frozen(), "blip {blip} must stay silent");
            assert_eq!(detached.reason(), "HorizonDetachmentUncorroborated");
        }
    }

    #[test]
    fn reattachment_restarts_the_corroboration_from_zero() {
        let mut watch = VacuumHorizonWatch::with_limits(0, 100);
        assert!(
            !watch
                .observe(&slot(SlotActivity::Inactive, Some((754, 1_000)), None))
                .is_frozen()
        );
        assert!(
            watch
                .observe(&slot(SlotActivity::Inactive, Some((754, 1_100)), None))
                .is_frozen()
        );
        assert_eq!(
            watch
                .observe(&slot(
                    SlotActivity::Active(pid(4_242)),
                    Some((754, 1_100)),
                    None
                ))
                .reason(),
            "HorizonReportedByWalSender"
        );
        let after = watch.observe(&slot(SlotActivity::Inactive, Some((754, 1_100)), None));
        assert!(!after.is_frozen());
        assert_eq!(after.reason(), "HorizonDetachmentUncorroborated");
    }

    #[test]
    fn a_new_xmin_restarts_the_corroboration_from_zero() {
        let mut watch = VacuumHorizonWatch::with_limits(0, 100);
        assert!(
            !watch
                .observe(&slot(SlotActivity::Inactive, Some((754, 1_000)), None))
                .is_frozen()
        );
        let moved = watch.observe(&slot(SlotActivity::Inactive, Some((900, 1_100)), None));
        assert!(!moved.is_frozen());
        assert_eq!(moved.reason(), "HorizonDetachmentUncorroborated");
        assert!(
            watch
                .observe(&slot(SlotActivity::Inactive, Some((900, 1_200)), None))
                .is_frozen()
        );
    }

    #[test]
    fn a_different_slot_never_corroborates_another_slots_detachment() {
        let mut watch = VacuumHorizonWatch::with_limits(0, 100);
        watch.observe(&named_slot(
            "pgshard_member_0001",
            SlotActivity::Inactive,
            Some((754, 1_000)),
            None,
        ));
        let other = watch.observe(&named_slot(
            "pgshard_member_0002",
            SlotActivity::Inactive,
            Some((754, 1_100)),
            None,
        ));
        assert!(!other.is_frozen());
        assert_eq!(other.slot().as_str(), "pgshard_member_0002");
        assert_eq!(other.reason(), "HorizonDetachmentUncorroborated");
    }

    #[test]
    fn an_owned_slot_is_never_frozen_however_old_its_horizon_is() {
        let mut watch = VacuumHorizonWatch::new();
        let mut condition = watch.observe(&slot(
            SlotActivity::Active(pid(2_443_170)),
            Some((754, ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE * 2)),
            None,
        ));
        for _ in 0..4 {
            assert!(!condition.is_frozen());
            assert_eq!(condition.reason(), "HorizonReportedByWalSender");
            condition = watch.observe(&slot(
                SlotActivity::Active(pid(2_443_170)),
                Some((754, ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE * 2)),
                None,
            ));
        }
        assert_eq!(
            condition.state(),
            VacuumHorizonState::Attached {
                pid: pid(2_443_170),
                age: 400_000_000,
            }
        );
        assert_eq!(condition.remediation_budget_xids(), Some(0));
        assert!(condition.message().contains("pid 2443170"));
    }

    #[test]
    fn a_corroborated_detachment_below_the_reportable_age_is_not_frozen() {
        let mut watch = VacuumHorizonWatch::new();
        watch.observe(&slot(SlotActivity::Inactive, Some((754, 0)), None));
        let condition = watch.observe(&slot(
            SlotActivity::Inactive,
            Some((754, FROZEN_DATA_HORIZON_XID_AGE - 1)),
            None,
        ));
        assert!(!condition.is_frozen());
        assert_eq!(condition.reason(), "HorizonUnreportedWithinBudget");
        assert_eq!(
            condition.pinned_age(),
            Some(FROZEN_DATA_HORIZON_XID_AGE - 1)
        );
        assert!(condition.message().contains("below the reportable age"));
    }

    #[test]
    fn the_reportable_age_itself_is_frozen_once_corroborated() {
        let mut watch = VacuumHorizonWatch::with_limits(FROZEN_DATA_HORIZON_XID_AGE, 0);
        let condition = watch.observe(&slot(
            SlotActivity::Inactive,
            Some((754, FROZEN_DATA_HORIZON_XID_AGE)),
            None,
        ));
        assert!(condition.is_frozen());
        assert_eq!(condition.reason(), "VacuumHorizonFrozen");
        assert_eq!(
            condition.remediation_budget_xids(),
            Some(ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE - FROZEN_DATA_HORIZON_XID_AGE)
        );
        assert!(condition.message().contains("unchanged across"));
        assert!(!condition.message().contains("below the reportable age"));
    }

    #[test]
    fn a_detached_slot_without_a_stored_horizon_is_unpinned() {
        let mut watch = VacuumHorizonWatch::new();
        let condition = watch.observe(&slot(SlotActivity::Inactive, None, None));
        assert!(!condition.is_frozen());
        assert_eq!(condition.reason(), "HorizonUnpinned");
        assert_eq!(condition.pinned_age(), None);
        assert_eq!(condition.remediation_budget_xids(), None);
        assert_eq!(condition.state(), VacuumHorizonState::Unpinned);
    }

    #[test]
    fn an_unpinned_sample_forgets_a_running_corroboration() {
        let mut watch = VacuumHorizonWatch::with_limits(0, 100);
        watch.observe(&slot(SlotActivity::Inactive, Some((754, 1_000)), None));
        watch.observe(&slot(SlotActivity::Inactive, None, None));
        assert!(
            !watch
                .observe(&slot(SlotActivity::Inactive, Some((754, 1_100)), None))
                .is_frozen()
        );
    }

    #[test]
    fn an_invalidated_slot_reports_a_stale_horizon_and_never_fires() {
        for cause in [
            SlotInvalidation::WalRemoved,
            SlotInvalidation::RowsRemoved,
            SlotInvalidation::WalLevelInsufficient,
            SlotInvalidation::IdleTimeout,
        ] {
            let mut watch = VacuumHorizonWatch::with_limits(0, 0);
            watch.observe(&slot(SlotActivity::Inactive, Some((754, 0)), None));
            let condition = watch.observe(&slot(
                SlotActivity::Inactive,
                Some((754, ANTI_WRAPAROUND_AUTOVACUUM_XID_AGE)),
                Some(cause),
            ));
            assert!(!condition.is_frozen(), "{cause:?} must not fire");
            assert_eq!(condition.reason(), "SlotInvalidated");
            assert_eq!(condition.pinned_age(), None);
            assert_eq!(condition.state(), VacuumHorizonState::Invalidated { cause });
            assert!(condition.message().contains("no longer constrains vacuum"));
        }
    }

    #[test]
    fn an_invalidated_sample_forgets_a_running_corroboration() {
        let mut watch = VacuumHorizonWatch::with_limits(0, 100);
        watch.observe(&slot(SlotActivity::Inactive, Some((754, 1_000)), None));
        watch.observe(&slot(
            SlotActivity::Inactive,
            Some((754, 1_050)),
            Some(SlotInvalidation::WalRemoved),
        ));
        assert!(
            !watch
                .observe(&slot(SlotActivity::Inactive, Some((754, 1_100)), None))
                .is_frozen()
        );
    }

    #[test]
    fn both_reportable_bounds_are_caller_overridable() {
        let first = slot(SlotActivity::Inactive, Some((754, 10)), None);
        let second = slot(SlotActivity::Inactive, Some((754, 20)), None);

        let mut shipped = VacuumHorizonWatch::new();
        shipped.observe(&first);
        assert!(!shipped.observe(&second).is_frozen());

        let mut reachable = VacuumHorizonWatch::with_limits(20, 10);
        reachable.observe(&first);
        assert!(reachable.observe(&second).is_frozen());

        let mut age_out_of_reach = VacuumHorizonWatch::with_limits(21, 10);
        age_out_of_reach.observe(&first);
        assert!(!age_out_of_reach.observe(&second).is_frozen());

        let mut corroboration_out_of_reach = VacuumHorizonWatch::with_limits(20, 11);
        corroboration_out_of_reach.observe(&first);
        assert!(!corroboration_out_of_reach.observe(&second).is_frozen());
    }

    #[test]
    fn the_condition_names_the_slot_it_evaluated() {
        let mut watch = VacuumHorizonWatch::new();
        let condition = watch.observe(&slot(SlotActivity::Inactive, Some((754, 60_000_000)), None));
        assert_eq!(condition.slot().as_str(), "pgshard_member_0001");
        assert!(condition.message().contains("pgshard_member_0001"));
    }
}
