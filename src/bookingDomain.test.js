"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildBookingSchedule,
  buildTransitionPatch,
  canEditBooking,
  deriveAcceptDeadlineAt,
  deriveLastMinuteWindowStartsAt,
  deriveLegacyBookingStatus,
  deriveLegacyIsPaid,
  meetsMinimumNotice,
  deriveRequestedEndDateTime,
  deriveExpiresAt,
  normalizeMinimumNoticeMinutes,
  normalizeLegacyStatusUpdate,
} = require("./bookingDomain");

test("deriveRequestedEndDateTime returns null when start or duration is missing", () => {
  assert.equal(deriveRequestedEndDateTime(null, 30), null);
  assert.equal(deriveRequestedEndDateTime("2026-03-29T10:00:00.000Z", null), null);
});

test("deriveRequestedEndDateTime adds duration minutes to the requested start", () => {
  const result = deriveRequestedEndDateTime("2026-03-29T10:00:00.000Z", 95);
  assert.ok(result instanceof Date);
  assert.equal(result.toISOString(), "2026-03-29T11:35:00.000Z");
});

test("normalizeMinimumNoticeMinutes accepts nullable and non negative values", () => {
  assert.equal(normalizeMinimumNoticeMinutes(null), null);
  assert.equal(normalizeMinimumNoticeMinutes(""), null);
  assert.equal(normalizeMinimumNoticeMinutes(-5), null);
  assert.equal(normalizeMinimumNoticeMinutes("120"), 120);
});

test("meetsMinimumNotice ignores bookings without start or without policy", () => {
  assert.equal(
    meetsMinimumNotice({
      requestedStartDateTime: null,
      minimumNoticeMinutes: 120,
      now: "2026-03-29T10:00:00.000Z",
    }),
    true
  );
  assert.equal(
    meetsMinimumNotice({
      requestedStartDateTime: "2026-03-29T13:00:00.000Z",
      minimumNoticeMinutes: null,
      now: "2026-03-29T10:00:00.000Z",
    }),
    true
  );
});

test("meetsMinimumNotice enforces the configured lead time", () => {
  assert.equal(
    meetsMinimumNotice({
      requestedStartDateTime: "2026-03-29T11:59:00.000Z",
      minimumNoticeMinutes: 120,
      now: "2026-03-29T10:00:00.000Z",
    }),
    false
  );
  assert.equal(
    meetsMinimumNotice({
      requestedStartDateTime: "2026-03-29T12:00:00.000Z",
      minimumNoticeMinutes: 120,
      now: "2026-03-29T10:00:00.000Z",
    }),
    true
  );
});

test("deriveAcceptDeadlineAt uses start datetime directly when lead time is under one hour", () => {
  const result = deriveAcceptDeadlineAt(
    "2026-03-29T10:00:00.000Z",
    "2026-03-29T10:45:00.000Z"
  );
  assert.equal(result.toISOString(), "2026-03-29T10:45:00.000Z");
});

test("deriveAcceptDeadlineAt applies the 67 percent rule for longer lead times", () => {
  const result = deriveAcceptDeadlineAt(
    "2026-03-29T10:00:00.000Z",
    "2026-03-29T16:00:00.000Z"
  );
  assert.equal(result.toISOString(), "2026-03-29T14:00:00.000Z");
});

test("deriveExpiresAt falls back to created_at plus 24 hours when no start exists", () => {
  const result = deriveExpiresAt("2026-03-29T10:00:00.000Z", null);
  assert.equal(result.toISOString(), "2026-03-30T10:00:00.000Z");
});

test("deriveExpiresAt chooses the earliest deadline between 24 hours and acceptance deadline", () => {
  const result = deriveExpiresAt(
    "2026-03-29T10:00:00.000Z",
    "2026-03-29T16:00:00.000Z"
  );
  assert.equal(result.toISOString(), "2026-03-29T14:00:00.000Z");
});

test("deriveLastMinuteWindowStartsAt clamps to one hour minimum", () => {
  const result = deriveLastMinuteWindowStartsAt(
    "2026-03-29T10:00:00.000Z",
    "2026-03-29T11:30:00.000Z"
  );
  assert.equal(result.toISOString(), "2026-03-29T10:30:00.000Z");
});

test("deriveLastMinuteWindowStartsAt clamps to twenty four hours maximum", () => {
  const result = deriveLastMinuteWindowStartsAt(
    "2026-03-01T10:00:00.000Z",
    "2026-03-11T10:00:00.000Z"
  );
  assert.equal(result.toISOString(), "2026-03-10T10:00:00.000Z");
});

test("buildBookingSchedule returns the normalized lifecycle dates together", () => {
  const schedule = buildBookingSchedule({
    createdAt: "2026-03-29T10:00:00.000Z",
    requestedStartDateTime: "2026-03-29T13:00:00.000Z",
    requestedDurationMinutes: 90,
  });

  assert.equal(schedule.requestedEndDateTime.toISOString(), "2026-03-29T14:30:00.000Z");
  assert.equal(schedule.acceptDeadlineAt.toISOString(), "2026-03-29T12:00:00.000Z");
  assert.equal(schedule.expiresAt.toISOString(), "2026-03-29T12:00:00.000Z");
  assert.equal(schedule.lastMinuteWindowStartsAt.toISOString(), "2026-03-29T12:00:00.000Z");
});

test("normalizeLegacyStatusUpdate maps old statuses to the new axes", () => {
  assert.deepEqual(normalizeLegacyStatusUpdate("accepted"), { serviceStatus: "accepted" });
  assert.deepEqual(normalizeLegacyStatusUpdate("payment_failed"), {
    serviceStatus: "pending_deposit",
    settlementStatus: "payment_failed",
  });
  assert.deepEqual(
    normalizeLegacyStatusUpdate("completed", { settlement_status: "none" }),
    { serviceStatus: "finished", settlementStatus: "awaiting_payment" }
  );
  assert.deepEqual(normalizeLegacyStatusUpdate("rejected"), {
    serviceStatus: "canceled",
    cancellationReasonCode: "rejected",
  });
});

test("buildTransitionPatch stamps timestamps when entering lifecycle milestones", () => {
  const currentBooking = {
    service_status: "requested",
    settlement_status: "none",
    accepted_at: null,
    started_at: null,
    finished_at: null,
    canceled_at: null,
    expired_at: null,
  };

  const acceptedTransition = buildTransitionPatch(currentBooking, {
    nextServiceStatus: "accepted",
    now: "2026-03-29T10:00:00.000Z",
  });
  assert.equal(acceptedTransition.patch.service_status, "accepted");
  assert.equal(acceptedTransition.patch.accepted_at.toISOString(), "2026-03-29T10:00:00.000Z");

  const finishedTransition = buildTransitionPatch(
    { ...currentBooking, service_status: "in_progress" },
    {
      nextServiceStatus: "finished",
      nextSettlementStatus: "awaiting_payment",
      now: "2026-03-29T12:00:00.000Z",
    }
  );
  assert.equal(finishedTransition.patch.service_status, "finished");
  assert.equal(finishedTransition.patch.settlement_status, "awaiting_payment");
  assert.equal(finishedTransition.patch.finished_at.toISOString(), "2026-03-29T12:00:00.000Z");
});

test("canEditBooking only allows accepted and in_progress bookings outside blocked settlement states", () => {
  assert.equal(canEditBooking({ service_status: "requested", settlement_status: "none" }), false);
  assert.equal(canEditBooking({ service_status: "accepted", settlement_status: "none" }), true);
  assert.equal(canEditBooking({ service_status: "in_progress", settlement_status: "awaiting_payment" }), true);
  assert.equal(canEditBooking({ service_status: "accepted", settlement_status: "in_dispute" }), false);
});

test("legacy helpers preserve compatibility for old UI assumptions", () => {
  assert.equal(
    deriveLegacyBookingStatus({
      serviceStatus: "finished",
      settlementStatus: "awaiting_payment",
    }),
    "completed"
  );
  assert.equal(
    deriveLegacyBookingStatus({
      serviceStatus: "canceled",
      settlementStatus: "none",
      cancellationReasonCode: "rejected",
    }),
    "rejected"
  );
  assert.equal(deriveLegacyIsPaid("paid"), true);
  assert.equal(deriveLegacyIsPaid("awaiting_payment"), false);
});
