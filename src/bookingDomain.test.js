"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  AUTO_CHARGE_TOLERANCE_FACTOR,
  MIN_BOOKING_DURATION_MINUTES,
  buildBookingSchedule,
  buildTransitionPatch,
  canReleaseProviderPayout,
  canReportBookingIssue,
  canEditBooking,
  computeSettlementAmounts,
  getAcceptedBookingInactivityStage,
  deriveAcceptDeadlineAt,
  deriveLastMinuteWindowStartsAt,
  deriveLegacyBookingStatus,
  deriveLegacyIsPaid,
  deriveProviderPayoutEligibleAt,
  evaluateAutoChargeEligibility,
  hasBookingChangeRequestExpired,
  meetsMinimumNotice,
  deriveRequestedEndDateTime,
  deriveExpiresAt,
  hasRequestedStartDateTimePassed,
  isDurationMinutesInRange,
  isWithinLastMinuteWindow,
  normalizeMinimumNoticeMinutes,
  normalizeLegacyStatusUpdate,
  isProtectedLegacyClosureMutation,
  shouldResetBookingToAcceptedAfterFutureReschedule,
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

test("isDurationMinutesInRange enforces only the minimum duration", () => {
  const tenYearsMinutes = 10 * 365 * 24 * 60;

  assert.equal(isDurationMinutesInRange(null), true);
  assert.equal(isDurationMinutesInRange(MIN_BOOKING_DURATION_MINUTES - 1), false);
  assert.equal(isDurationMinutesInRange(MIN_BOOKING_DURATION_MINUTES), true);
  assert.equal(isDurationMinutesInRange(tenYearsMinutes), true);
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

test("deriveExpiresAt uses the full 33 percent window when a start exists even beyond 24 hours", () => {
  const result = deriveExpiresAt(
    "2026-03-29T10:00:00.000Z",
    "2026-04-02T10:00:00.000Z"
  );
  assert.equal(result.toISOString(), "2026-04-01T02:00:00.000Z");
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

test("deriveProviderPayoutEligibleAt adds a seven day hold to the payment reference", () => {
  const result = deriveProviderPayoutEligibleAt("2026-03-29T10:00:00.000Z");
  assert.equal(result.toISOString(), "2026-04-05T10:00:00.000Z");
});

test("isWithinLastMinuteWindow turns true once the computed window starts", () => {
  assert.equal(
    isWithinLastMinuteWindow({
      createdAt: "2026-03-29T10:00:00.000Z",
      requestedStartDateTime: "2026-03-29T16:00:00.000Z",
      now: "2026-03-29T13:59:59.000Z",
    }),
    false
  );
  assert.equal(
    isWithinLastMinuteWindow({
      createdAt: "2026-03-29T10:00:00.000Z",
      requestedStartDateTime: "2026-03-29T16:00:00.000Z",
      now: "2026-03-29T14:00:00.000Z",
    }),
    true
  );
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

test("hasRequestedStartDateTimePassed only returns true once the requested start is reached", () => {
  assert.equal(
    hasRequestedStartDateTimePassed("2026-03-29T10:00:00.000Z", "2026-03-29T09:59:59.000Z"),
    false
  );
  assert.equal(
    hasRequestedStartDateTimePassed("2026-03-29T10:00:00.000Z", "2026-03-29T10:00:00.000Z"),
    true
  );
});

test("canReportBookingIssue enables general problems for all accepted bookings and keeps no-show time gated", () => {
  assert.equal(
    canReportBookingIssue(
      { service_status: "accepted", requested_start_datetime: "2026-03-29T10:00:00.000Z" },
      "2026-03-29T09:59:00.000Z"
    ),
    true
  );
  assert.equal(
    canReportBookingIssue(
      { service_status: "accepted", requested_start_datetime: "2026-03-29T10:00:00.000Z" },
      "2026-03-29T09:59:00.000Z",
      { issueType: "no_show_client" }
    ),
    false
  );
  assert.equal(
    canReportBookingIssue(
      { service_status: "accepted", requested_start_datetime: "2026-03-29T10:00:00.000Z" },
      "2026-03-29T10:00:00.000Z"
    ),
    true
  );
  assert.equal(
    canReportBookingIssue(
      { service_status: "accepted", requested_start_datetime: null },
      "2026-03-29T08:00:00.000Z"
    ),
    true
  );
  assert.equal(
    canReportBookingIssue(
      { service_status: "in_progress", requested_start_datetime: null },
      "2026-03-29T08:00:00.000Z"
    ),
    true
  );
});

test("canReportBookingIssue lets the client report a general issue during final payment approval", () => {
  const pendingClientApprovalBooking = {
    service_status: "finished",
    settlement_status: "pending_client_approval",
    requested_start_datetime: "2026-03-29T10:00:00.000Z",
  };

  assert.equal(
    canReportBookingIssue(
      pendingClientApprovalBooking,
      "2026-03-29T12:00:00.000Z",
      { reporterRole: "client", issueType: "general_problem" }
    ),
    true
  );
  assert.equal(
    canReportBookingIssue(
      pendingClientApprovalBooking,
      "2026-03-29T12:00:00.000Z",
      { reporterRole: "pro", issueType: "general_problem" }
    ),
    false
  );
  assert.equal(
    canReportBookingIssue(
      pendingClientApprovalBooking,
      "2026-03-29T12:00:00.000Z",
      { reporterRole: "client", issueType: "no_show_provider" }
    ),
    false
  );
});

test("getAcceptedBookingInactivityStage returns the next unsent reminder for inactive accepted bookings", () => {
  assert.deepEqual(
    getAcceptedBookingInactivityStage(
      {
        service_status: "accepted",
        settlement_status: "none",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
        updated_at: "2026-03-29T09:00:00.000Z",
      },
      { now: "2026-03-29T11:00:00.000Z" }
    ),
    {
      type: "reminder",
      key: "1h",
      elapsedMs: 60 * 60 * 1000,
      thresholdMs: 60 * 60 * 1000,
      reasonCode: "accepted_inactivity_reminder_1h",
    }
  );

  assert.deepEqual(
    getAcceptedBookingInactivityStage(
      {
        service_status: "accepted",
        settlement_status: "none",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
        updated_at: "2026-03-29T09:00:00.000Z",
      },
      {
        now: "2026-03-30T10:00:00.000Z",
        isReminderSent: (reasonCode) => reasonCode === "accepted_inactivity_reminder_1h",
      }
    ),
    {
      type: "reminder",
      key: "24h",
      elapsedMs: 24 * 60 * 60 * 1000,
      thresholdMs: 24 * 60 * 60 * 1000,
      reasonCode: "accepted_inactivity_reminder_24h",
    }
  );
});

test("getAcceptedBookingInactivityStage ignores accepted bookings with interaction and auto-cancels after seven days", () => {
  assert.equal(
    getAcceptedBookingInactivityStage(
      {
        service_status: "accepted",
        settlement_status: "none",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
        updated_at: "2026-03-29T10:00:01.000Z",
      },
      { now: "2026-03-29T11:00:00.000Z" }
    ),
    null
  );

  assert.deepEqual(
    getAcceptedBookingInactivityStage(
      {
        service_status: "accepted",
        settlement_status: "none",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
        updated_at: "2026-03-29T09:00:00.000Z",
      },
      { now: "2026-04-05T10:00:00.000Z" }
    ),
    {
      type: "auto_cancel",
      key: "7d",
      elapsedMs: 7 * 24 * 60 * 60 * 1000,
      thresholdMs: 7 * 24 * 60 * 60 * 1000,
      reasonCode: "accepted_inactivity_auto_canceled",
    }
  );
});

test("computeSettlementAmounts keeps the one euro floor when refunding excess deposit", () => {
  assert.deepEqual(
    computeSettlementAmounts({
      depositAlreadyPaidAmountCents: 500,
      proposedTotalAmountCents: 0,
      providerPayoutAmountCents: 0,
      minimumChargeAmountCents: 100,
    }),
    {
      depositAlreadyPaidAmountCents: 500,
      proposedTotalAmountCents: 0,
      effectiveTotalAmountCents: 100,
      amountDueFromClientCents: 0,
      amountToRefundCents: 400,
      providerPayoutAmountCents: 0,
      platformAmountCents: 100,
    }
  );
});

test("computeSettlementAmounts charges the delta when the final total exceeds the deposit", () => {
  assert.deepEqual(
    computeSettlementAmounts({
      depositAlreadyPaidAmountCents: 500,
      proposedTotalAmountCents: 1650,
      providerPayoutAmountCents: 1500,
      minimumChargeAmountCents: 100,
    }),
    {
      depositAlreadyPaidAmountCents: 500,
      proposedTotalAmountCents: 1650,
      effectiveTotalAmountCents: 1650,
      amountDueFromClientCents: 1150,
      amountToRefundCents: 0,
      providerPayoutAmountCents: 1500,
      platformAmountCents: 150,
    }
  );
});

test("canReleaseProviderPayout requires finished paid booking and succeeded payments", () => {
  const now = "2026-04-27T10:00:00.000Z";
  const booking = {
    service_status: "finished",
    settlement_status: "paid",
  };
  const payment = {
    status: "succeeded",
    provider_payout_status: "pending_release",
    provider_payout_amount_cents: 9000,
    provider_payout_eligible_at: "2026-04-27T09:59:00.000Z",
  };
  const depositPayment = {
    status: "succeeded",
  };

  assert.equal(canReleaseProviderPayout({ booking, payment, depositPayment, now }), true);
  assert.equal(
    canReleaseProviderPayout({
      booking: { ...booking, settlement_status: "awaiting_payment" },
      payment,
      depositPayment,
      now,
    }),
    false
  );
  assert.equal(
    canReleaseProviderPayout({
      booking,
      payment: { ...payment, status: "requires_payment_method" },
      depositPayment,
      now,
    }),
    false
  );
  assert.equal(
    canReleaseProviderPayout({
      booking,
      payment: { ...payment, provider_payout_eligible_at: "2026-04-27T10:01:00.000Z" },
      depositPayment,
      now,
    }),
    false
  );
});

test("evaluateAutoChargeEligibility blocks budget closures and allows small hourly adjustments", () => {
  assert.deepEqual(
    evaluateAutoChargeEligibility({
      priceType: "budget",
      estimatedTotalAmountCents: 5000,
      proposedTotalAmountCents: 5000,
    }),
    {
      eligible: false,
      reason: "budget_requires_manual_approval",
      needsAdjustmentNotice: false,
      toleranceLimitAmountCents: 5000,
    }
  );

  assert.deepEqual(
    evaluateAutoChargeEligibility({
      priceType: "hour",
      estimatedTotalAmountCents: 5000,
      proposedTotalAmountCents: 5900,
      estimatedDurationMinutes: 120,
      proposedFinalDurationMinutes: 140,
      toleranceFactor: AUTO_CHARGE_TOLERANCE_FACTOR,
    }),
    {
      eligible: true,
      reason: "within_tolerance",
      needsAdjustmentNotice: true,
      toleranceLimitAmountCents: 6000,
    }
  );
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

test("isProtectedLegacyClosureMutation flags direct closure and final amount payloads", () => {
  const inProgressBooking = { service_status: "in_progress", settlement_status: "none" };

  assert.equal(
    isProtectedLegacyClosureMutation({ status: "completed" }, inProgressBooking),
    true
  );
  assert.equal(
    isProtectedLegacyClosureMutation({ service_status: "finished" }, inProgressBooking),
    true
  );
  assert.equal(
    isProtectedLegacyClosureMutation({ final_price: 0 }, inProgressBooking),
    true
  );
  assert.equal(
    isProtectedLegacyClosureMutation({ requested_duration_minutes: 0 }, inProgressBooking),
    true
  );
  assert.equal(
    isProtectedLegacyClosureMutation(
      { status: "in_progress" },
      { service_status: "accepted", settlement_status: "none" }
    ),
    false
  );
  assert.equal(
    isProtectedLegacyClosureMutation({ status: "canceled" }, inProgressBooking),
    false
  );
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

test("shouldResetBookingToAcceptedAfterFutureReschedule only resets in-progress bookings when the schedule changes to a future start", () => {
  assert.equal(
    shouldResetBookingToAcceptedAfterFutureReschedule(
      {
        service_status: "accepted",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
      },
      {
        nextRequestedStartDateTime: "2026-03-29T12:00:00.000Z",
        now: "2026-03-29T09:00:00.000Z",
      }
    ),
    false
  );

  assert.equal(
    shouldResetBookingToAcceptedAfterFutureReschedule(
      {
        service_status: "in_progress",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
      },
      {
        nextRequestedStartDateTime: "2026-03-29T10:00:00.000Z",
        now: "2026-03-29T09:00:00.000Z",
      }
    ),
    false
  );

  assert.equal(
    shouldResetBookingToAcceptedAfterFutureReschedule(
      {
        service_status: "in_progress",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
      },
      {
        nextRequestedStartDateTime: "2026-03-29T09:30:00.000Z",
        now: "2026-03-29T09:45:00.000Z",
      }
    ),
    false
  );

  assert.equal(
    shouldResetBookingToAcceptedAfterFutureReschedule(
      {
        service_status: "in_progress",
        requested_start_datetime: "2026-03-29T10:00:00.000Z",
      },
      {
        nextRequestedStartDateTime: "2026-03-29T12:00:00.000Z",
        now: "2026-03-29T10:30:00.000Z",
      }
    ),
    true
  );
});

test("hasBookingChangeRequestExpired does not apply a fixed ttl", () => {
  assert.equal(
    hasBookingChangeRequestExpired(null, {
      booking: {
        service_status: "accepted",
        settlement_status: "none",
        requested_start_datetime: "2026-03-29T11:00:00.000Z",
      },
      now: "2026-03-29T12:00:00.000Z",
    }),
    false
  );

  assert.equal(
    hasBookingChangeRequestExpired(
      { status: "pending", created_at: "2026-03-29T10:00:00.000Z" },
      {
        booking: { service_status: "accepted", settlement_status: "none" },
        now: "2026-04-02T10:00:00.000Z",
      }
    ),
    false
  );
});

test("hasBookingChangeRequestExpired expires at the original or proposed start", () => {
  assert.equal(
    hasBookingChangeRequestExpired(
      { status: "pending", created_at: "2026-03-29T10:00:00.000Z" },
      {
        booking: {
          service_status: "accepted",
          settlement_status: "none",
          requested_start_datetime: "2026-03-30T10:00:00.000Z",
        },
        now: "2026-03-30T10:00:00.000Z",
      }
    ),
    true
  );

  assert.equal(
    hasBookingChangeRequestExpired(
      {
        status: "pending",
        created_at: "2026-03-29T10:00:00.000Z",
        changes_json: JSON.stringify({
          requested_start_datetime: "2026-03-30T09:00:00.000Z",
        }),
      },
      {
        booking: { service_status: "accepted", settlement_status: "none" },
        now: "2026-03-30T09:00:00.000Z",
      }
    ),
    true
  );
});

test("hasBookingChangeRequestExpired expires when the booking advances after the request", () => {
  assert.equal(
    hasBookingChangeRequestExpired(
      { status: "pending", created_at: "2026-03-29T10:00:00.000Z" },
      {
        booking: {
          service_status: "in_progress",
          settlement_status: "none",
          started_at: "2026-03-29T10:15:00.000Z",
        },
        now: "2026-03-29T10:20:00.000Z",
      }
    ),
    true
  );

  assert.equal(
    hasBookingChangeRequestExpired(
      { status: "pending", created_at: "2026-03-29T10:30:00.000Z" },
      {
        booking: {
          service_status: "in_progress",
          settlement_status: "none",
          started_at: "2026-03-29T10:15:00.000Z",
        },
        now: "2026-03-29T10:40:00.000Z",
      }
    ),
    false
  );
});

test("hasBookingChangeRequestExpired ignores resolved requests", () => {
  assert.equal(
    hasBookingChangeRequestExpired(
      { status: "accepted", created_at: "2026-03-29T10:00:00.000Z" },
      {
        booking: {
          service_status: "accepted",
          settlement_status: "none",
          requested_start_datetime: "2026-03-29T11:00:00.000Z",
        },
        now: "2026-03-29T12:00:00.000Z",
      }
    ),
    false
  );
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
