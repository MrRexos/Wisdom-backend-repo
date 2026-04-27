"use strict";

const SERVICE_STATUSES = Object.freeze([
  "pending_deposit",
  "requested",
  "accepted",
  "in_progress",
  "finished",
  "canceled",
  "expired",
]);

const SETTLEMENT_STATUSES = Object.freeze([
  "none",
  "pending_client_approval",
  "awaiting_payment",
  "paid",
  "refund_pending",
  "partially_refunded",
  "refunded",
  "payment_failed",
  "manual_review_required",
  "in_dispute",
]);

const BOOKING_CHANGE_REQUEST_STATUSES = Object.freeze([
  "pending",
  "accepted",
  "rejected",
  "canceled",
  "expired",
]);

const MIN_BOOKING_DURATION_MINUTES = 5;
const ONE_HOUR_MS = 60 * 60 * 1000;
const ONE_DAY_MS = 24 * ONE_HOUR_MS;
const THREE_DAYS_MS = 3 * ONE_DAY_MS;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
const AUTO_CHARGE_TOLERANCE_FACTOR = 1.2;
const PROVIDER_PAYOUT_TIME_ZONE = "Europe/Madrid";
const PROVIDER_PAYOUT_MORNING_START_HOUR = 8;
const PROVIDER_PAYOUT_MORNING_END_HOUR = 12;
const ACCEPTED_BOOKING_INACTIVITY_REMINDER_STAGES = Object.freeze([
  Object.freeze({
    key: "1h",
    delayMs: ONE_HOUR_MS,
    reasonCode: "accepted_inactivity_reminder_1h",
  }),
  Object.freeze({
    key: "24h",
    delayMs: ONE_DAY_MS,
    reasonCode: "accepted_inactivity_reminder_24h",
  }),
  Object.freeze({
    key: "72h",
    delayMs: THREE_DAYS_MS,
    reasonCode: "accepted_inactivity_reminder_72h",
  }),
]);
const ACCEPTED_BOOKING_INACTIVITY_AUTO_CANCEL_REASON_CODE = "accepted_inactivity_auto_canceled";
const LEGACY_CLOSURE_MUTATION_FIELDS = Object.freeze([
  "final_price",
  "proposed_final_price",
  "service_duration",
  "requested_duration_minutes",
  "proposed_final_duration_minutes",
  "zero_charge_mode",
]);

function isValidDate(value) {
  return value instanceof Date && !Number.isNaN(value.getTime());
}

function getZonedDateTimeParts(value, timeZone = PROVIDER_PAYOUT_TIME_ZONE) {
  const date = parseDateInput(value) || new Date();

  try {
    const formatter = new Intl.DateTimeFormat("en-GB", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    });
    const parts = formatter.formatToParts(date).reduce((acc, part) => {
      if (part.type !== "literal") {
        acc[part.type] = part.value;
      }
      return acc;
    }, {});

    return {
      year: Number(parts.year),
      month: Number(parts.month),
      day: Number(parts.day),
      hour: Number(parts.hour) === 24 ? 0 : Number(parts.hour),
      minute: Number(parts.minute),
      second: Number(parts.second),
    };
  } catch {
    return null;
  }
}

function isFirstWednesdayMorningPayoutWindow(now = new Date(), {
  timeZone = PROVIDER_PAYOUT_TIME_ZONE,
  startHour = PROVIDER_PAYOUT_MORNING_START_HOUR,
  endHour = PROVIDER_PAYOUT_MORNING_END_HOUR,
} = {}) {
  const parts = getZonedDateTimeParts(now, timeZone);
  if (
    !parts
    || !Number.isInteger(parts.year)
    || !Number.isInteger(parts.month)
    || !Number.isInteger(parts.day)
    || !Number.isInteger(parts.hour)
  ) {
    return false;
  }

  const dayOfWeek = new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay();
  return dayOfWeek === 3
    && parts.day >= 1
    && parts.day <= 7
    && parts.hour >= startHour
    && parts.hour < endHour;
}

function parseDateInput(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (isValidDate(value)) {
    return new Date(value.getTime());
  }

  if (typeof value === "number") {
    const parsedFromNumber = new Date(value);
    return isValidDate(parsedFromNumber) ? parsedFromNumber : null;
  }

  if (typeof value !== "string") {
    return null;
  }

  const trimmedValue = value.trim();
  if (!trimmedValue) {
    return null;
  }

  const normalizedIso = trimmedValue.includes("T")
    ? trimmedValue
    : trimmedValue.replace(" ", "T");

  const parsedFromIso = new Date(normalizedIso);
  return isValidDate(parsedFromIso) ? parsedFromIso : null;
}

function normalizeEnumValue(value, allowedValues, fallbackValue = null) {
  if (typeof value !== "string") {
    return fallbackValue;
  }

  const normalizedValue = value.trim().toLowerCase().replace(/-/g, "_");
  return allowedValues.includes(normalizedValue) ? normalizedValue : fallbackValue;
}

function normalizeServiceStatus(value, fallbackValue = "pending_deposit") {
  return normalizeEnumValue(value, SERVICE_STATUSES, fallbackValue);
}

function normalizeSettlementStatus(value, fallbackValue = "none") {
  return normalizeEnumValue(value, SETTLEMENT_STATUSES, fallbackValue);
}

function normalizeBookingChangeRequestStatus(value, fallbackValue = "pending") {
  return normalizeEnumValue(value, BOOKING_CHANGE_REQUEST_STATUSES, fallbackValue);
}

function normalizeDurationMinutes(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const numericValue = Math.round(Number(value));
  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return null;
  }

  return numericValue;
}

function normalizeMinimumNoticeMinutes(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const numericValue = Math.round(Number(value));
  if (!Number.isFinite(numericValue) || numericValue < 0) {
    return null;
  }

  return numericValue;
}

function normalizeAmountCents(value) {
  const numericValue = Math.round(Number(value));
  if (!Number.isFinite(numericValue) || numericValue < 0) {
    return 0;
  }

  return numericValue;
}

function isDurationMinutesInRange(value) {
  if (value === null || value === undefined) {
    return true;
  }

  return value >= MIN_BOOKING_DURATION_MINUTES;
}

function deriveRequestedEndDateTime(requestedStartDateTime, requestedDurationMinutes) {
  const startDate = parseDateInput(requestedStartDateTime);
  const durationMinutes = normalizeDurationMinutes(requestedDurationMinutes);

  if (!startDate || durationMinutes === null) {
    return null;
  }

  return new Date(startDate.getTime() + durationMinutes * 60 * 1000);
}

function meetsMinimumNotice({
  requestedStartDateTime,
  minimumNoticeMinutes,
  now = new Date(),
} = {}) {
  const normalizedRequestedStartDateTime = parseDateInput(requestedStartDateTime);
  const normalizedMinimumNoticeMinutes = normalizeMinimumNoticeMinutes(minimumNoticeMinutes);
  const normalizedNow = parseDateInput(now) || new Date();

  if (!normalizedRequestedStartDateTime || normalizedMinimumNoticeMinutes === null) {
    return true;
  }

  if (normalizedMinimumNoticeMinutes <= 0) {
    return true;
  }

  return normalizedRequestedStartDateTime.getTime() - normalizedNow.getTime()
    >= normalizedMinimumNoticeMinutes * 60 * 1000;
}

function deriveAcceptDeadlineAt(createdAtInput, requestedStartDateTimeInput) {
  const createdAt = parseDateInput(createdAtInput);
  const requestedStartDateTime = parseDateInput(requestedStartDateTimeInput);

  if (!createdAt || !requestedStartDateTime) {
    return null;
  }

  const leadTimeMs = requestedStartDateTime.getTime() - createdAt.getTime();
  if (leadTimeMs <= ONE_HOUR_MS) {
    return new Date(requestedStartDateTime.getTime());
  }

  return new Date(createdAt.getTime() + Math.round(leadTimeMs * (2 / 3)));
}

function deriveExpiresAt(createdAtInput, requestedStartDateTimeInput) {
  const createdAt = parseDateInput(createdAtInput);
  if (!createdAt) {
    return null;
  }

  const acceptDeadlineAt = deriveAcceptDeadlineAt(createdAt, requestedStartDateTimeInput);

  if (!acceptDeadlineAt) {
    return new Date(createdAt.getTime() + ONE_DAY_MS);
  }

  return acceptDeadlineAt;
}

function deriveProviderPayoutEligibleAt(referenceDateTimeInput) {
  const referenceDateTime = parseDateInput(referenceDateTimeInput);

  if (!referenceDateTime) {
    return null;
  }

  return new Date(referenceDateTime.getTime() + ONE_WEEK_MS);
}

function deriveLastMinuteWindowStartsAt(createdAtInput, requestedStartDateTimeInput) {
  const createdAt = parseDateInput(createdAtInput);
  const requestedStartDateTime = parseDateInput(requestedStartDateTimeInput);

  if (!createdAt || !requestedStartDateTime) {
    return null;
  }

  const leadTimeMs = requestedStartDateTime.getTime() - createdAt.getTime();
  if (leadTimeMs <= 0) {
    return new Date(requestedStartDateTime.getTime());
  }

  const unclampedWindowMs = Math.round(leadTimeMs / 3);
  const clampedWindowMs = Math.max(ONE_HOUR_MS, Math.min(ONE_DAY_MS, unclampedWindowMs));

  return new Date(requestedStartDateTime.getTime() - clampedWindowMs);
}

function isWithinLastMinuteWindow({
  createdAt,
  requestedStartDateTime,
  lastMinuteWindowStartsAt = null,
  now = new Date(),
} = {}) {
  const normalizedNow = parseDateInput(now) || new Date();
  const normalizedRequestedStartDateTime = parseDateInput(requestedStartDateTime);

  if (!normalizedRequestedStartDateTime) {
    return false;
  }

  const windowStartsAt = parseDateInput(lastMinuteWindowStartsAt)
    || deriveLastMinuteWindowStartsAt(createdAt, normalizedRequestedStartDateTime);

  if (!windowStartsAt) {
    return false;
  }

  return normalizedNow.getTime() >= windowStartsAt.getTime();
}

function hasRequestedStartDateTimePassed(requestedStartDateTimeInput, now = new Date()) {
  const requestedStartDateTime = parseDateInput(requestedStartDateTimeInput);
  const normalizedNow = parseDateInput(now) || new Date();

  if (!requestedStartDateTime) {
    return false;
  }

  return requestedStartDateTime.getTime() <= normalizedNow.getTime();
}

function getAcceptedBookingInactivityStage(booking, {
  now = new Date(),
  isReminderSent = () => false,
  reminderStages = ACCEPTED_BOOKING_INACTIVITY_REMINDER_STAGES,
  autoCancelDelayMs = ONE_WEEK_MS,
} = {}) {
  const normalizedServiceStatus = normalizeServiceStatus(booking?.service_status, "pending_deposit");
  const normalizedSettlementStatus = normalizeSettlementStatus(booking?.settlement_status, "none");
  if (normalizedServiceStatus !== "accepted" || normalizedSettlementStatus !== "none") {
    return null;
  }

  const requestedStartDateTime = parseDateInput(
    booking?.requested_start_datetime ?? booking?.booking_start_datetime
  );
  const normalizedNow = parseDateInput(now) || new Date();
  if (!requestedStartDateTime || normalizedNow.getTime() < requestedStartDateTime.getTime()) {
    return null;
  }

  const updatedAt = parseDateInput(booking?.updated_at);
  if (updatedAt && updatedAt.getTime() > requestedStartDateTime.getTime()) {
    return null;
  }

  const elapsedMs = normalizedNow.getTime() - requestedStartDateTime.getTime();
  const normalizedAutoCancelDelayMs = Number.isFinite(Number(autoCancelDelayMs)) && Number(autoCancelDelayMs) > 0
    ? Number(autoCancelDelayMs)
    : ONE_WEEK_MS;

  if (elapsedMs >= normalizedAutoCancelDelayMs) {
    return {
      type: "auto_cancel",
      key: "7d",
      elapsedMs,
      thresholdMs: normalizedAutoCancelDelayMs,
      reasonCode: ACCEPTED_BOOKING_INACTIVITY_AUTO_CANCEL_REASON_CODE,
    };
  }

  const normalizedReminderStages = Array.isArray(reminderStages)
    ? [...reminderStages]
      .filter((stage) => Number.isFinite(Number(stage?.delayMs)) && Number(stage.delayMs) > 0)
      .sort((left, right) => Number(right.delayMs) - Number(left.delayMs))
    : [];

  for (const stage of normalizedReminderStages) {
    if (elapsedMs < Number(stage.delayMs)) {
      continue;
    }

    if (typeof isReminderSent === "function" && isReminderSent(stage.reasonCode, stage)) {
      continue;
    }

    return {
      type: "reminder",
      key: stage.key || null,
      elapsedMs,
      thresholdMs: Number(stage.delayMs),
      reasonCode: stage.reasonCode || null,
    };
  }

  return null;
}

function canReportBookingIssue(booking, now = new Date(), options = {}) {
  const normalizedServiceStatus = normalizeServiceStatus(booking?.service_status, "pending_deposit");
  const normalizedSettlementStatus = normalizeSettlementStatus(booking?.settlement_status, "none");
  const reporterRole = typeof options?.reporterRole === "string"
    ? options.reporterRole.trim().toLowerCase()
    : null;
  const issueType = typeof options?.issueType === "string"
    ? options.issueType.trim().toLowerCase()
    : "general_problem";

  if (
    reporterRole === "client"
    && issueType === "general_problem"
    && normalizedServiceStatus === "finished"
    && normalizedSettlementStatus === "pending_client_approval"
  ) {
    return true;
  }

  if (normalizedServiceStatus === "in_progress") {
    return true;
  }

  if (normalizedServiceStatus !== "accepted") {
    return false;
  }

  if (issueType === "general_problem") {
    return true;
  }

  const requestedStartDateTime = booking?.requested_start_datetime ?? booking?.booking_start_datetime;
  if (!parseDateInput(requestedStartDateTime)) {
    return true;
  }

  return hasRequestedStartDateTimePassed(
    requestedStartDateTime,
    now
  );
}

function computeSettlementAmounts({
  depositAlreadyPaidAmountCents,
  proposedTotalAmountCents,
  providerPayoutAmountCents,
  minimumChargeAmountCents = 0,
} = {}) {
  const depositAmount = normalizeAmountCents(depositAlreadyPaidAmountCents);
  const proposedTotalAmount = normalizeAmountCents(proposedTotalAmountCents);
  const providerPayoutAmount = normalizeAmountCents(providerPayoutAmountCents);
  const minimumChargeAmount = normalizeAmountCents(minimumChargeAmountCents);

  let effectiveTotalAmount = proposedTotalAmount;
  if (depositAmount > proposedTotalAmount && depositAmount > 0) {
    effectiveTotalAmount = Math.max(proposedTotalAmount, minimumChargeAmount);
  }
  effectiveTotalAmount = Math.max(effectiveTotalAmount, providerPayoutAmount);

  return {
    depositAlreadyPaidAmountCents: depositAmount,
    proposedTotalAmountCents: proposedTotalAmount,
    effectiveTotalAmountCents: effectiveTotalAmount,
    amountDueFromClientCents: Math.max(0, effectiveTotalAmount - depositAmount),
    amountToRefundCents: Math.max(0, depositAmount - effectiveTotalAmount),
    providerPayoutAmountCents: providerPayoutAmount,
    platformAmountCents: Math.max(0, effectiveTotalAmount - providerPayoutAmount),
  };
}

function canReleaseProviderPayout({
  booking = {},
  payment = {},
  depositPayment = {},
  now = new Date(),
} = {}) {
  const normalizedNow = parseDateInput(now) || new Date();
  const payoutEligibleAt = parseDateInput(payment?.provider_payout_eligible_at);
  const depositStatus = String(depositPayment?.status || "").trim().toLowerCase();

  return normalizeServiceStatus(booking?.service_status, "pending_deposit") === "finished"
    && normalizeSettlementStatus(booking?.settlement_status, "none") === "paid"
    && String(payment?.status || "").trim().toLowerCase() === "succeeded"
    && ["succeeded", "partially_refunded"].includes(depositStatus)
    && String(payment?.provider_payout_status || "").trim().toLowerCase() === "pending_release"
    && normalizeAmountCents(payment?.provider_payout_amount_cents) > 0
    && payoutEligibleAt !== null
    && payoutEligibleAt.getTime() <= normalizedNow.getTime()
    && isFirstWednesdayMorningPayoutWindow(normalizedNow);
}

function evaluateAutoChargeEligibility({
  priceType,
  estimatedTotalAmountCents,
  proposedTotalAmountCents,
  estimatedDurationMinutes = null,
  proposedFinalDurationMinutes = null,
  zeroChargeMode = false,
  toleranceFactor = AUTO_CHARGE_TOLERANCE_FACTOR,
} = {}) {
  const normalizedPriceType = String(priceType || "").trim().toLowerCase();
  const estimatedTotalAmount = Number.isFinite(Number(estimatedTotalAmountCents))
    ? normalizeAmountCents(estimatedTotalAmountCents)
    : null;
  const proposedTotalAmount = normalizeAmountCents(proposedTotalAmountCents);
  const normalizedEstimatedDurationMinutes = normalizeDurationMinutes(estimatedDurationMinutes);
  const normalizedProposedFinalDurationMinutes = normalizeDurationMinutes(proposedFinalDurationMinutes);

  if (zeroChargeMode || proposedTotalAmount <= 0) {
    return {
      eligible: true,
      reason: "zero_charge",
      needsAdjustmentNotice: false,
      toleranceLimitAmountCents: estimatedTotalAmount,
    };
  }

  if (normalizedPriceType === "budget") {
    return {
      eligible: false,
      reason: "budget_requires_manual_approval",
      needsAdjustmentNotice: false,
      toleranceLimitAmountCents: estimatedTotalAmount,
    };
  }

  if (normalizedPriceType === "hour" && normalizedEstimatedDurationMinutes === null) {
    return {
      eligible: false,
      reason: "hour_missing_estimate",
      needsAdjustmentNotice: false,
      toleranceLimitAmountCents: estimatedTotalAmount,
    };
  }

  if (estimatedTotalAmount === null || estimatedTotalAmount <= 0) {
    return {
      eligible: false,
      reason: "missing_estimate",
      needsAdjustmentNotice: false,
      toleranceLimitAmountCents: estimatedTotalAmount,
    };
  }

  if (
    normalizedPriceType === "hour"
    && normalizedProposedFinalDurationMinutes !== null
    && normalizedEstimatedDurationMinutes !== null
    && normalizedProposedFinalDurationMinutes <= normalizedEstimatedDurationMinutes
    && proposedTotalAmount <= estimatedTotalAmount
  ) {
    return {
      eligible: true,
      reason: "within_estimate",
      needsAdjustmentNotice: false,
      toleranceLimitAmountCents: estimatedTotalAmount,
    };
  }

  if (proposedTotalAmount <= estimatedTotalAmount) {
    return {
      eligible: true,
      reason: "within_estimate",
      needsAdjustmentNotice: false,
      toleranceLimitAmountCents: estimatedTotalAmount,
    };
  }

  const safeToleranceFactor = Number.isFinite(Number(toleranceFactor)) && Number(toleranceFactor) > 1
    ? Number(toleranceFactor)
    : AUTO_CHARGE_TOLERANCE_FACTOR;
  const toleranceLimitAmountCents = Math.round(estimatedTotalAmount * safeToleranceFactor);

  if (proposedTotalAmount <= toleranceLimitAmountCents) {
    return {
      eligible: true,
      reason: "within_tolerance",
      needsAdjustmentNotice: true,
      toleranceLimitAmountCents,
    };
  }

  return {
    eligible: false,
    reason: "above_tolerance",
    needsAdjustmentNotice: true,
    toleranceLimitAmountCents,
  };
}

function buildBookingSchedule({
  createdAt,
  requestedStartDateTime,
  requestedDurationMinutes,
}) {
  const normalizedCreatedAt = parseDateInput(createdAt);
  const normalizedRequestedStartDateTime = parseDateInput(requestedStartDateTime);
  const normalizedRequestedDurationMinutes = normalizeDurationMinutes(requestedDurationMinutes);

  return {
    requestedStartDateTime: normalizedRequestedStartDateTime,
    requestedDurationMinutes: normalizedRequestedDurationMinutes,
    requestedEndDateTime: deriveRequestedEndDateTime(
      normalizedRequestedStartDateTime,
      normalizedRequestedDurationMinutes
    ),
    acceptDeadlineAt: deriveAcceptDeadlineAt(normalizedCreatedAt, normalizedRequestedStartDateTime),
    expiresAt: deriveExpiresAt(normalizedCreatedAt, normalizedRequestedStartDateTime),
    lastMinuteWindowStartsAt: deriveLastMinuteWindowStartsAt(
      normalizedCreatedAt,
      normalizedRequestedStartDateTime
    ),
  };
}

function deriveLegacyBookingStatus({
  serviceStatus,
  settlementStatus,
  cancellationReasonCode,
}) {
  const normalizedServiceStatus = normalizeServiceStatus(serviceStatus, "pending_deposit");
  const normalizedSettlementStatus = normalizeSettlementStatus(settlementStatus, "none");
  const normalizedCancellationReasonCode = String(cancellationReasonCode || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");

  if (normalizedSettlementStatus === "payment_failed") {
    return "payment_failed";
  }

  if (normalizedServiceStatus === "canceled") {
    if (
      normalizedCancellationReasonCode === "rejected" ||
      normalizedCancellationReasonCode === "provider_rejected" ||
      normalizedCancellationReasonCode === "rejected_by_provider"
    ) {
      return "rejected";
    }

    return "canceled";
  }

  if (normalizedServiceStatus === "finished") {
    return "completed";
  }

  return normalizedServiceStatus;
}

function deriveLegacyIsPaid(settlementStatus) {
  const normalizedSettlementStatus = normalizeSettlementStatus(settlementStatus, "none");
  return normalizedSettlementStatus === "paid";
}

function canEditBooking(booking) {
  const normalizedServiceStatus = normalizeServiceStatus(booking?.service_status, "pending_deposit");
  const normalizedSettlementStatus = normalizeSettlementStatus(booking?.settlement_status, "none");

  if (normalizedServiceStatus !== "accepted" && normalizedServiceStatus !== "in_progress") {
    return false;
  }

  return ![
    "pending_client_approval",
    "manual_review_required",
    "in_dispute",
  ].includes(normalizedSettlementStatus);
}

function shouldResetBookingToAcceptedAfterFutureReschedule(currentBooking, {
  nextRequestedStartDateTime,
  now = new Date(),
} = {}) {
  const normalizedServiceStatus = normalizeServiceStatus(
    currentBooking?.service_status,
    "pending_deposit"
  );
  if (normalizedServiceStatus !== "in_progress") {
    return false;
  }

  const parsedNextRequestedStartDateTime = parseDateInput(nextRequestedStartDateTime);
  if (!parsedNextRequestedStartDateTime) {
    return false;
  }

  const parsedCurrentRequestedStartDateTime = parseDateInput(
    currentBooking?.requested_start_datetime
    ?? currentBooking?.booking_start_datetime
    ?? null
  );
  if (
    parsedCurrentRequestedStartDateTime
    && parsedCurrentRequestedStartDateTime.getTime() === parsedNextRequestedStartDateTime.getTime()
  ) {
    return false;
  }

  const normalizedNow = parseDateInput(now) || new Date();
  return parsedNextRequestedStartDateTime.getTime() > normalizedNow.getTime();
}

function parseChangeRequestChanges(changeRequest) {
  const rawChanges = changeRequest?.changes
    ?? changeRequest?.changes_json
    ?? changeRequest?.changesJson
    ?? null;

  if (!rawChanges) {
    return null;
  }

  if (typeof rawChanges === "object" && !Array.isArray(rawChanges)) {
    return rawChanges;
  }

  if (typeof rawChanges !== "string") {
    return null;
  }

  try {
    const parsedChanges = JSON.parse(rawChanges);
    return parsedChanges && typeof parsedChanges === "object" && !Array.isArray(parsedChanges)
      ? parsedChanges
      : null;
  } catch {
    return null;
  }
}

function getChangeRequestProposedStartDateTime(changeRequest) {
  const changes = parseChangeRequestChanges(changeRequest);
  return parseDateInput(
    changes?.requested_start_datetime
    ?? changes?.booking_start_datetime
    ?? changeRequest?.requested_start_datetime
    ?? changeRequest?.booking_start_datetime
    ?? null
  );
}

function hasBookingAdvancedAfterChangeRequest(changeRequest, booking) {
  const createdAt = parseDateInput(changeRequest?.created_at ?? changeRequest?.createdAt);
  if (!createdAt || !booking || typeof booking !== "object") {
    return false;
  }

  const lifecycleAdvancedDates = [
    booking.accepted_at,
    booking.started_at,
    booking.finished_at,
    booking.canceled_at,
    booking.expired_at,
  ]
    .map((value) => parseDateInput(value))
    .filter(Boolean);

  return lifecycleAdvancedDates.some(
    (advancedAt) => advancedAt.getTime() >= createdAt.getTime()
  );
}

function hasBookingChangeRequestExpired(changeRequest, {
  now = new Date(),
  booking = null,
} = {}) {
  if (!changeRequest || typeof changeRequest !== "object") {
    return false;
  }

  const normalizedStatus = normalizeBookingChangeRequestStatus(
    changeRequest?.status,
    "pending"
  );

  if (normalizedStatus !== "pending") {
    return false;
  }

  const normalizedNow = parseDateInput(now) || new Date();
  const originalStartDateTime = parseDateInput(
    booking?.requested_start_datetime
    ?? booking?.booking_start_datetime
    ?? null
  );
  if (
    originalStartDateTime
    && originalStartDateTime.getTime() <= normalizedNow.getTime()
  ) {
    return true;
  }

  const proposedStartDateTime = getChangeRequestProposedStartDateTime(changeRequest);
  if (
    proposedStartDateTime
    && proposedStartDateTime.getTime() <= normalizedNow.getTime()
  ) {
    return true;
  }

  if (booking && typeof booking === "object" && !canEditBooking(booking)) {
    return true;
  }

  return hasBookingAdvancedAfterChangeRequest(changeRequest, booking);
}

function buildTransitionPatch(
  currentBooking,
  {
    nextServiceStatus,
    nextSettlementStatus,
    now = new Date(),
  } = {}
) {
  const normalizedNow = parseDateInput(now) || new Date();
  const currentServiceStatus = normalizeServiceStatus(
    currentBooking?.service_status,
    "pending_deposit"
  );
  const currentSettlementStatus = normalizeSettlementStatus(
    currentBooking?.settlement_status,
    "none"
  );
  const targetServiceStatus = normalizeServiceStatus(nextServiceStatus, currentServiceStatus);
  const targetSettlementStatus = normalizeSettlementStatus(
    nextSettlementStatus,
    currentSettlementStatus
  );
  const patch = {};

  if (targetServiceStatus !== currentServiceStatus) {
    patch.service_status = targetServiceStatus;

    if (targetServiceStatus === "accepted" && !currentBooking?.accepted_at) {
      patch.accepted_at = normalizedNow;
    }
    if (targetServiceStatus === "in_progress" && !currentBooking?.started_at) {
      patch.started_at = normalizedNow;
    }
    if (targetServiceStatus === "finished" && !currentBooking?.finished_at) {
      patch.finished_at = normalizedNow;
    }
    if (targetServiceStatus === "canceled" && !currentBooking?.canceled_at) {
      patch.canceled_at = normalizedNow;
    }
    if (targetServiceStatus === "expired" && !currentBooking?.expired_at) {
      patch.expired_at = normalizedNow;
    }
  }

  if (targetSettlementStatus !== currentSettlementStatus) {
    patch.settlement_status = targetSettlementStatus;
  }

  return {
    patch,
    changed:
      Object.prototype.hasOwnProperty.call(patch, "service_status") ||
      Object.prototype.hasOwnProperty.call(patch, "settlement_status"),
    fromServiceStatus: currentServiceStatus,
    toServiceStatus: targetServiceStatus,
    fromSettlementStatus: currentSettlementStatus,
    toSettlementStatus: targetSettlementStatus,
  };
}

function normalizeLegacyStatusUpdate(status, currentBooking = null) {
  const normalizedStatus = String(status || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  const currentSettlementStatus = normalizeSettlementStatus(
    currentBooking?.settlement_status,
    "none"
  );

  switch (normalizedStatus) {
    case "pending_deposit":
      return { serviceStatus: "pending_deposit" };
    case "requested":
      return { serviceStatus: "requested" };
    case "accepted":
      return { serviceStatus: "accepted" };
    case "in_progress":
      return { serviceStatus: "in_progress" };
    case "completed":
      return {
        serviceStatus: "finished",
        settlementStatus:
          currentSettlementStatus === "paid" ? "paid" : "awaiting_payment",
      };
    case "canceled":
    case "cancelled":
      return { serviceStatus: "canceled" };
    case "rejected":
      return {
        serviceStatus: "canceled",
        cancellationReasonCode: "rejected",
      };
    case "expired":
      return { serviceStatus: "expired" };
    case "payment_failed":
      return {
        serviceStatus: "pending_deposit",
        settlementStatus: "payment_failed",
      };
    default: {
      const normalizedServiceStatus = normalizeServiceStatus(normalizedStatus, null);
      if (normalizedServiceStatus) {
        return { serviceStatus: normalizedServiceStatus };
      }

      const normalizedSettlementStatus = normalizeSettlementStatus(normalizedStatus, null);
      if (normalizedSettlementStatus) {
        return { settlementStatus: normalizedSettlementStatus };
      }

      return {};
    }
  }
}

function isProtectedLegacyClosureMutation(payload = {}, currentBooking = null) {
  if (!payload || typeof payload !== "object") {
    return false;
  }

  const mappedLegacyStatus = Object.prototype.hasOwnProperty.call(payload, "status")
    ? normalizeLegacyStatusUpdate(payload.status, currentBooking)
    : {};
  const requestedServiceStatus = Object.prototype.hasOwnProperty.call(payload, "service_status")
    ? normalizeServiceStatus(payload.service_status, null)
    : mappedLegacyStatus.serviceStatus;
  const currentServiceStatus = normalizeServiceStatus(
    currentBooking?.service_status,
    "pending_deposit"
  );
  const targetServiceStatus = requestedServiceStatus || currentServiceStatus;

  if (targetServiceStatus === "finished") {
    return true;
  }

  return LEGACY_CLOSURE_MUTATION_FIELDS.some((field) => (
    Object.prototype.hasOwnProperty.call(payload, field)
  ));
}

module.exports = {
  AUTO_CHARGE_TOLERANCE_FACTOR,
  ONE_WEEK_MS,
  THREE_DAYS_MS,
  SERVICE_STATUSES,
  SETTLEMENT_STATUSES,
  BOOKING_CHANGE_REQUEST_STATUSES,
  ACCEPTED_BOOKING_INACTIVITY_REMINDER_STAGES,
  ACCEPTED_BOOKING_INACTIVITY_AUTO_CANCEL_REASON_CODE,
  PROVIDER_PAYOUT_TIME_ZONE,
  PROVIDER_PAYOUT_MORNING_START_HOUR,
  PROVIDER_PAYOUT_MORNING_END_HOUR,
  MIN_BOOKING_DURATION_MINUTES,
  parseDateInput,
  normalizeServiceStatus,
  normalizeSettlementStatus,
  normalizeBookingChangeRequestStatus,
  normalizeDurationMinutes,
  normalizeMinimumNoticeMinutes,
  isDurationMinutesInRange,
  deriveRequestedEndDateTime,
  meetsMinimumNotice,
  deriveAcceptDeadlineAt,
  deriveExpiresAt,
  deriveProviderPayoutEligibleAt,
  deriveLastMinuteWindowStartsAt,
  isWithinLastMinuteWindow,
  hasRequestedStartDateTimePassed,
  getAcceptedBookingInactivityStage,
  canReportBookingIssue,
  computeSettlementAmounts,
  canReleaseProviderPayout,
  isFirstWednesdayMorningPayoutWindow,
  evaluateAutoChargeEligibility,
  buildBookingSchedule,
  deriveLegacyBookingStatus,
  deriveLegacyIsPaid,
  canEditBooking,
  shouldResetBookingToAcceptedAfterFutureReschedule,
  hasBookingChangeRequestExpired,
  buildTransitionPatch,
  normalizeLegacyStatusUpdate,
  isProtectedLegacyClosureMutation,
};
