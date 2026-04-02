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

const MIN_BOOKING_DURATION_MINUTES = 5;
const MAX_BOOKING_DURATION_MINUTES = 180 * 24 * 60;
const ONE_HOUR_MS = 60 * 60 * 1000;
const ONE_DAY_MS = 24 * ONE_HOUR_MS;

function isValidDate(value) {
  return value instanceof Date && !Number.isNaN(value.getTime());
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

function isDurationMinutesInRange(value) {
  if (value === null || value === undefined) {
    return true;
  }

  return value >= MIN_BOOKING_DURATION_MINUTES && value <= MAX_BOOKING_DURATION_MINUTES;
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

  const defaultExpiryDate = new Date(createdAt.getTime() + ONE_DAY_MS);
  const acceptDeadlineAt = deriveAcceptDeadlineAt(createdAt, requestedStartDateTimeInput);

  if (!acceptDeadlineAt) {
    return defaultExpiryDate;
  }

  return acceptDeadlineAt.getTime() < defaultExpiryDate.getTime()
    ? acceptDeadlineAt
    : defaultExpiryDate;
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

function hasRequestedStartDateTimePassed(requestedStartDateTimeInput, now = new Date()) {
  const requestedStartDateTime = parseDateInput(requestedStartDateTimeInput);
  const normalizedNow = parseDateInput(now) || new Date();

  if (!requestedStartDateTime) {
    return false;
  }

  return requestedStartDateTime.getTime() <= normalizedNow.getTime();
}

function canReportBookingIssue(booking, now = new Date()) {
  const normalizedServiceStatus = normalizeServiceStatus(booking?.service_status, "pending_deposit");

  if (normalizedServiceStatus === "in_progress") {
    return true;
  }

  if (normalizedServiceStatus !== "accepted") {
    return false;
  }

  return hasRequestedStartDateTimePassed(
    booking?.requested_start_datetime ?? booking?.booking_start_datetime,
    now
  );
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

module.exports = {
  SERVICE_STATUSES,
  SETTLEMENT_STATUSES,
  MIN_BOOKING_DURATION_MINUTES,
  MAX_BOOKING_DURATION_MINUTES,
  parseDateInput,
  normalizeServiceStatus,
  normalizeSettlementStatus,
  normalizeDurationMinutes,
  normalizeMinimumNoticeMinutes,
  isDurationMinutesInRange,
  deriveRequestedEndDateTime,
  meetsMinimumNotice,
  deriveAcceptDeadlineAt,
  deriveExpiresAt,
  deriveLastMinuteWindowStartsAt,
  hasRequestedStartDateTimePassed,
  canReportBookingIssue,
  buildBookingSchedule,
  deriveLegacyBookingStatus,
  deriveLegacyIsPaid,
  canEditBooking,
  buildTransitionPatch,
  normalizeLegacyStatusUpdate,
};
