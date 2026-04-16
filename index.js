require('dotenv').config();

const bodyParser = require('body-parser');
const express = require('express');
const mysql = require('mysql2');
const axios = require('axios');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const { Storage } = require('@google-cloud/storage');
const { OAuth2Client } = require('google-auth-library');
const appleSigninAuth = require('apple-signin-auth');
const multer = require('multer');
const path = require('path');
const sharp = require('sharp');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const nodemailer = require('nodemailer');
const crypto = require("crypto");
const PDFDocument = require('pdfkit');
const fs = require('fs');
const os = require('os');
const { computeServiceResponseTime, computeServiceSuccessRate } = require('./src/serviceMetrics');
const { createVisionIdDetector } = require('./src/visionIdDetection');
const { getFirestore } = require('./src/firestore');
const {
  buildServiceSearchCandidateClause,
  buildServiceSearchPlan,
  buildServiceSearchRecommendedTaxonomy,
  buildServiceSearchSuggestions,
  rankServiceSearchCandidates,
} = require('./src/serviceSearchEngine');
const {
  AUTO_CHARGE_TOLERANCE_FACTOR,
  MIN_BOOKING_DURATION_MINUTES,
  MAX_BOOKING_DURATION_MINUTES,
  normalizeServiceStatus,
  normalizeSettlementStatus,
  normalizeBookingChangeRequestStatus,
  normalizeDurationMinutes,
  normalizeMinimumNoticeMinutes,
  isDurationMinutesInRange,
  buildBookingSchedule,
  deriveProviderPayoutEligibleAt,
  deriveLegacyBookingStatus,
  deriveLegacyIsPaid,
  meetsMinimumNotice,
  canReportBookingIssue,
  canEditBooking,
  hasBookingChangeRequestExpired,
  buildTransitionPatch,
  computeSettlementAmounts,
  evaluateAutoChargeEligibility,
  isWithinLastMinuteWindow,
  getAcceptedBookingInactivityStage,
  normalizeLegacyStatusUpdate,
  ACCEPTED_BOOKING_INACTIVITY_REMINDER_STAGES,
  ACCEPTED_BOOKING_INACTIVITY_AUTO_CANCEL_REASON_CODE,
} = require('./src/bookingDomain');
const IMG_WISDOM = 'https://storage.googleapis.com/wisdom-images/email_wisdom_logo.png';
const IMG_INSTA = 'https://storage.googleapis.com/wisdom-images/email_insta_logo.png';
const IMG_X = 'https://storage.googleapis.com/wisdom-images/email_x_logo.png';

const Stripe = require('stripe');
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET;
const APPLE_CLIENT_ID = process.env.APPLE_CLIENT_ID || 'com.rexos.Wisdom';
const APPLE_TEAM_ID = process.env.APPLE_TEAM_ID || '';
const APPLE_KEY_ID = process.env.APPLE_KEY_ID || '';
const APPLE_PRIVATE_KEY = typeof process.env.APPLE_PRIVATE_KEY === 'string'
  ? process.env.APPLE_PRIVATE_KEY.replace(/\\n/g, '\n').trim()
  : '';
const APPLE_PRIVATE_KEY_PATH = typeof process.env.APPLE_PRIVATE_KEY_PATH === 'string'
  ? process.env.APPLE_PRIVATE_KEY_PATH.trim()
  : '';
async function handleStripeRollbackIfNeeded(error) {
  try {
    if (error && error.payment_intent) {
      const intentId = typeof error.payment_intent === "string" ? error.payment_intent : error.payment_intent.id;
      await stripe.paymentIntents.cancel(intentId);
    }
  } catch (cancelErr) {
    console.error("Error cancelling payment intent:", cancelErr);
  }
}

async function syncPreBookingConversationUnlock({
  serviceId,
  clientUserId,
  providerUserId,
  bookingId,
}) {
  const normalizedServiceId = Number(serviceId);
  const normalizedClientUserId = Number(clientUserId);
  const normalizedProviderUserId = Number(providerUserId);

  if (
    !Number.isFinite(normalizedServiceId)
    || !Number.isFinite(normalizedClientUserId)
    || !Number.isFinite(normalizedProviderUserId)
  ) {
    return;
  }

  const firestore = getFirestore();
  if (!firestore) {
    return;
  }

  const conversationId = [normalizedClientUserId, normalizedProviderUserId]
    .sort((left, right) => left - right)
    .join('_');

  try {
    await firestore.collection('conversations').doc(conversationId).set({
      contextType: 'service_prebooking',
      serviceId: normalizedServiceId,
      serviceOwnerId: normalizedProviderUserId,
      preBookingMessageLimit: 5,
      bookingUnlocked: true,
      unlockedBookingId: Number.isFinite(Number(bookingId)) ? Number(bookingId) : null,
    }, { merge: true });
  } catch (error) {
    console.error('Error syncing pre-booking chat unlock:', {
      serviceId: normalizedServiceId,
      clientUserId: normalizedClientUserId,
      providerUserId: normalizedProviderUserId,
      bookingId,
      error: error.message,
    });
  }
}

const ACCEPTED_BOOKING_INACTIVITY_REASON_CODES = [
  ...ACCEPTED_BOOKING_INACTIVITY_REMINDER_STAGES.map((stage) => stage.reasonCode),
  ACCEPTED_BOOKING_INACTIVITY_AUTO_CANCEL_REASON_CODE,
];



const app = express();
// Heroku y otros proxies envían cabeceras X-Forwarded-*; esto permite que Express las use.
app.set('trust proxy', 1);
const port = process.env.PORT || 3000;

app.set('etag', false);

// Asegura que los webhooks de Stripe se procesen con el cuerpo sin parsear
app.use('/webhooks/stripe', express.raw({ type: 'application/json' }));

// Cabeceras de NO-CACHE en todo /api
app.use('/api', (req, res, next) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  res.set('Vary', 'Authorization');
  delete req.headers['if-none-match'];
  delete req.headers['if-modified-since'];
  res.removeHeader('ETag');
  res.removeHeader('Last-Modified');
  next();
});

// Middleware para parsear JSON.
app.use(bodyParser.json());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
const upload = multer({ storage: multer.memoryStorage() });
const uploadDni = multer({
  dest: os.tmpdir(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'image/jpeg' || file.mimetype === 'image/png') {
      cb(null, true);
    } else {
      cb(new Error('INVALID_FILE_TYPE'));
    }
  },
});

// CIDs for email images
const wisdomLogoCid = 'wisdom_logo';
const instagramLogoCid = 'instagram_logo';
const twitterLogoCid = 'twitter_logo';

// Configuración de transporte para enviar correos.
const emailProvider = (process.env.EMAIL_PROVIDER || (process.env.BREVO_API_KEY ? 'brevo' : 'smtp'))
  .trim()
  .toLowerCase();
const configuredEmailFrom = (process.env.EMAIL_FROM || '').trim();
const defaultSmtpEmailFrom = '"Wisdom" <wisdom.helpcontact@gmail.com>';
const configuredEmailReplyTo = (process.env.EMAIL_REPLY_TO || '').trim();
const bookingSupportEmail = (
  process.env.BOOKING_SUPPORT_EMAIL
  || process.env.SUPPORT_EMAIL
  || configuredEmailReplyTo
  || ''
).trim();
const brevoApiKey = (process.env.BREVO_API_KEY || '').trim();
const brevoApiBaseUrl = (process.env.BREVO_API_BASE_URL || 'https://api.brevo.com/v3').trim().replace(/\/+$/, '');
const emailPort = Number(process.env.EMAIL_PORT || 587);
const smtpTransporter = emailProvider === 'smtp'
  ? nodemailer.createTransport({
      host: process.env.EMAIL_HOST,
      port: emailPort,
      secure: emailPort === 465,
      auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS,
      },
    })
  : null;

function normalizeEmailRecipients(to) {
  if (Array.isArray(to)) {
    return to.map(value => String(value || '').trim()).filter(Boolean);
  }

  if (typeof to === 'string') {
    const normalized = to.trim();
    return normalized ? [normalized] : [];
  }

  return [];
}

function buildEmailPayload(message = {}) {
  const payload = {
    ...message,
    from: typeof message.from === 'string' && message.from.trim()
      ? message.from.trim()
      : (configuredEmailFrom || defaultSmtpEmailFrom),
  };

  if (!payload.replyTo && configuredEmailReplyTo) {
    payload.replyTo = configuredEmailReplyTo;
  }

  return payload;
}

function parseMailbox(value) {
  const normalized = String(value || '').trim();
  if (!normalized) {
    return { email: '', name: '' };
  }

  const match = normalized.match(/^(.+?)\s*<([^<>]+)>$/);
  if (match) {
    return {
      name: match[1].trim().replace(/^"|"$/g, ''),
      email: match[2].trim(),
    };
  }

  return { email: normalized, name: '' };
}

function toBrevoRecipient(value) {
  const mailbox = parseMailbox(value);
  if (!mailbox.email) {
    return null;
  }

  const recipient = { email: mailbox.email };
  if (mailbox.name) {
    recipient.name = mailbox.name;
  }
  return recipient;
}

async function sendEmailWithBrevo(message) {
  if (!brevoApiKey) {
    throw new Error('BREVO_API_KEY missing');
  }

  if (!configuredEmailFrom) {
    throw new Error('EMAIL_FROM missing');
  }

  const payload = buildEmailPayload(message);
  const recipients = normalizeEmailRecipients(payload.to).map(toBrevoRecipient).filter(Boolean);
  if (recipients.length === 0) {
    throw new Error('Email recipients missing');
  }

  const senderMailbox = parseMailbox(payload.from);
  if (!senderMailbox.email) {
    throw new Error('EMAIL_FROM invalid');
  }

  const brevoPayload = {
    sender: {
      email: senderMailbox.email,
    },
    to: recipients,
    subject: payload.subject,
  };

  if (senderMailbox.name) {
    brevoPayload.sender.name = senderMailbox.name;
  }

  if (typeof payload.text === 'string' && payload.text.trim()) {
    brevoPayload.textContent = payload.text;
  }

  if (typeof payload.html === 'string' && payload.html.trim()) {
    brevoPayload.htmlContent = payload.html;
  }

  if (payload.replyTo) {
    const replyToMailbox = parseMailbox(payload.replyTo);
    if (replyToMailbox.email) {
      brevoPayload.replyTo = { email: replyToMailbox.email };
      if (replyToMailbox.name) {
        brevoPayload.replyTo.name = replyToMailbox.name;
      }
    }
  }

  if (payload.headers && typeof payload.headers === 'object' && Object.keys(payload.headers).length > 0) {
    brevoPayload.headers = payload.headers;
  }

  try {
    const response = await axios.post(`${brevoApiBaseUrl}/smtp/email`, brevoPayload, {
      headers: {
        'accept': 'application/json',
        'api-key': brevoApiKey,
        'Content-Type': 'application/json',
      },
      timeout: 15000,
    });

    return response.data;
  } catch (error) {
    const details = error?.response?.data || error?.message || error;
    console.error('Brevo email error:', details);
    throw error;
  }
}

async function sendEmail(message) {
  const payload = buildEmailPayload(message);

  if (emailProvider === 'brevo') {
    return sendEmailWithBrevo(payload);
  }

  if (!smtpTransporter) {
    throw new Error(`Unsupported email provider: ${emailProvider}`);
  }

  return smtpTransporter.sendMail(payload);
}

if (emailProvider === 'smtp') {
  smtpTransporter.verify().catch(err => {
    console.error('Nodemailer configuration error:', err);
  });
} else if (emailProvider === 'brevo') {
  const missingBrevoConfig = [];
  if (!brevoApiKey) missingBrevoConfig.push('BREVO_API_KEY');
  if (!configuredEmailFrom) missingBrevoConfig.push('EMAIL_FROM');

  if (missingBrevoConfig.length > 0) {
    console.error(`Brevo configuration error: missing ${missingBrevoConfig.join(', ')}`);
  }
} else {
  console.error(`Unsupported EMAIL_PROVIDER configured: ${emailProvider}`);
}

const GUEST_ALLOWED_BROWSE_PATHS = [
  /^\/api\/suggested_professional$/,
  /^\/api\/category\/\d+\/services$/,
  /^\/api\/services$/,
  /^\/api\/service\/\d+$/,
  /^\/api\/suggestions$/,
];

function getRequestPath(req) {
  return `${req.baseUrl || ''}${req.path || ''}`;
}

function isGuestTokenPayload(payload) {
  return payload?.guest === true && payload?.token_type === 'guest';
}

function isGuestAllowedRequest(req) {
  const method = (req.method || '').toUpperCase();
  if (!['GET', 'HEAD', 'OPTIONS'].includes(method)) {
    return false;
  }

  const requestPath = getRequestPath(req);
  return GUEST_ALLOWED_BROWSE_PATHS.some((pattern) => pattern.test(requestPath));
}

// Middleware para verificar tokens JWT
function authenticateToken(req, res, next) {
  const h = req.headers['authorization'] || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : null;

  if (!token) {
    return res.status(401)
      .set('WWW-Authenticate', 'Bearer error="invalid_token", error_description="missing token"')
      .json({ error: 'missing_token' });
  }

  jwt.verify(token, process.env.JWT_SECRET, (err, payload) => {
    if (err) {
      if (err.name === 'TokenExpiredError') {
        return res.status(401)
          .set('WWW-Authenticate', 'Bearer error="invalid_token", error_description="token expired"')
          .json({ error: 'token_expired' });
      }
      return res.status(401)
        .set('WWW-Authenticate', 'Bearer error="invalid_token", error_description="invalid token"')
        .json({ error: 'invalid_token' });
    }

    if (isGuestTokenPayload(payload)) {
      if (!isGuestAllowedRequest(req)) {
        return res.status(403).json({ error: 'guest_not_allowed' });
      }

      req.user = {
        id: null,
        guest: true,
        token_type: 'guest',
        scope: payload.scope || 'browse:read',
        guest_session_id: payload.guest_session_id || null,
        exp: payload.exp,
        iat: payload.iat,
      };
      return next();
    }

    req.user = { id: payload.id || payload.sub, ...payload };
    next();
  });
}

//Formats dates and times in English (GB)
function formatDateTime(date) {
  return new Date(date).toLocaleString('en-GB', {
    dateStyle: 'medium',
    timeStyle: 'short'
  });
}

function formatDateTimeEs(date) {
  if (!date) return 'No especificada';
  const dt = new Date(date);
  if (Number.isNaN(dt.getTime())) return 'No especificada';
  return dt.toLocaleString('es-ES', {
    dateStyle: 'full',
    timeStyle: 'short'
  });
}

const SQL_DATE_ONLY_REGEX = /^(\d{4})-(\d{2})-(\d{2})$/;
const SQL_DATETIME_REGEX = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,6})?)?$/;

function formatUtcSqlDateTime(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return null;
  }

  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
}

function parseDateTimeInput(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  if (typeof value === 'number') {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const isoCandidate = trimmed.replace(' ', 'T');
  if (/[zZ]$|[+-]\d{2}:?\d{2}$/.test(isoCandidate)) {
    const parsed = new Date(isoCandidate);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const sqlDateTimeMatch = trimmed.match(SQL_DATETIME_REGEX);
  if (sqlDateTimeMatch) {
    const [, year, month, day, hour, minute, second = '00'] = sqlDateTimeMatch;
    const parsed = new Date(Date.UTC(
      Number(year),
      Number(month) - 1,
      Number(day),
      Number(hour),
      Number(minute),
      Number(second)
    ));
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const sqlDateMatch = trimmed.match(SQL_DATE_ONLY_REGEX);
  if (sqlDateMatch) {
    const [, year, month, day] = sqlDateMatch;
    const parsed = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), 0, 0, 0));
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const fallback = new Date(trimmed);
  return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function toUtcSqlDateTime(value) {
  const parsed = parseDateTimeInput(value);
  return parsed ? formatUtcSqlDateTime(parsed) : null;
}

function composeDisplayName({ firstName, surname, username, email }) {
  const fullName = [firstName, surname].filter(Boolean).join(' ').trim();
  if (fullName) return fullName;
  if (username) return username;
  if (email) return email;
  return 'No disponible';
}

function escapeHtml(value) {
  if (value === null || value === undefined) return '';
  return String(value).replace(/[&<>"']/g, (char) => {
    switch (char) {
      case '&': return '&amp;';
      case '<': return '&lt;';
      case '>': return '&gt;';
      case '"': return '&quot;';
      case "'": return '&#39;';
      default: return char;
    }
  });
}

//Conversión segura a céntimos
const toCents = (n) => {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.round(x * 100);
};

// Conversión de estados del PaymentIntent a nuestra convención interna
const mapStatus = (piStatus) => {
  switch (piStatus) {
    case 'succeeded': return 'succeeded';
    case 'processing': return 'processing';
    case 'requires_payment_method':
    case 'requires_action': return 'requires_action';
    default: return String(piStatus || 'unknown');
  }
};

// Helpers adicionales para pagos
const stableKey = (parts) => parts.join(':');
const BOOKING_CHANGE_REQUEST_TTL_MS = (
  Number.isFinite(Number(process.env.BOOKING_CHANGE_REQUEST_TTL_MS))
  && Number(process.env.BOOKING_CHANGE_REQUEST_TTL_MS) > 0
)
  ? Number(process.env.BOOKING_CHANGE_REQUEST_TTL_MS)
  : 24 * 60 * 60 * 1000;

// Redondeos consistentes con frontend (BookingScreen)
const round1 = (n) => {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.round(x * 10) / 10;
};
const round2 = (n) => {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.round(x * 100) / 100;
};

const CURRENCY_ALIASES = Object.freeze({
  RMB: 'CNY',
});

// Base EUR reference rates snapshot (ECB reference rates for 27 March 2026 where available).
// AED and SAR are inferred from their long-standing USD pegs, and MAD uses an approximate
// late-March 2026 market reference because the ECB table does not publish MAD on that page.
const DEFAULT_EUR_BASE_EXCHANGE_RATES = Object.freeze({
  EUR: 1,
  USD: 1.1517,
  GBP: 0.8672,
  JPY: 184.16,
  CAD: 1.5974,
  MXN: 20.7971,
  BRL: 6.0535,
  CNY: 7.9626,
  INR: 109.1945,
  AUD: 1.6731,
  SGD: 1.4831,
  HKD: 9.0223,
  NZD: 2.0019,
  KRW: 1740.79,
  PHP: 69.752,
  CHF: 0.9178,
  AED: 4.2291,
  SAR: 4.3189,
  TRY: 51.2001,
  ZAR: 19.7984,
  MAD: 10.82,
});
let eurBaseExchangeRates = { ...DEFAULT_EUR_BASE_EXCHANGE_RATES };
let exchangeRatesMeta = {
  base: 'EUR',
  source: 'fallback_snapshot',
  fetchedAt: null,
  effectiveDate: null,
  isFallback: true,
};
let exchangeRatesRefreshPromise = null;
const EXCHANGE_RATES_CACHE_TTL_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.EXCHANGE_RATES_CACHE_TTL_MS || 6 * 60 * 60 * 1000)
);
const SUPPORTED_DYNAMIC_CURRENCIES = Object.freeze(
  Object.keys(DEFAULT_EUR_BASE_EXCHANGE_RATES).filter((currencyCode) => currencyCode !== 'EUR')
);
const FRANKFURTER_V2_URL = (
  process.env.EXCHANGE_RATES_API_URL
  || 'https://api.frankfurter.dev/v2/rates'
).trim();
const CURRENCY_PATTERN_LOCALES = Object.freeze({
  EUR: 'es-ES',
  USD: 'en-US',
  GBP: 'en-GB',
  JPY: 'ja-JP',
  CAD: 'en-CA',
  MXN: 'es-MX',
  BRL: 'pt-BR',
  CNY: 'zh-CN',
  INR: 'hi-IN',
  AUD: 'en-AU',
  SGD: 'en-SG',
  HKD: 'zh-HK',
  NZD: 'en-NZ',
  KRW: 'ko-KR',
  PHP: 'en-PH',
  CHF: 'de-CH',
  AED: 'ar-AE',
  SAR: 'ar-SA',
  TRY: 'tr-TR',
  ZAR: 'en-ZA',
  MAD: 'ar-MA',
});

function getDefaultExchangeRates() {
  return { ...DEFAULT_EUR_BASE_EXCHANGE_RATES };
}

function getCurrentExchangeRates() {
  return eurBaseExchangeRates;
}

function normalizeExchangeRatesMap(rates) {
  const normalizedRates = getDefaultExchangeRates();
  normalizedRates.EUR = 1;

  if (!rates || typeof rates !== 'object') {
    return normalizedRates;
  }

  for (const [currencyCode, rawRate] of Object.entries(rates)) {
    const normalizedCurrency = normalizeCurrencyCode(currencyCode, null);
    const numericRate = Number(rawRate);
    if (!normalizedCurrency || normalizedCurrency === 'EUR' || !Number.isFinite(numericRate) || numericRate <= 0) {
      continue;
    }
    normalizedRates[normalizedCurrency] = numericRate;
  }

  return normalizedRates;
}

function parseFrankfurterRatesPayload(payload) {
  if (Array.isArray(payload)) {
    const rates = {};
    let effectiveDate = null;

    for (const entry of payload) {
      const quote = normalizeCurrencyCode(entry?.quote, null);
      const rate = Number(entry?.price ?? entry?.rate ?? entry?.value);
      if (quote && Number.isFinite(rate) && rate > 0) {
        rates[quote] = rate;
      }
      if (!effectiveDate && typeof entry?.date === 'string' && entry.date.trim()) {
        effectiveDate = entry.date.trim();
      }
    }

    return { rates, effectiveDate };
  }

  if (payload && typeof payload === 'object') {
    return {
      rates: payload.rates && typeof payload.rates === 'object' ? payload.rates : {},
      effectiveDate: typeof payload.date === 'string' ? payload.date.trim() : null,
    };
  }

  return { rates: {}, effectiveDate: null };
}

function isExchangeRatesCacheFresh() {
  if (!exchangeRatesMeta?.fetchedAt) {
    return false;
  }

  const fetchedAtMs = Date.parse(exchangeRatesMeta.fetchedAt);
  return Number.isFinite(fetchedAtMs) && (Date.now() - fetchedAtMs) < EXCHANGE_RATES_CACHE_TTL_MS;
}

async function refreshExchangeRates({ force = false } = {}) {
  if (!force && isExchangeRatesCacheFresh()) {
    return {
      ...exchangeRatesMeta,
      rates: { ...eurBaseExchangeRates },
    };
  }

  if (exchangeRatesRefreshPromise) {
    return exchangeRatesRefreshPromise;
  }

  exchangeRatesRefreshPromise = (async () => {
    const fallbackRates = getCurrentExchangeRates();
    const requestUrl = `${FRANKFURTER_V2_URL}?base=EUR&quotes=${SUPPORTED_DYNAMIC_CURRENCIES.join(',')}`;

    try {
      const response = await axios.get(requestUrl, {
        timeout: 8000,
        headers: { Accept: 'application/json' },
      });
      const { rates, effectiveDate } = parseFrankfurterRatesPayload(response.data);
      eurBaseExchangeRates = normalizeExchangeRatesMap(rates);
      exchangeRatesMeta = {
        base: 'EUR',
        source: 'frankfurter',
        fetchedAt: new Date().toISOString(),
        effectiveDate: effectiveDate || null,
        isFallback: false,
      };
    } catch (error) {
      console.error('Error refreshing exchange rates:', error?.response?.data || error?.message || error);
      eurBaseExchangeRates = normalizeExchangeRatesMap(fallbackRates);
      exchangeRatesMeta = {
        ...exchangeRatesMeta,
        base: 'EUR',
        source: exchangeRatesMeta?.fetchedAt ? exchangeRatesMeta.source : 'fallback_snapshot',
        fetchedAt: exchangeRatesMeta?.fetchedAt || new Date().toISOString(),
        effectiveDate: exchangeRatesMeta?.effectiveDate || null,
        isFallback: true,
      };
    } finally {
      exchangeRatesRefreshPromise = null;
    }

    return {
      ...exchangeRatesMeta,
      rates: { ...eurBaseExchangeRates },
    };
  })();

  return exchangeRatesRefreshPromise;
}

async function ensureExchangeRatesFresh({ force = false } = {}) {
  try {
    return await refreshExchangeRates({ force });
  } catch (error) {
    console.error('Error ensuring exchange rates freshness:', error);
    return {
      ...exchangeRatesMeta,
      rates: { ...eurBaseExchangeRates },
    };
  }
}

const STRIPE_ZERO_DECIMAL_CURRENCIES = new Set(['JPY', 'KRW']);

function getExchangeRatePerEuro(currency) {
  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');
  const rate = getCurrentExchangeRates()[normalizedCurrency];
  return Number.isFinite(rate) && rate > 0 ? rate : 1;
}

function convertAmount(amount, fromCurrency = 'EUR', toCurrency = 'EUR') {
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    return 0;
  }

  const normalizedFrom = normalizeCurrencyCode(fromCurrency, 'EUR');
  const normalizedTo = normalizeCurrencyCode(toCurrency, 'EUR');
  if (normalizedFrom === normalizedTo) {
    return round2(numericAmount);
  }

  const fromRate = getExchangeRatePerEuro(normalizedFrom);
  const toRate = getExchangeRatePerEuro(normalizedTo);
  return round2((numericAmount / fromRate) * toRate);
}

function getMinimumCommissionAmount(currency) {
  return round1(convertAmount(1, 'EUR', currency));
}

function getStripeMinorUnitFactor(currency) {
  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');
  return STRIPE_ZERO_DECIMAL_CURRENCIES.has(normalizedCurrency) ? 1 : 100;
}

function toMinorUnits(amount, currency = 'EUR') {
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    return 0;
  }

  return Math.round(numericAmount * getStripeMinorUnitFactor(currency));
}

function fromMinorUnits(amountMinorUnits, currency = 'EUR') {
  const numericAmount = Number(amountMinorUnits);
  if (!Number.isFinite(numericAmount)) {
    return 0;
  }

  return round2(numericAmount / getStripeMinorUnitFactor(currency));
}

function toStripeCurrencyCode(currency) {
  return normalizeCurrencyCode(currency, 'EUR').toLowerCase();
}

function sanitizeCurrencyDisplayValue(value) {
  return String(value || '')
    .replace(/[\u200e\u200f\u061c\u2066-\u2069]/gu, '')
    .replace(/\s+/gu, ' ')
    .trim();
}

function getCurrencyDisplaySymbol(currency, locale = 'es-ES') {
  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');
  const candidates = [];

  for (const currencyDisplay of ['narrowSymbol', 'symbol']) {
    try {
      const formatter = new Intl.NumberFormat(locale, {
        style: 'currency',
        currency: normalizedCurrency,
        currencyDisplay,
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      });
      const value = sanitizeCurrencyDisplayValue(
        formatter.formatToParts(0).find((part) => part.type === 'currency')?.value
      );
      if (value && value.toUpperCase() !== normalizedCurrency) {
        candidates.push({ value, currencyDisplay });
      }
    } catch (error) {}
  }

  if (!candidates.length) {
    return normalizedCurrency;
  }

  const preferredCandidates = candidates.some((candidate) => !/[A-Za-z]/.test(candidate.value))
    ? candidates.filter((candidate) => !/[A-Za-z]/.test(candidate.value))
    : candidates;

  preferredCandidates.sort((left, right) => (
    Number(right.currencyDisplay === 'narrowSymbol') - Number(left.currencyDisplay === 'narrowSymbol')
    || left.value.length - right.value.length
  ));

  return preferredCandidates[0].value;
}

function getCurrencyPatternLocale(currency, locale = 'es-ES') {
  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');
  return CURRENCY_PATTERN_LOCALES[normalizedCurrency] || locale;
}

function getCurrencyLayout(currency, locale = 'es-ES') {
  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');
  const patternLocale = getCurrencyPatternLocale(normalizedCurrency, locale);

  try {
    const parts = new Intl.NumberFormat(patternLocale, {
      style: 'currency',
      currency: normalizedCurrency,
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    }).formatToParts(1234.5);

    const currencyIndex = parts.findIndex((part) => part.type === 'currency');
    const firstNumberIndex = parts.findIndex((part) => ['integer', 'fraction'].includes(part.type));
    let lastNumberIndex = -1;
    for (let index = parts.length - 1; index >= 0; index -= 1) {
      if (['integer', 'fraction'].includes(parts[index]?.type)) {
        lastNumberIndex = index;
        break;
      }
    }

    const placement = currencyIndex >= 0 && firstNumberIndex >= 0 && currencyIndex < firstNumberIndex
      ? 'prefix'
      : 'suffix';
    const literalParts = placement === 'prefix'
      ? parts.slice(currencyIndex + 1, firstNumberIndex)
      : parts.slice(lastNumberIndex + 1, currencyIndex);
    const separator = literalParts.some((part) => /\s/u.test(part?.value || '')) ? ' ' : '';

    return { placement, separator };
  } catch (error) {
    return { placement: 'suffix', separator: ' ' };
  }
}

function formatCurrencyAmount(amount, currency = 'EUR', locale = 'es-ES') {
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    return 'No disponible';
  }

  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');
  const cleanSymbol = getCurrencyDisplaySymbol(normalizedCurrency, locale);
  const { placement, separator } = getCurrencyLayout(normalizedCurrency, locale);
  const absoluteAmount = Math.abs(numericAmount);
  const sign = numericAmount < 0 ? '-' : '';
  const formattedNumber = absoluteAmount.toLocaleString(locale, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });

  return placement === 'prefix'
    ? `${sign}${cleanSymbol}${separator}${formattedNumber}`.trim()
    : `${sign}${formattedNumber}${separator}${cleanSymbol}`.trim();
}

function buildCurrencyRateCaseExpression(currencyExpression) {
  const currentRates = getCurrentExchangeRates();
  const whenClauses = Object.entries(currentRates)
    .map(([currencyCode, rate]) => `WHEN '${currencyCode}' THEN ${rate}`)
    .join(' ');

  return `
    CASE UPPER(COALESCE(${currencyExpression}, 'EUR'))
      WHEN 'RMB' THEN ${currentRates.CNY || 1}
      ${whenClauses}
      ELSE 1
    END
  `.trim();
}

function parseQueryNumber(value) {
  if (value === undefined || value === null || value === '') return null;
  if (Array.isArray(value)) {
    for (let i = value.length - 1; i >= 0; i -= 1) {
      const parsed = parseQueryNumber(value[i]);
      if (parsed !== null) return parsed;
    }
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function parseQueryBoolean(value) {
  if (value === undefined || value === null) return false;
  if (Array.isArray(value) && value.length > 0) {
    return parseQueryBoolean(value[value.length - 1]);
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (!normalized) return false;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return false;
  }
  return Boolean(value);
}

function parseQueryStringArray(value) {
  const result = new Set();

  const process = (val) => {
    if (val === undefined || val === null) return;
    if (Array.isArray(val)) {
      val.forEach(process);
      return;
    }

    if (typeof val === 'string') {
      const trimmed = val.trim();
      if (!trimmed) return;

      if ((trimmed.startsWith('[') && trimmed.endsWith(']')) || (trimmed.startsWith('{') && trimmed.endsWith('}'))) {
        try {
          const parsed = JSON.parse(trimmed);
          process(parsed);
          return;
        } catch (e) {
          // Valor no es JSON válido, continuar con el flujo habitual.
        }
      }

      trimmed.split(',').forEach((part) => {
        const piece = part.trim();
        if (piece) result.add(piece);
      });
      return;
    }

    const str = String(val).trim();
    if (str) result.add(str);
  };

  process(value);
  return Array.from(result);
}

function extractServiceFilters(query) {
  const minPrice = parseQueryNumber(query.min_price ?? query.price_min);
  const maxPrice = parseQueryNumber(query.max_price ?? query.price_max);
  const durationMinutes = parseQueryNumber(query.duration_minutes ?? query.duration ?? query.duration_min ?? query.duration_mins);
  const maxActionRate = parseQueryNumber(query.max_action_rate ?? query.action_rate_max);
  const minRating = parseQueryNumber(query.min_rating ?? query.rating_min);
  const experienceYears = parseQueryStringArray(query.experience_years ?? query.experienceYears ?? query.experience_year ?? query.experienceYear);
  const requireCompany = parseQueryBoolean(query.require_company ?? query.company_profile ?? query.company_only);
  const requireVerified = parseQueryBoolean(query.require_verified ?? query.verified_only);
  const inPersonOnly = parseQueryBoolean(query.in_person_only ?? query.presential_only ?? query.onsite_only);
  const onlineOnly = parseQueryBoolean(query.online_only ?? query.remote_only);
  const languages = parseQueryStringArray(query.languages ?? query.language ?? query.lang);
  const categoryIds = parseQueryStringArray(query.category_ids ?? query.categoryIds ?? query.categories);
  const serviceFamilyIds = parseQueryStringArray(query.family_ids ?? query.familyIds ?? query.families ?? query.service_family_ids ?? query.serviceFamilyIds ?? query.service_family_id ?? query.family_id ?? query.family ?? query.serviceFamilyId);
  const serviceFamilyId = serviceFamilyIds.length > 0 ? parseQueryNumber(serviceFamilyIds[0]) : null;
  const maxDistanceKm = parseQueryNumber(query.max_distance_km ?? query.distance_km ?? query.distance ?? query.distance_max ?? query.maxDistance);
  const originLat = parseQueryNumber(query.origin_lat ?? query.originLat ?? query.latitude ?? query.lat);
  const originLng = parseQueryNumber(query.origin_lng ?? query.originLng ?? query.longitude ?? query.lng);

  return {
    minPrice,
    maxPrice,
    durationMinutes,
    maxActionRate,
    minRating,
    experienceYears,
    requireCompany,
    requireVerified,
    inPersonOnly,
    onlineOnly,
    languages,
    categoryIds,
    serviceFamilyIds,
    serviceFamilyId,
    maxDistanceKm,
    originLat,
    originLng,
  };
}

function buildComparablePriceExpression({
  priceAlias = 'price',
  durationMinutes = null,
  targetCurrency = 'EUR',
} = {}) {
  const safeDurationMinutes = Number.isFinite(durationMinutes) && durationMinutes > 0
    ? durationMinutes
    : null;
  const durationFactor = safeDurationMinutes !== null
    ? safeDurationMinutes / 60
    : 1;
  const targetRate = getExchangeRatePerEuro(targetCurrency);
  const sourceRateExpression = buildCurrencyRateCaseExpression(`${priceAlias}.currency`);
  const sourceAmountExpression = `
    CASE
      WHEN ${priceAlias}.price_type = 'fix' THEN ${priceAlias}.price
      WHEN ${priceAlias}.price_type = 'hour' THEN ${priceAlias}.price * ${durationFactor}
      ELSE NULL
    END
  `.trim();

  return `
    CASE
      WHEN ${priceAlias}.price_type IN ('fix', 'hour')
        THEN ((${sourceAmountExpression}) / NULLIF(${sourceRateExpression}, 0)) * ${targetRate}
      ELSE NULL
    END
  `.trim();
}

function buildBayesianRatingExpression({
  reviewAlias = 'review_data',
  priorCount = 5,
  priorMean = 4.0,
} = {}) {
  return `
    (
      (COALESCE(${reviewAlias}.review_count, 0) / (COALESCE(${reviewAlias}.review_count, 0) + ${priorCount}.0)) * COALESCE(${reviewAlias}.average_rating, 0)
      + (${priorCount}.0 / (COALESCE(${reviewAlias}.review_count, 0) + ${priorCount}.0)) * ${priorMean}
    )
  `.trim();
}

function buildServiceFilterClause(filters, {
  serviceAlias = 'service',
  priceAlias = 'price',
  userAlias = 'user_account',
  reviewAlias = 'review_data',
  distanceExpression = null,
  targetCurrency = 'EUR',
} = {}) {
  const clauses = [];
  const params = [];

  const {
    minPrice,
    maxPrice,
    durationMinutes,
    maxActionRate,
    minRating,
    experienceYears,
    requireCompany,
    requireVerified,
    inPersonOnly,
    onlineOnly,
    languages,
    categoryIds,
    serviceFamilyIds,
    serviceFamilyId,
    maxDistanceKm,
    originLat,
    originLng,
  } = filters;

  if (minPrice !== null || maxPrice !== null) {
    const comparablePriceExpression = buildComparablePriceExpression({ priceAlias, durationMinutes, targetCurrency });
    const knownPriceConditions = [`${priceAlias}.price_type IN ('fix', 'hour')`];

    if (minPrice !== null) {
      knownPriceConditions.push(`${comparablePriceExpression} >= ?`);
      params.push(minPrice);
    }
    if (maxPrice !== null) {
      knownPriceConditions.push(`${comparablePriceExpression} <= ?`);
      params.push(maxPrice);
    }

    const priceConditions = [`(${knownPriceConditions.join(' AND ')})`];
    if (maxPrice === null) {
      priceConditions.push(`${priceAlias}.price_type = 'budget'`);
    }

    clauses.push(`(${priceConditions.join(' OR ')})`);
  }

  if (Number.isFinite(maxActionRate)) {
    clauses.push(`(${serviceAlias}.action_rate IS NULL OR ${serviceAlias}.action_rate <= ?)`);
    params.push(maxActionRate);
  }

  if (Number.isFinite(minRating)) {
    clauses.push(`COALESCE(${reviewAlias}.average_rating, 0) >= ?`);
    params.push(minRating);
  }

  const numericExperienceYears = Array.isArray(experienceYears)
    ? experienceYears
      .map((value) => {
        const num = Number(value);
        return Number.isFinite(num) ? num : null;
      })
      .filter((value) => value !== null)
    : [];
  if (numericExperienceYears.length > 0) {
    const placeholders = numericExperienceYears.map(() => '?').join(', ');
    clauses.push(`${serviceAlias}.experience_years IN (${placeholders})`);
    params.push(...numericExperienceYears);
  }

  if (requireCompany) {
    clauses.push(`COALESCE(${serviceAlias}.is_individual, 1) = 0`);
  }

  if (requireVerified) {
    clauses.push(`${userAlias}.is_professional = 1`);
    clauses.push(`${userAlias}.is_verified = 1`);
  }

  if (inPersonOnly && !onlineOnly) {
    clauses.push(`${serviceAlias}.latitude IS NOT NULL`);
    clauses.push(`${serviceAlias}.longitude IS NOT NULL`);
  } else if (onlineOnly && !inPersonOnly) {
    clauses.push(`(
      ${serviceAlias}.latitude IS NULL
      OR ${serviceAlias}.longitude IS NULL
    )`);
  }

  if (languages && languages.length > 0) {
    const placeholders = languages.map(() => '?').join(', ');
    clauses.push(`EXISTS (SELECT 1 FROM service_language sl WHERE sl.service_id = ${serviceAlias}.id AND sl.language IN (${placeholders}))`);
    params.push(...languages);
  }

  const numericCategoryIds = Array.isArray(categoryIds)
    ? categoryIds
      .map((value) => {
        const num = Number(value);
        return Number.isFinite(num) ? num : null;
      })
      .filter((value) => value !== null)
    : [];
  const numericFamilyIds = Array.isArray(serviceFamilyIds)
    ? serviceFamilyIds
      .map((value) => {
        const num = Number(value);
        return Number.isFinite(num) ? num : null;
      })
      .filter((value) => value !== null)
    : [];

  if (numericCategoryIds.length > 0 || numericFamilyIds.length > 0 || Number.isFinite(serviceFamilyId)) {
    const taxonomyConditions = [];

    if (numericCategoryIds.length > 0) {
      const placeholders = numericCategoryIds.map(() => '?').join(', ');
      taxonomyConditions.push(`${serviceAlias}.service_category_id IN (${placeholders})`);
      params.push(...numericCategoryIds);
    }

    const effectiveFamilyIds = numericFamilyIds.length > 0
      ? numericFamilyIds
      : (Number.isFinite(serviceFamilyId) ? [serviceFamilyId] : []);

    if (effectiveFamilyIds.length > 0) {
      const placeholders = effectiveFamilyIds.map(() => '?').join(', ');
      taxonomyConditions.push(`${serviceAlias}.service_category_id IN (
        SELECT id FROM service_category WHERE service_family_id IN (${placeholders})
      )`);
      params.push(...effectiveFamilyIds);
    }

    if (taxonomyConditions.length > 0) {
      clauses.push(`(${taxonomyConditions.join(' OR ')})`);
    }
  }

  const hasDistanceContext = Number.isFinite(maxDistanceKm) && distanceExpression && distanceExpression.sql;
  const shouldApplyDistanceFilter = hasDistanceContext && !(onlineOnly && !inPersonOnly);
  if (shouldApplyDistanceFilter) {
    clauses.push(`(
      ${serviceAlias}.latitude IS NOT NULL
      AND ${serviceAlias}.longitude IS NOT NULL
      AND ${distanceExpression.sql} <= ?
    )`);
    params.push(...distanceExpression.params, maxDistanceKm);
  }

  if (!clauses.length) {
    return { sql: '', params: [] };
  }

  return {
    sql: ` AND ${clauses.join(' AND ')}`,
    params,
    distanceExpression,
  };
}

function buildDistanceExpression({
  latColumn,
  lngColumn,
  originLat,
  originLng,
}) {
  if (!Number.isFinite(originLat) || !Number.isFinite(originLng)) {
    return null;
  }

  const sql = `
    6371 * ACOS(
      LEAST(1, GREATEST(-1,
        COS(RADIANS(?)) * COS(RADIANS(${latColumn})) *
        COS(RADIANS(${lngColumn}) - RADIANS(?)) +
        SIN(RADIANS(?)) * SIN(RADIANS(${latColumn}))
      ))
    )
  `;

  return {
    sql: sql.trim(),
    params: [originLat, originLng, originLat],
  };
}

function normalizeCoordinateValue(value, {
  min = -180,
  max = 180,
} = {}) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const numericValue = Number(value);
  if (!Number.isFinite(numericValue) || numericValue < min || numericValue > max) {
    return null;
  }

  return numericValue;
}

function buildLocationObject(latitude, longitude) {
  const normalizedLatitude = normalizeCoordinateValue(latitude, { min: -90, max: 90 });
  const normalizedLongitude = normalizeCoordinateValue(longitude, { min: -180, max: 180 });

  if (normalizedLatitude === null || normalizedLongitude === null) {
    return null;
  }

  return {
    lat: normalizedLatitude,
    lng: normalizedLongitude,
  };
}

function calculateDistanceKmBetweenLocations(origin, target) {
  if (!origin || !target) {
    return null;
  }

  const earthRadiusKm = 6371;
  const toRadians = (value) => value * (Math.PI / 180);
  const deltaLat = toRadians(target.lat - origin.lat);
  const deltaLng = toRadians(target.lng - origin.lng);
  const originLat = toRadians(origin.lat);
  const targetLat = toRadians(target.lat);

  const haversine = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2)
    + Math.cos(originLat) * Math.cos(targetLat)
    * Math.sin(deltaLng / 2) * Math.sin(deltaLng / 2);
  const centralAngle = 2 * Math.atan2(Math.sqrt(haversine), Math.sqrt(1 - haversine));
  return earthRadiusKm * centralAngle;
}

function getBookingAddressRuleForService(serviceRow = {}) {
  const serviceLocation = buildLocationObject(serviceRow.latitude, serviceRow.longitude);
  const rawActionRate = Number(serviceRow.action_rate);
  const actionRate = Number.isFinite(rawActionRate) ? rawActionRate : null;

  if (!serviceLocation) {
    return {
      mode: 'hidden',
      actionRate,
      serviceLocation: null,
    };
  }

  if (actionRate === null || actionRate <= 0) {
    return {
      mode: 'fixed',
      actionRate,
      serviceLocation,
    };
  }

  return {
    mode: 'required',
    actionRate,
    serviceLocation,
  };
}

function hasInlineAddressFields(body = {}) {
  return Boolean(
    body.address_type
    && body.address_1
    && body.postal_code
    && body.city
    && body.state
    && body.country
  );
}

function extractAddressLocationFromSource(source = {}) {
  return buildLocationObject(
    source.latitude
    ?? source.address_latitude
    ?? source.lat
    ?? source.location?.lat
    ?? source.location?.latitude,
    source.longitude
    ?? source.address_longitude
    ?? source.lng
    ?? source.location?.lng
    ?? source.location?.longitude
  );
}

function assertBookingAddressRulesForService({
  serviceRow,
  addressRow = null,
  requestBody = {},
  allowUnknownAddressLocation = false,
}) {
  const rule = getBookingAddressRuleForService(serviceRow);
  const hasAddress = Boolean(addressRow) || hasInlineAddressFields(requestBody);

  if (rule.mode === 'hidden') {
    return {
      rule,
      addressLocation: null,
      distanceKm: null,
    };
  }

  if (!hasAddress) {
    const error = new Error(
      rule.mode === 'fixed'
        ? 'Este servicio tiene una ubicación fija y la reserva debe conservar esa dirección.'
        : 'Este servicio requiere una dirección dentro del radio de acción del profesional.'
    );
    error.statusCode = 400;
    throw error;
  }

  const addressLocation = extractAddressLocationFromSource(addressRow || requestBody);
  if (!addressLocation) {
    if (allowUnknownAddressLocation) {
      return {
        rule,
        addressLocation: null,
        distanceKm: null,
      };
    }

    const error = new Error('No se ha podido validar la dirección seleccionada. Vuelve a seleccionarla.');
    error.statusCode = 400;
    throw error;
  }

  const distanceKm = calculateDistanceKmBetweenLocations(rule.serviceLocation, addressLocation);
  if (!Number.isFinite(distanceKm)) {
    return {
      rule,
      addressLocation,
      distanceKm: null,
    };
  }

  if (rule.mode === 'required' && Number.isFinite(rule.actionRate) && distanceKm > rule.actionRate) {
    const error = new Error('La dirección indicada está fuera del radio de acción del profesional para este servicio.');
    error.statusCode = 400;
    throw error;
  }

  return {
    rule,
    addressLocation,
    distanceKm,
  };
}

function generateObjectName(originalName = '') {
  const extension = path.extname(originalName || '') || '';
  const uniqueId = typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : crypto.randomBytes(16).toString('hex');
  return `${Date.now()}_${uniqueId}${extension}`;
}

const ALLOWED_UPLOAD_IMAGE_TYPES = new Map([
  ['image/jpeg', '.jpg'],
  ['image/jpg', '.jpg'],
  ['image/pjpeg', '.jpg'],
  ['image/png', '.png'],
  ['image/webp', '.webp'],
]);

const MAX_UPLOAD_IMAGE_SIZE_BYTES = (() => {
  const envValue = Number(process.env.UPLOAD_SIGN_MAX_SIZE_BYTES);
  if (Number.isFinite(envValue) && envValue > 0) {
    return envValue;
  }
  return 5 * 1024 * 1024; // 5MB por defecto
})();

const DEFAULT_UPLOAD_SIGN_EXPIRATION_SECONDS = (() => {
  const envValue = Number(process.env.UPLOAD_SIGN_EXPIRES_SECONDS);
  if (Number.isFinite(envValue)) {
    return Math.min(Math.max(envValue, 60), 120);
  }
  return 90;
})();

function normalizeUploadPrefix(prefix) {
  const fallback = 'reg/';
  if (typeof prefix !== 'string' || !prefix.trim()) {
    return fallback;
  }

  const sanitized = prefix
    .trim()
    .replace(/^\/+/, '')
    .replace(/\.\.+/g, '')
    .replace(/[^A-Za-z0-9/_-]/g, '');

  if (!sanitized) {
    return fallback;
  }

  return sanitized.endsWith('/') ? sanitized : `${sanitized}/`;
}

const UPLOAD_SIGN_PREFIX = normalizeUploadPrefix(process.env.UPLOAD_SIGN_PREFIX);

function sanitizeUploadBaseName(name) {
  if (typeof name !== 'string') {
    return 'image';
  }

  const basename = path.posix.basename(name).replace(/\s+/g, '_');
  const parsed = path.parse(basename);
  const clean = parsed.name.replace(/[^A-Za-z0-9._-]/g, '').slice(0, 80);
  return clean || 'image';
}

function createUploadObjectKey(originalName, extension) {
  const base = sanitizeUploadBaseName(originalName);
  const uniqueId = typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : crypto.randomBytes(12).toString('hex');
  return `${UPLOAD_SIGN_PREFIX}${base}_${Date.now()}_${uniqueId}${extension}`;
}

function buildPublicUrl(bucketName, objectKey) {
  if (!bucketName || !objectKey) {
    return null;
  }

  const encodedKey = objectKey
    .split('/')
    .map(segment => encodeURIComponent(segment))
    .join('/');

  return `https://storage.googleapis.com/${bucketName}/${encodedKey}`;
}

function extractObjectNameFromUrl(url, bucketName) {
  if (!bucketName || typeof url !== 'string') {
    return null;
  }

  const trimmedUrl = url.trim();
  if (!trimmedUrl) {
    return null;
  }

  const prefixes = [
    `https://storage.googleapis.com/${bucketName}/`,
    `http://storage.googleapis.com/${bucketName}/`,
    `https://${bucketName}.storage.googleapis.com/`,
    `http://${bucketName}.storage.googleapis.com/`
  ];

  for (const prefix of prefixes) {
    if (trimmedUrl.startsWith(prefix)) {
      const objectPath = trimmedUrl.slice(prefix.length);
      return objectPath ? decodeURIComponent(objectPath) : null;
    }
  }

  return null;
}

function resolveImageObjectName(candidate, url, bucketName) {
  if (typeof candidate === 'string') {
    const trimmed = candidate.trim();
    if (trimmed.length > 0) {
      return trimmed;
    }
  }

  const fromUrl = extractObjectNameFromUrl(url, bucketName);
  return fromUrl || null;
}

function resolveImageUrl(image) {
  if (!image || typeof image !== 'object') {
    return null;
  }
  const raw = image.url ?? image.image_url ?? image.imageUrl;
  if (typeof raw !== 'string') {
    return null;
  }
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : null;
}

// Recalcula base, comisión y total como en BookingScreen.pricing
function computePricing({ priceType, unitPrice, durationMinutes, currency = 'EUR' }) {
  const type = String(priceType || '').toLowerCase();
  const unit = Number.parseFloat(unitPrice) || 0;
  const minutes = Math.max(0, Math.round(Number(durationMinutes) || 0));
  const hours = minutes / 60;
  const pricingCurrency = normalizeCurrencyCode(currency, 'EUR');

  let base = 0;
  if (type === 'hour') base = unit * hours;
  else base = unit;
  base = round2(base);

  const minimumCommission = getMinimumCommissionAmount(pricingCurrency);
  const commission = Math.max(minimumCommission, round1(base * 0.1));

  const shouldNullFinal = (type === 'hour' || type === 'budget') && minutes <= 0;
  const final = round2(base + commission);

  return { base, commission, final, minutes };
}

function computeBookingPricingSnapshot({ priceType, unitPrice, durationMinutes, currency = 'EUR' }) {
  const normalizedPriceType = String(priceType || '').trim().toLowerCase();
  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');
  const normalizedMinutes = Math.max(0, Math.round(Number(durationMinutes) || 0));
  const normalizedUnitPrice = Number.parseFloat(unitPrice) || 0;

  let baseAmount = 0;
  if (normalizedPriceType === 'hour') {
    baseAmount = normalizedUnitPrice * (normalizedMinutes / 60);
  } else if (normalizedPriceType === 'fix' || normalizedPriceType === 'budget') {
    baseAmount = normalizedUnitPrice;
  } else {
    baseAmount = normalizedUnitPrice;
  }

  const roundedBaseAmount = round2(baseAmount);
  const roundedCommissionAmount = Math.max(
    getMinimumCommissionAmount(normalizedCurrency),
    round1(roundedBaseAmount * 0.1)
  );
  const shouldNullFinalAmount =
    (normalizedPriceType === 'hour' && normalizedMinutes <= 0) ||
    (normalizedPriceType === 'budget' && roundedBaseAmount <= 0);

  return {
    type: normalizedPriceType,
    currency: normalizedCurrency,
    minutes: normalizedMinutes,
    base: roundedBaseAmount,
    commission: roundedCommissionAmount,
    final: shouldNullFinalAmount ? null : round2(roundedBaseAmount + roundedCommissionAmount),
  };
}

function normalizeNullableInteger(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const parsedValue = Number.parseInt(value, 10);
  return Number.isInteger(parsedValue) ? parsedValue : null;
}

function toDbDateTime(value) {
  if (value instanceof Date) {
    return formatUtcSqlDateTime(value);
  }

  return value;
}

function normalizeBooleanInput(value, fallback = false) {
  if (value === null || typeof value === 'undefined' || value === '') {
    return fallback;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'number') {
    return value !== 0;
  }

  const normalizedValue = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalizedValue)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(normalizedValue)) {
    return false;
  }

  return fallback;
}

function formatStoredCardExpiry(expMonth, expYear) {
  const monthNumber = Number.parseInt(expMonth, 10);
  const yearNumber = Number.parseInt(expYear, 10);
  if (!Number.isInteger(monthNumber) || monthNumber < 1 || monthNumber > 12 || !Number.isInteger(yearNumber)) {
    return '';
  }

  const normalizedMonth = String(monthNumber).padStart(2, '0');
  const normalizedYear = String(yearNumber).slice(-2).padStart(2, '0');
  return `${normalizedMonth}/${normalizedYear}`;
}

function parseStoredCardExpiry(expiryValue) {
  const rawValue = typeof expiryValue === 'string' ? expiryValue.trim() : '';
  const match = rawValue.match(/^(\d{2})\/(\d{2,4})$/);
  if (!match) {
    return { month: null, year: null };
  }

  return {
    month: match[1],
    year: match[2],
  };
}

function mapStoredPaymentMethodForApi(row = {}) {
  const stripePaymentMethodId = typeof row.customer_payment_method_stripe_id === 'string' && row.customer_payment_method_stripe_id.startsWith('pm_')
    ? row.customer_payment_method_stripe_id
    : (
      typeof row.payment_type === 'string' && row.payment_type.startsWith('pm_')
        ? row.payment_type
        : null
    );

  if (!stripePaymentMethodId) {
    return null;
  }

  const parsedExpiry = parseStoredCardExpiry(row.expiry_date);
  const last4 = row.card_number ? String(row.card_number).slice(-4) : null;

  return {
    record_id: normalizeNullableInteger(row.selected_customer_payment_method_id ?? row.id),
    id: stripePaymentMethodId,
    last4,
    brand: typeof row.brand === 'string' && row.brand.trim().length > 0 ? row.brand.trim() : null,
    expiryMonth: parsedExpiry.month,
    expiryYear: parsedExpiry.year,
    expiryLabel: row.expiry_date || null,
    isSaved: row.is_safed === true || row.is_safed === 1,
    isDefault: row.is_default === true || row.is_default === 1,
    provider: row.provider || 'STRIPE',
  };
}

function mapBookingPaymentSummaryForApi(row = null, fallbackCurrency = 'EUR') {
  if (!row) {
    return null;
  }

  const summaryCurrency = normalizeCurrencyCode(row.currency, fallbackCurrency);
  const amount = row.amount_cents !== null && row.amount_cents !== undefined
    ? fromMinorUnits(row.amount_cents, summaryCurrency)
    : null;

  return {
    type: row.type || null,
    status: row.status || null,
    currency: summaryCurrency,
    amount,
    payment_method_id: row.payment_method_id || null,
    last4: row.payment_method_last4 || null,
    brand: typeof row.payment_method_brand === 'string' && row.payment_method_brand.trim().length > 0
      ? row.payment_method_brand.trim()
      : null,
  };
}

function parseJsonObject(value) {
  if (!value) {
    return null;
  }

  if (typeof value === 'object' && !Array.isArray(value)) {
    return value;
  }

  if (typeof value !== 'string') {
    return null;
  }

  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? parsed
      : null;
  } catch {
    return null;
  }
}

function mapEmbeddedBookingChangeRequestForApi(row = {}) {
  const changeRequestId = normalizeNullableInteger(row.change_request_id);
  if (!changeRequestId) {
    return null;
  }

  return {
    id: changeRequestId,
    booking_id: normalizeNullableInteger(row.booking_id ?? row.id),
    requested_by_user_id: normalizeNullableInteger(row.change_request_requested_by_user_id),
    target_user_id: normalizeNullableInteger(row.change_request_target_user_id),
    status: normalizeBookingChangeRequestStatus(row.change_request_status, 'pending'),
    changes: parseJsonObject(row.change_request_changes_json),
    message: typeof row.change_request_message === 'string' && row.change_request_message.trim()
      ? row.change_request_message.trim()
      : null,
    created_at: row.change_request_created_at ?? null,
    resolved_at: row.change_request_resolved_at ?? null,
  };
}

function mapBookingRecordForApi(row = {}) {
  const normalizedServiceStatus = normalizeServiceStatus(row.service_status, 'pending_deposit');
  const normalizedSettlementStatus = normalizeSettlementStatus(row.settlement_status, 'none');
  const effectivePriceType = String(row.price_type_snapshot || row.price_type || '').trim().toLowerCase();
  const serviceCurrency = normalizeCurrencyCode(
    row.service_currency_snapshot,
    normalizeCurrencyCode(row.currency, normalizeCurrencyCode(row.service_currency, 'EUR'))
  );
  const requestedStartDateTime = row.requested_start_datetime ?? row.booking_start_datetime ?? null;
  const requestedEndDateTime = row.requested_end_datetime ?? row.booking_end_datetime ?? null;
  const requestedDurationMinutes = row.requested_duration_minutes ?? row.service_duration ?? null;
  const normalizedClosureStatus = typeof row.closure_status === 'string'
    ? row.closure_status.trim().toLowerCase()
    : null;
  const closureProposalId = row.closure_proposal_id ?? null;
  const closureTotalAmountCents = row.proposed_total_amount_cents ?? row.closure_proposed_total_amount_cents ?? null;
  const closureCommissionAmountCents = row.proposed_commission_amount_cents ?? row.closure_proposed_commission_amount_cents ?? null;
  const closureBaseAmountCents = row.proposed_base_amount_cents ?? row.closure_proposed_base_amount_cents ?? null;
  const closureDepositAlreadyPaidAmountCents = row.deposit_already_paid_amount_cents ?? row.closure_deposit_already_paid_amount_cents ?? row.deposit_amount_cents_snapshot ?? null;
  const closureAmountDueFromClientCents = row.amount_due_from_client_cents ?? row.closure_amount_due_from_client_cents ?? null;
  const closureAmountToRefundCents = row.amount_to_refund_cents ?? row.closure_amount_to_refund_cents ?? null;
  const closureProviderPayoutAmountCents = row.provider_payout_amount_cents ?? row.closure_provider_payout_amount_cents ?? null;
  const closurePlatformAmountCents = row.platform_amount_cents ?? row.closure_platform_amount_cents ?? null;
  const estimatedTotalPrice = row.estimated_total_amount_cents !== null && row.estimated_total_amount_cents !== undefined
    ? fromMinorUnits(row.estimated_total_amount_cents, serviceCurrency)
    : null;
  const estimatedCommission = row.estimated_commission_amount_cents !== null && row.estimated_commission_amount_cents !== undefined
    ? fromMinorUnits(row.estimated_commission_amount_cents, serviceCurrency)
    : null;
  const estimatedBasePrice = row.estimated_base_amount_cents !== null && row.estimated_base_amount_cents !== undefined
    ? fromMinorUnits(row.estimated_base_amount_cents, serviceCurrency)
    : null;
  const closureTotalPrice = closureTotalAmountCents !== null && closureTotalAmountCents !== undefined
    ? fromMinorUnits(closureTotalAmountCents, serviceCurrency)
    : null;
  const closureCommission = closureCommissionAmountCents !== null && closureCommissionAmountCents !== undefined
    ? fromMinorUnits(closureCommissionAmountCents, serviceCurrency)
    : null;
  const closureBasePrice = closureBaseAmountCents !== null && closureBaseAmountCents !== undefined
    ? fromMinorUnits(closureBaseAmountCents, serviceCurrency)
    : null;
  const closureDepositAlreadyPaid = closureDepositAlreadyPaidAmountCents !== null && closureDepositAlreadyPaidAmountCents !== undefined
    ? fromMinorUnits(closureDepositAlreadyPaidAmountCents, serviceCurrency)
    : null;
  const closureAmountDueFromClient = closureAmountDueFromClientCents !== null && closureAmountDueFromClientCents !== undefined
    ? fromMinorUnits(closureAmountDueFromClientCents, serviceCurrency)
    : null;
  const closureAmountToRefund = closureAmountToRefundCents !== null && closureAmountToRefundCents !== undefined
    ? fromMinorUnits(closureAmountToRefundCents, serviceCurrency)
    : null;
  const closureProviderPayoutAmount = closureProviderPayoutAmountCents !== null && closureProviderPayoutAmountCents !== undefined
    ? fromMinorUnits(closureProviderPayoutAmountCents, serviceCurrency)
    : null;
  const closurePlatformAmount = closurePlatformAmountCents !== null && closurePlatformAmountCents !== undefined
    ? fromMinorUnits(closurePlatformAmountCents, serviceCurrency)
    : null;
  const closureAutoChargeEligible = row.auto_charge_eligible === true || row.auto_charge_eligible === 1;
  const hasActiveClosureProposal = normalizedClosureStatus
    ? normalizedClosureStatus === 'active'
    : closureProposalId !== null && closureProposalId !== undefined;
  const hasOpenIssueReport = row.open_issue_report_id !== null
    && row.open_issue_report_id !== undefined;
  const latestChangeRequest = mapEmbeddedBookingChangeRequestForApi(row);
  const hasPendingChangeRequest = latestChangeRequest?.status === 'pending';
  const baseCanEditBooking = canEditBooking({
    service_status: normalizedServiceStatus,
    settlement_status: normalizedSettlementStatus,
  });
  const isActionableChangeRequest = hasPendingChangeRequest && baseCanEditBooking;
  const needsClosureInput = (normalizedServiceStatus === 'finished' || normalizedServiceStatus === 'in_progress')
    && normalizedSettlementStatus === 'none'
    && ['hour', 'budget'].includes(effectivePriceType)
    && !hasActiveClosureProposal;

  let finalPrice = row.final_price ?? null;
  if (finalPrice === null) {
    if (closureTotalPrice !== null) {
      finalPrice = closureTotalPrice;
    } else if (estimatedTotalPrice !== null) {
      finalPrice = estimatedTotalPrice;
    }
  }

  let commission = row.commission ?? null;
  if (commission === null) {
    if (closureCommission !== null) {
      commission = closureCommission;
    } else if (estimatedCommission !== null) {
      commission = estimatedCommission;
    }
  }

  let basePrice = row.base_price ?? null;
  if (basePrice === null) {
    if (closureBasePrice !== null) {
      basePrice = closureBasePrice;
    } else if (estimatedBasePrice !== null) {
      basePrice = estimatedBasePrice;
    }
  }

  const selectedCustomerPaymentMethod = mapStoredPaymentMethodForApi(row);

  return {
    ...row,
    id: row.id ?? row.booking_id ?? null,
    booking_id: row.booking_id ?? row.id ?? null,
    user_id: row.user_id ?? row.client_user_id ?? null,
    client_user_id: row.client_user_id ?? row.user_id ?? null,
    service_user_id: row.service_user_id ?? row.provider_user_id_snapshot ?? null,
    service_status: normalizedServiceStatus,
    settlement_status: normalizedSettlementStatus,
    booking_status: deriveLegacyBookingStatus({
      serviceStatus: normalizedServiceStatus,
      settlementStatus: normalizedSettlementStatus,
      cancellationReasonCode: row.cancellation_reason_code,
    }),
    is_paid: deriveLegacyIsPaid(normalizedSettlementStatus) ? 1 : 0,
    booking_start_datetime: requestedStartDateTime,
    booking_end_datetime: requestedEndDateTime,
    service_duration: requestedDurationMinutes,
    requested_start_datetime: requestedStartDateTime,
    requested_end_datetime: requestedEndDateTime,
    requested_duration_minutes: requestedDurationMinutes,
    order_datetime: row.order_datetime ?? row.created_at ?? null,
    final_price: finalPrice,
    commission,
    base_price: basePrice,
    estimated_total_price: estimatedTotalPrice,
    estimated_commission: estimatedCommission,
    estimated_base_price: estimatedBasePrice,
    closure_proposal_id: closureProposalId,
    closure_status: normalizedClosureStatus,
    closure_sent_at: row.closure_sent_at ?? null,
    closure_accepted_at: row.closure_accepted_at ?? null,
    closure_rejected_at: row.closure_rejected_at ?? null,
    closure_revoked_at: row.closure_revoked_at ?? null,
    closure_total_price: closureTotalPrice,
    closure_commission: closureCommission,
    closure_base_price: closureBasePrice,
    closure_deposit_already_paid: closureDepositAlreadyPaid,
    closure_amount_due_from_client: closureAmountDueFromClient,
    closure_amount_to_refund: closureAmountToRefund,
    closure_provider_payout_amount: closureProviderPayoutAmount,
    closure_platform_amount: closurePlatformAmount,
    closure_final_duration_minutes: row.proposed_final_duration_minutes ?? null,
    closure_zero_charge_mode: row.zero_charge_mode === true || row.zero_charge_mode === 1,
    closure_auto_charge_eligible: closureAutoChargeEligible,
    closure_auto_charge_scheduled_at: row.auto_charge_scheduled_at ?? null,
    has_active_closure_proposal: hasActiveClosureProposal,
    has_open_issue_report: hasOpenIssueReport,
    has_pending_change_request: hasPendingChangeRequest,
    has_actionable_change_request: isActionableChangeRequest,
    latest_change_request: latestChangeRequest,
    needs_closure_input: needsClosureInput,
    can_edit: baseCanEditBooking,
    selected_customer_payment_method: selectedCustomerPaymentMethod,
    price_type: row.price_type ?? row.price_type_snapshot ?? null,
    currency: normalizeCurrencyCode(row.currency, serviceCurrency),
    location: buildLocationObject(
      row.address_latitude ?? row.latitude ?? null,
      row.address_longitude ?? row.longitude ?? null
    ),
  };
}

function buildBookingStatusFilter(status, tableAlias = 'b') {
  const normalizedStatus = String(status || '').trim().toLowerCase().replace(/-/g, '_');
  if (!normalizedStatus) {
    return { clause: '', params: [] };
  }

  switch (normalizedStatus) {
    case 'completed':
      return { clause: ` AND ${tableAlias}.service_status = ?`, params: ['finished'] };
    case 'rejected':
      return {
        clause: ` AND ${tableAlias}.service_status = 'canceled' AND COALESCE(${tableAlias}.cancellation_reason_code, '') IN ('rejected','provider_rejected','rejected_by_provider')`,
        params: [],
      };
    case 'paid':
      return { clause: ` AND ${tableAlias}.settlement_status = ?`, params: ['paid'] };
    case 'payment_failed':
      return { clause: ` AND ${tableAlias}.settlement_status = ?`, params: ['payment_failed'] };
    default: {
      const mappedLegacyStatus = normalizeLegacyStatusUpdate(normalizedStatus);
      if (mappedLegacyStatus.serviceStatus) {
        return {
          clause: ` AND ${tableAlias}.service_status = ?`,
          params: [mappedLegacyStatus.serviceStatus],
        };
      }
      if (mappedLegacyStatus.settlementStatus) {
        return {
          clause: ` AND ${tableAlias}.settlement_status = ?`,
          params: [mappedLegacyStatus.settlementStatus],
        };
      }
      return { clause: '', params: [] };
    }
  }
}

async function insertBookingStatusHistory(connection, {
  bookingId,
  fromServiceStatus,
  toServiceStatus,
  fromSettlementStatus,
  toSettlementStatus,
  changedByUserId = null,
  reasonCode = null,
  note = null,
}) {
  await connection.query(
    `INSERT INTO booking_status_history
      (booking_id, from_service_status, to_service_status, from_settlement_status, to_settlement_status, changed_by_user_id, reason_code, note)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      bookingId,
      fromServiceStatus || null,
      toServiceStatus,
      fromSettlementStatus || null,
      toSettlementStatus,
      changedByUserId || null,
      reasonCode || null,
      note || null,
    ]
  );
}

async function transitionBookingStateRecord(connection, currentBooking, {
  nextServiceStatus,
  nextSettlementStatus,
  changedByUserId = null,
  reasonCode = null,
  note = null,
  extraPatch = {},
}) {
  const transition = buildTransitionPatch(currentBooking, {
    nextServiceStatus,
    nextSettlementStatus,
  });
  const patch = { ...transition.patch, ...extraPatch };
  const patchEntries = Object.entries(patch).filter(([, value]) => typeof value !== 'undefined');

  if (patchEntries.length === 0) {
    return {
      ...transition,
      appliedPatch: {},
    };
  }

  const setClause = patchEntries.map(([columnName]) => `${columnName} = ?`).join(', ');
  const values = patchEntries.map(([, value]) => toDbDateTime(value));

  await connection.query(
    `UPDATE booking SET ${setClause} WHERE id = ?`,
    [...values, currentBooking.id]
  );

  if (transition.changed) {
    await insertBookingStatusHistory(connection, {
      bookingId: currentBooking.id,
      fromServiceStatus: transition.fromServiceStatus,
      toServiceStatus: transition.toServiceStatus,
      fromSettlementStatus: transition.fromSettlementStatus,
      toSettlementStatus: transition.toSettlementStatus,
      changedByUserId,
      reasonCode,
      note,
    });
  }

  return {
    ...transition,
    appliedPatch: patch,
  };
}

function canAutoExpireRequestedBooking(booking, now = new Date()) {
  if (normalizeServiceStatus(booking?.service_status, 'pending_deposit') !== 'requested') {
    return false;
  }

  const normalizedNow = parseDateTimeInput(now) || new Date();
  const expiresAt = parseDateTimeInput(booking?.expires_at)
    || buildBookingSchedule({
      createdAt: booking?.order_datetime ?? booking?.created_at,
      requestedStartDateTime: booking?.requested_start_datetime,
      requestedDurationMinutes: booking?.requested_duration_minutes,
    }).expiresAt;

  if (!expiresAt) {
    return false;
  }

  return expiresAt.getTime() <= normalizedNow.getTime();
}

function assertBookingStatusUpdateAllowed({
  booking,
  nextServiceStatus,
  isClientOwner,
  isProviderOwner,
  isStaff,
  cancellationReasonCode = null,
}) {
  const normalizedCurrentServiceStatus = normalizeServiceStatus(
    booking?.service_status,
    'pending_deposit'
  );
  const normalizedNextServiceStatus = normalizeServiceStatus(
    nextServiceStatus,
    normalizedCurrentServiceStatus
  );
  const normalizedCancellationReasonCode = String(cancellationReasonCode || '')
    .trim()
    .toLowerCase()
    .replace(/-/g, '_');
  const throwValidationError = (message) => {
    const error = new Error(message);
    error.statusCode = 400;
    throw error;
  };

  if (normalizedCurrentServiceStatus === normalizedNextServiceStatus || isStaff) {
    return {
      normalizedCurrentServiceStatus,
      normalizedNextServiceStatus,
    };
  }

  switch (normalizedNextServiceStatus) {
    case 'accepted':
      if (!isProviderOwner) {
        throwValidationError('Solo el profesional puede aceptar esta solicitud.');
      }
      if (normalizedCurrentServiceStatus !== 'requested') {
        throwValidationError('Solo se puede aceptar una solicitud pendiente.');
      }
      break;
    case 'in_progress':
      if (!isProviderOwner) {
        throwValidationError('Solo el profesional puede iniciar el servicio.');
      }
      if (normalizedCurrentServiceStatus !== 'accepted') {
        throwValidationError('El servicio solo puede iniciarse cuando la reserva está aceptada.');
      }
      break;
    case 'finished':
      if (!isClientOwner && !isProviderOwner) {
        throwValidationError('No autorizado para finalizar la reserva.');
      }
      if (normalizedCurrentServiceStatus !== 'in_progress') {
        throwValidationError('El servicio solo puede finalizarse cuando está en progreso.');
      }
      break;
    case 'expired':
      throwValidationError('El estado expired solo puede aplicarse automáticamente.');
    case 'canceled':
      if (!isClientOwner && !isProviderOwner) {
        throwValidationError('No autorizado para cancelar la reserva.');
      }
      if (
        normalizedCancellationReasonCode === 'rejected'
        || normalizedCancellationReasonCode === 'provider_rejected'
        || normalizedCancellationReasonCode === 'rejected_by_provider'
      ) {
        if (!isProviderOwner) {
          throwValidationError('Solo el profesional puede rechazar una solicitud.');
        }
        if (normalizedCurrentServiceStatus !== 'requested') {
          throwValidationError('Solo se puede rechazar una solicitud pendiente.');
        }
      } else if (
        normalizedCurrentServiceStatus === 'accepted'
        && hasRequestedStartDateTimePassed(booking?.requested_start_datetime)
      ) {
        throwValidationError('Una vez superada la hora de inicio ya no se puede cancelar la reserva desde este estado.');
      }
      break;
    default:
      break;
  }

  return {
    normalizedCurrentServiceStatus,
    normalizedNextServiceStatus,
  };
}

function canInitiateDepositRefund(paymentRow) {
  const normalizedPaymentStatus = String(paymentRow?.status || '').trim().toLowerCase();
  return Boolean(paymentRow?.payment_intent_id) && normalizedPaymentStatus === 'succeeded';
}

async function listStripeTransfersByGroup(transferGroup) {
  const normalizedTransferGroup = typeof transferGroup === 'string' ? transferGroup.trim() : '';
  if (!normalizedTransferGroup) {
    return [];
  }

  const transfers = [];
  let startingAfter = null;

  while (true) {
    const page = await stripe.transfers.list({
      transfer_group: normalizedTransferGroup,
      limit: 100,
      ...(startingAfter ? { starting_after: startingAfter } : {}),
    });
    const pageTransfers = Array.isArray(page?.data) ? page.data : [];
    if (pageTransfers.length === 0) {
      break;
    }
    transfers.push(...pageTransfers);
    if (!page?.has_more) {
      break;
    }
    startingAfter = pageTransfers[pageTransfers.length - 1].id;
  }

  return transfers;
}

function getTransferNetAmountCents(transfer) {
  const totalAmount = Number(transfer?.amount || 0);
  const reversedAmount = Number(transfer?.amount_reversed || 0);
  return Math.max(0, totalAmount - reversedAmount);
}

function filterBookingTransfersByPurpose(transfers, purpose, destinationAccountId = null, { includeUnscopedTransfers = false } = {}) {
  return (transfers || []).filter((transfer) => {
    const transferPurpose = String(transfer?.metadata?.booking_purpose || '').trim().toLowerCase();
    const samePurpose = transferPurpose === String(purpose || '').trim().toLowerCase()
      || (includeUnscopedTransfers && !transferPurpose);
    const sameDestination = !destinationAccountId || transfer?.destination === destinationAccountId;
    return samePurpose && sameDestination;
  });
}

async function reverseBookingTransfersIfNeeded({
  transferGroup,
  amountCents,
  purpose = null,
  destinationAccountId = null,
  metadata = {},
}) {
  const normalizedAmountCents = Math.max(0, Math.round(Number(amountCents || 0)));
  if (!normalizedAmountCents) {
    return { reversedAmountCents: 0, reversals: [] };
  }

  const transfers = purpose
    ? filterBookingTransfersByPurpose(await listStripeTransfersByGroup(transferGroup), purpose, destinationAccountId)
    : await listStripeTransfersByGroup(transferGroup);
  let remainingAmountCents = normalizedAmountCents;
  const reversals = [];

  for (const transfer of transfers.sort((left, right) => {
    const leftCreated = Number(left?.created || 0);
    const rightCreated = Number(right?.created || 0);
    return rightCreated - leftCreated;
  })) {
    if (remainingAmountCents <= 0) {
      break;
    }

    const reversibleAmountCents = getTransferNetAmountCents(transfer);
    if (reversibleAmountCents <= 0) {
      continue;
    }

    const amountToReverse = Math.min(reversibleAmountCents, remainingAmountCents);
    const reversal = await stripe.transfers.createReversal(
      transfer.id,
      {
        amount: amountToReverse,
        metadata,
      },
      {
        idempotencyKey: stableKey(['transfer_reversal', transfer.id, amountToReverse, metadata?.source || 'booking']),
      }
    );
    reversals.push(reversal);
    remainingAmountCents -= amountToReverse;
  }

  return {
    reversedAmountCents: normalizedAmountCents - remainingAmountCents,
    reversals,
  };
}

async function ensureNetBookingTransferAmount({
  bookingId,
  transferGroup,
  destinationAccountId,
  currency,
  purpose,
  targetAmountCents,
  metadata = {},
  includeUnscopedTransfers = false,
}) {
  const normalizedTargetAmountCents = Math.max(0, Math.round(Number(targetAmountCents || 0)));
  const transfers = filterBookingTransfersByPurpose(
    await listStripeTransfersByGroup(transferGroup),
    purpose,
    destinationAccountId,
    { includeUnscopedTransfers }
  );
  const currentNetAmountCents = transfers.reduce((sum, transfer) => sum + getTransferNetAmountCents(transfer), 0);

  if (currentNetAmountCents > normalizedTargetAmountCents) {
    await reverseBookingTransfersIfNeeded({
      transferGroup,
      amountCents: currentNetAmountCents - normalizedTargetAmountCents,
      purpose,
      destinationAccountId,
      metadata: {
        booking_id: String(bookingId),
        booking_purpose: purpose,
        ...metadata,
      },
    });
  }

  if (currentNetAmountCents >= normalizedTargetAmountCents) {
    return { transferredAmountCents: normalizedTargetAmountCents, createdTransfer: null };
  }

  const missingAmountCents = normalizedTargetAmountCents - currentNetAmountCents;
  const createdTransfer = await stripe.transfers.create(
    {
      amount: missingAmountCents,
      currency: toStripeCurrencyCode(currency),
      destination: destinationAccountId,
      transfer_group: transferGroup,
      metadata: {
        booking_id: String(bookingId),
        booking_purpose: purpose,
        ...metadata,
      },
    },
    {
      idempotencyKey: stableKey(['transfer', transferGroup, purpose, normalizedTargetAmountCents]),
    }
  );

  return {
    transferredAmountCents: normalizedTargetAmountCents,
    createdTransfer,
  };
}

async function retrievePaymentIntentWithCharge(paymentIntentId) {
  if (!paymentIntentId) {
    return null;
  }

  return stripe.paymentIntents.retrieve(paymentIntentId, {
    expand: ['latest_charge'],
  });
}

async function ensureRefundForPaymentIntent({
  paymentIntentId,
  amountCents = null,
  transferGroup = null,
  reverseTransferPurpose = null,
  reverseTransferDestinationAccountId = null,
  metadata = {},
}) {
  if (!paymentIntentId) {
    return null;
  }

  const intent = await retrievePaymentIntentWithCharge(paymentIntentId);
  const latestCharge = intent?.latest_charge;
  if (!latestCharge) {
    return null;
  }

  const capturedAmountCents = Number(latestCharge.amount_captured || latestCharge.amount || intent?.amount || 0);
  const alreadyRefundedAmountCents = Number(latestCharge.amount_refunded || 0);
  const refundableAmountCents = Math.max(0, capturedAmountCents - alreadyRefundedAmountCents);
  const normalizedRequestedAmountCents = amountCents === null || amountCents === undefined
    ? refundableAmountCents
    : Math.max(0, Math.round(Number(amountCents || 0)));
  const effectiveRefundAmountCents = Math.min(refundableAmountCents, normalizedRequestedAmountCents);

  if (effectiveRefundAmountCents <= 0) {
    return null;
  }

  if (transferGroup) {
    await reverseBookingTransfersIfNeeded({
      transferGroup,
      amountCents: effectiveRefundAmountCents,
      purpose: reverseTransferPurpose,
      destinationAccountId: reverseTransferDestinationAccountId,
      metadata,
    });
  }

  return stripe.refunds.create(
    {
      payment_intent: paymentIntentId,
      amount: effectiveRefundAmountCents,
      metadata,
    },
    {
      idempotencyKey: stableKey(['refund', paymentIntentId, effectiveRefundAmountCents, metadata?.source || 'booking']),
    }
  );
}

async function triggerStripeRefundForPaymentIntent(paymentIntentId, metadata = {}) {
  return ensureRefundForPaymentIntent({
    paymentIntentId,
    metadata,
  });
}

async function upsertBookingIssueReport(connection, {
  bookingId,
  reportedByUserId = null,
  reportedAgainstUserId = null,
  issueType,
  status = 'open',
  details,
  resolvedAt = null,
}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  const normalizedIssueType = String(issueType || '').trim().toLowerCase();
  if (!normalizedBookingId || !normalizedIssueType) {
    return null;
  }

  const [rows] = await connection.query(
    `
    SELECT id, status
    FROM booking_issue_report
    WHERE booking_id = ?
      AND issue_type = ?
    ORDER BY id DESC
    LIMIT 1
    FOR UPDATE
    `,
    [normalizedBookingId, normalizedIssueType]
  );
  const existingIssue = rows[0] || null;

  if (existingIssue) {
    await connection.query(
      `
      UPDATE booking_issue_report
      SET reported_by_user_id = COALESCE(?, reported_by_user_id),
          reported_against_user_id = COALESCE(?, reported_against_user_id),
          status = ?,
          details = ?,
          resolved_at = ?
      WHERE id = ?
      `,
      [
        reportedByUserId || null,
        reportedAgainstUserId || null,
        status,
        details,
        toDbDateTime(resolvedAt),
        existingIssue.id,
      ]
    );
    return existingIssue.id;
  }

  const [insertResult] = await connection.query(
    `
    INSERT INTO booking_issue_report
      (booking_id, reported_by_user_id, reported_against_user_id, issue_type, status, details, resolved_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
    [
      normalizedBookingId,
      reportedByUserId || null,
      reportedAgainstUserId || null,
      normalizedIssueType,
      status,
      details,
      toDbDateTime(resolvedAt),
    ]
  );
  return insertResult.insertId;
}

async function incrementUserStrikeCount(connection, userId, amount = 1) {
  const normalizedUserId = normalizeNullableInteger(userId);
  const normalizedAmount = Math.max(0, Math.round(Number(amount || 0)));
  if (!normalizedUserId || normalizedAmount <= 0) {
    return;
  }

  await connection.query(
    `
    UPDATE user_account
    SET strikes_num = COALESCE(strikes_num, 0) + ?
    WHERE id = ?
    `,
    [normalizedAmount, normalizedUserId]
  );
}

async function transitionBookingWithOptionalDepositRefund(connection, currentBooking, {
  nextServiceStatus,
  nextSettlementStatus,
  changedByUserId = null,
  reasonCode = null,
  note = null,
  extraPatch = {},
  requestDepositRefund = false,
}) {
  const depositPayment = requestDepositRefund
    ? await getPaymentRow(connection, currentBooking.id, 'deposit')
    : null;
  const shouldRequestDepositRefund = requestDepositRefund && canInitiateDepositRefund(depositPayment);
  const transitionResult = await transitionBookingStateRecord(connection, currentBooking, {
    nextServiceStatus,
    nextSettlementStatus: shouldRequestDepositRefund ? 'refund_pending' : nextSettlementStatus,
    changedByUserId,
    reasonCode,
    note,
    extraPatch,
  });

  if (shouldRequestDepositRefund) {
    await connection.query(
      'UPDATE payments SET status = ? WHERE id = ?',
      ['refund_pending', depositPayment.id]
    );
  }

  return {
    ...transitionResult,
    refundRequest: shouldRequestDepositRefund
      ? {
          bookingId: currentBooking.id,
          paymentIntentId: depositPayment.payment_intent_id,
          reasonCode: reasonCode || 'deposit_refund_requested',
        }
      : null,
  };
}

async function expireRequestedBookingByIdIfNeeded(bookingId) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return { expired: false, refundRequested: false };
  }

  const connection = await pool.promise().getConnection();
  let refundRequest = null;

  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        id,
        service_status,
        settlement_status,
        order_datetime AS created_at,
        order_datetime,
        requested_start_datetime,
        requested_duration_minutes,
        expires_at
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [normalizedBookingId]
    );

    if (!booking || !canAutoExpireRequestedBooking(booking)) {
      await connection.rollback();
      return { expired: false, refundRequested: false };
    }

    const transitionResult = await transitionBookingWithOptionalDepositRefund(connection, booking, {
      nextServiceStatus: 'expired',
      nextSettlementStatus: booking.settlement_status,
      reasonCode: 'request_expired',
      requestDepositRefund: true,
    });
    refundRequest = transitionResult.refundRequest;

    await connection.commit();
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error expiring requested booking:', error);
    return { expired: false, refundRequested: false, error };
  } finally {
    connection.release();
  }

  if (refundRequest?.paymentIntentId) {
    try {
      await triggerStripeRefundForPaymentIntent(refundRequest.paymentIntentId, {
        booking_id: String(refundRequest.bookingId),
        source: 'booking_expired',
      });
    } catch (refundError) {
      console.error('Error refunding expired booking deposit:', {
        bookingId: refundRequest.bookingId,
        paymentIntentId: refundRequest.paymentIntentId,
        error: refundError.message,
      });
    }
  }

  return {
    expired: true,
    refundRequested: Boolean(refundRequest?.paymentIntentId),
  };
}

function normalizeNullableText(value) {
  return typeof value === 'string' && value.trim()
    ? value.trim()
    : null;
}

function buildBookingEditableComparableFields(source = {}) {
  return {
    requested_start_datetime: toDbDateTime(
      source.requested_start_datetime
      ?? source.booking_start_datetime
      ?? null
    ),
    requested_duration_minutes: normalizeDurationMinutes(
      source.requested_duration_minutes
      ?? source.service_duration
      ?? null
    ),
    address_id: normalizeNullableInteger(source.address_id),
    description: normalizeNullableText(source.description),
  };
}

function bookingEditableFieldsAreEqual(left = {}, right = {}) {
  return left.requested_start_datetime === right.requested_start_datetime
    && left.requested_duration_minutes === right.requested_duration_minutes
    && left.address_id === right.address_id
    && left.description === right.description;
}

function formatLeadTimeLabel(minutes) {
  const normalizedMinutes = normalizeMinimumNoticeMinutes(minutes);
  if (normalizedMinutes === null || normalizedMinutes <= 0) {
    return null;
  }

  if (normalizedMinutes === 2880) {
    return '48 horas';
  }

  if (normalizedMinutes % (7 * 24 * 60) === 0) {
    const weeks = normalizedMinutes / (7 * 24 * 60);
    return weeks === 1 ? '1 semana' : `${weeks} semanas`;
  }

  if (normalizedMinutes % (24 * 60) === 0) {
    const days = normalizedMinutes / (24 * 60);
    return days === 1 ? '24 horas' : `${days} días`;
  }

  if (normalizedMinutes % 60 === 0) {
    const hours = normalizedMinutes / 60;
    return hours === 1 ? '1 hora' : `${hours} horas`;
  }

  return normalizedMinutes === 1 ? '1 minuto' : `${normalizedMinutes} minutos`;
}

async function prepareBookingEditableUpdate(connection, currentBooking, requestBody = {}) {
  const hasStartField = Object.prototype.hasOwnProperty.call(requestBody, 'requested_start_datetime')
    || Object.prototype.hasOwnProperty.call(requestBody, 'booking_start_datetime');
  const hasDurationField = Object.prototype.hasOwnProperty.call(requestBody, 'requested_duration_minutes')
    || Object.prototype.hasOwnProperty.call(requestBody, 'service_duration');
  const hasDescriptionField = Object.prototype.hasOwnProperty.call(requestBody, 'description');
  const hasAddressField = Object.prototype.hasOwnProperty.call(requestBody, 'address_id');

  const [[serviceSnapshot]] = await connection.query(
    `
    SELECT
      id,
      latitude,
      longitude,
      action_rate
    FROM service
    WHERE id = ?
    LIMIT 1
    `,
    [currentBooking.service_id]
  );
  if (!serviceSnapshot) {
    const error = new Error('Servicio no encontrado.');
    error.statusCode = 404;
    throw error;
  }

  let nextAddressId = currentBooking.address_id;
  let currentAddressSnapshot = null;
  let nextAddressSnapshot = null;
  if (currentBooking.address_id !== null && currentBooking.address_id !== undefined) {
    const [[existingCurrentAddress]] = await connection.query(
      `
      SELECT
        id,
        address_type,
        street_number,
        address_1,
        address_2,
        postal_code,
        city,
        state,
        country,
        latitude,
        longitude
      FROM address
      WHERE id = ?
      LIMIT 1
      ${hasAddressField ? 'FOR UPDATE' : ''}
      `,
      [currentBooking.address_id]
    );
    currentAddressSnapshot = existingCurrentAddress || null;
    nextAddressSnapshot = currentAddressSnapshot
      ? {
        id: existingCurrentAddress.id,
        address_type: existingCurrentAddress.address_type,
        street_number: existingCurrentAddress.street_number,
        address_1: existingCurrentAddress.address_1,
        address_2: existingCurrentAddress.address_2,
        postal_code: existingCurrentAddress.postal_code,
        city: existingCurrentAddress.city,
        state: existingCurrentAddress.state,
        country: existingCurrentAddress.country,
        latitude: existingCurrentAddress.latitude,
        longitude: existingCurrentAddress.longitude,
      }
      : null;
  }

  if (hasAddressField) {
    nextAddressId = normalizeNullableInteger(requestBody.address_id);
    if (nextAddressId !== null) {
      const [[existingAddress]] = await connection.query(
        `
        SELECT
          id,
          address_type,
          street_number,
          address_1,
          address_2,
          postal_code,
          city,
          state,
          country,
          latitude,
          longitude
        FROM address
        WHERE id = ?
        LIMIT 1
        FOR UPDATE
        `,
        [nextAddressId]
      );
      if (!existingAddress) {
        const error = new Error('address_id no existe.');
        error.statusCode = 400;
        throw error;
      }
      nextAddressSnapshot = {
        id: existingAddress.id,
        address_type: existingAddress.address_type,
        street_number: existingAddress.street_number,
        address_1: existingAddress.address_1,
        address_2: existingAddress.address_2,
        postal_code: existingAddress.postal_code,
        city: existingAddress.city,
        state: existingAddress.state,
        country: existingAddress.country,
        latitude: existingAddress.latitude,
        longitude: existingAddress.longitude,
      };
    } else {
      nextAddressSnapshot = null;
    }
  }

  const bookingAddressRule = getBookingAddressRuleForService(serviceSnapshot);
  if (bookingAddressRule.mode === 'hidden') {
    if (hasAddressField) {
      nextAddressId = null;
      nextAddressSnapshot = null;
    }
  } else {
    const addressRowForValidation = hasAddressField ? nextAddressSnapshot : currentAddressSnapshot;
    assertBookingAddressRulesForService({
      serviceRow: serviceSnapshot,
      addressRow: addressRowForValidation,
      requestBody,
      allowUnknownAddressLocation: !hasAddressField,
    });
  }

  const requestedStartInput = hasStartField
    ? (requestBody.requested_start_datetime ?? requestBody.booking_start_datetime ?? null)
    : currentBooking.requested_start_datetime;
  const requestedStartDateTime = parseDateTimeInput(requestedStartInput);
  const requestedDurationMinutes = hasDurationField
    ? normalizeDurationMinutes(requestBody.requested_duration_minutes ?? requestBody.service_duration ?? null)
    : normalizeDurationMinutes(currentBooking.requested_duration_minutes);

  if (!isDurationMinutesInRange(requestedDurationMinutes)) {
    const error = new Error(`La duración debe estar entre ${MIN_BOOKING_DURATION_MINUTES} y ${MAX_BOOKING_DURATION_MINUTES} minutos.`);
    error.statusCode = 400;
    throw error;
  }

  if (requestedStartDateTime && requestedStartDateTime.getTime() < Date.now()) {
    const error = new Error('No se puede reprogramar la reserva en el pasado.');
    error.statusCode = 400;
    throw error;
  }

  const schedule = buildBookingSchedule({
    createdAt: currentBooking.order_datetime ?? currentBooking.created_at,
    requestedStartDateTime,
    requestedDurationMinutes,
  });
  const bookingCurrency = normalizeCurrencyCode(currentBooking.service_currency_snapshot, 'EUR');
  const unitPriceAmount = currentBooking.unit_price_amount_cents_snapshot === null || currentBooking.unit_price_amount_cents_snapshot === undefined
    ? 0
    : fromMinorUnits(currentBooking.unit_price_amount_cents_snapshot, bookingCurrency);
  const pricingSnapshot = computeBookingPricingSnapshot({
    priceType: currentBooking.price_type_snapshot,
    unitPrice: unitPriceAmount,
    durationMinutes: requestedDurationMinutes,
    currency: bookingCurrency,
  });
  const nextDescription = hasDescriptionField
    ? normalizeNullableText(requestBody.description)
    : normalizeNullableText(currentBooking.description);
  const estimatedBaseAmountCents = pricingSnapshot.final === null && pricingSnapshot.type !== 'fix'
    ? null
    : toMinorUnits(pricingSnapshot.base, bookingCurrency);
  const estimatedCommissionAmountCents = toMinorUnits(pricingSnapshot.commission || 0, bookingCurrency);
  const estimatedTotalAmountCents = pricingSnapshot.final === null
    ? null
    : toMinorUnits(pricingSnapshot.final, bookingCurrency);

  const comparableFields = {
    requested_start_datetime: toDbDateTime(schedule.requestedStartDateTime),
    requested_duration_minutes: schedule.requestedDurationMinutes,
    address_id: nextAddressId,
    description: nextDescription,
  };

  return {
    comparableFields,
    updatePatch: {
      requested_start_datetime: comparableFields.requested_start_datetime,
      requested_duration_minutes: comparableFields.requested_duration_minutes,
      requested_end_datetime: toDbDateTime(schedule.requestedEndDateTime),
      accept_deadline_at: toDbDateTime(schedule.acceptDeadlineAt),
      expires_at: toDbDateTime(schedule.expiresAt),
      last_minute_window_starts_at: toDbDateTime(schedule.lastMinuteWindowStartsAt),
      address_id: nextAddressId,
      description: nextDescription,
      estimated_base_amount_cents: estimatedBaseAmountCents,
      estimated_commission_amount_cents: estimatedCommissionAmountCents,
      estimated_total_amount_cents: estimatedTotalAmountCents,
      deposit_amount_cents_snapshot: estimatedCommissionAmountCents,
    },
    changeRequestPayload: {
      ...comparableFields,
      address_snapshot: nextAddressSnapshot,
    },
  };
}

async function applyPreparedBookingEditableUpdate(connection, bookingId, preparedUpdate) {
  const patch = preparedUpdate?.updatePatch;
  if (!patch) {
    return;
  }

  await connection.query(
    `
    UPDATE booking
    SET requested_start_datetime = ?,
        requested_duration_minutes = ?,
        requested_end_datetime = ?,
        accept_deadline_at = ?,
        expires_at = ?,
        last_minute_window_starts_at = ?,
        address_id = ?,
        description = ?,
        estimated_base_amount_cents = ?,
        estimated_commission_amount_cents = ?,
        estimated_total_amount_cents = ?,
        deposit_amount_cents_snapshot = ?
    WHERE id = ?
    `,
    [
      patch.requested_start_datetime,
      patch.requested_duration_minutes,
      patch.requested_end_datetime,
      patch.accept_deadline_at,
      patch.expires_at,
      patch.last_minute_window_starts_at,
      patch.address_id,
      patch.description,
      patch.estimated_base_amount_cents,
      patch.estimated_commission_amount_cents,
      patch.estimated_total_amount_cents,
      patch.deposit_amount_cents_snapshot,
      bookingId,
    ]
  );
}

async function getLatestBookingChangeRequest(connection, bookingId, {
  forUpdate = false,
  pendingOnly = false,
} = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return null;
  }

  const whereClauses = ['booking_id = ?'];
  const params = [normalizedBookingId];
  if (pendingOnly) {
    whereClauses.push(`status = 'pending'`);
  }

  const [rows] = await connection.query(
    `
    SELECT
      id,
      booking_id,
      requested_by_user_id,
      target_user_id,
      status,
      changes_json,
      message,
      created_at,
      resolved_at
    FROM booking_change_request
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY
      CASE WHEN status = 'pending' THEN 0 ELSE 1 END,
      created_at DESC,
      id DESC
    LIMIT 1
    ${forUpdate ? 'FOR UPDATE' : ''}
    `,
    params
  );
  return rows[0] || null;
}

async function expirePendingBookingChangeRequestsForBooking(connection, booking, {
  force = false,
  now = new Date(),
} = {}) {
  const normalizedBookingId = normalizeNullableInteger(booking?.id ?? booking?.booking_id);
  if (!normalizedBookingId) {
    return [];
  }

  const [rows] = await connection.query(
    `
    SELECT id, booking_id, status, created_at
    FROM booking_change_request
    WHERE booking_id = ?
      AND status = 'pending'
    ORDER BY created_at ASC, id ASC
    FOR UPDATE
    `,
    [normalizedBookingId]
  );

  const expiredIds = [];
  for (const row of rows || []) {
    const shouldExpire = force
      || !canEditBooking(booking)
      || hasBookingChangeRequestExpired(row, {
        now,
        ttlMs: BOOKING_CHANGE_REQUEST_TTL_MS,
      });

    if (!shouldExpire) {
      continue;
    }

    await connection.query(
      `
      UPDATE booking_change_request
      SET status = 'expired',
          resolved_at = ?
      WHERE id = ?
      `,
      [toDbDateTime(now), row.id]
    );
    expiredIds.push(row.id);
  }

  return expiredIds;
}

async function getBookingLifecycleNotificationContext(connection, bookingId) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return null;
  }

  const [[row]] = await connection.query(
    `
    SELECT
      b.id,
      COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
      b.requested_start_datetime,
      client.email AS client_email,
      client.first_name AS client_first_name,
      client.surname AS client_surname,
      client.username AS client_username,
      prof.email AS professional_email,
      prof.first_name AS professional_first_name,
      prof.surname AS professional_surname,
      prof.username AS professional_username
    FROM booking b
    LEFT JOIN service s ON b.service_id = s.id
    LEFT JOIN user_account client ON b.client_user_id = client.id
    LEFT JOIN user_account prof ON b.provider_user_id_snapshot = prof.id
    WHERE b.id = ?
    LIMIT 1
    `,
    [normalizedBookingId]
  );

  if (!row) {
    return null;
  }

  return {
    id: row.id,
    serviceTitle: row.service_title,
    start: row.requested_start_datetime,
    client: {
      email: row.client_email,
      firstName: row.client_first_name,
      surname: row.client_surname,
      username: row.client_username,
    },
    professional: {
      email: row.professional_email,
      firstName: row.professional_first_name,
      surname: row.professional_surname,
      username: row.professional_username,
    },
  };
}

async function processAcceptedBookingInactivity(bookingId) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return { handled: false, reason: 'invalid_booking_id' };
  }

  const connection = await pool.promise().getConnection();
  let notificationBooking = null;
  let autoCancelRefundRequest = null;
  let stage = null;

  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        id,
        service_id,
        client_user_id,
        provider_user_id_snapshot,
        service_status,
        settlement_status,
        requested_start_datetime,
        updated_at
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [normalizedBookingId]
    );

    if (!booking) {
      await connection.rollback();
      return { handled: false, reason: 'booking_not_found' };
    }

    const [historyRows] = await connection.query(
      `
      SELECT reason_code
      FROM booking_status_history
      WHERE booking_id = ?
        AND reason_code IN (${ACCEPTED_BOOKING_INACTIVITY_REASON_CODES.map(() => '?').join(', ')})
      `,
      [normalizedBookingId, ...ACCEPTED_BOOKING_INACTIVITY_REASON_CODES]
    );
    const sentReasonCodes = new Set(
      (historyRows || [])
        .map((row) => String(row.reason_code || '').trim().toLowerCase())
        .filter(Boolean)
    );

    stage = getAcceptedBookingInactivityStage(booking, {
      now: new Date(),
      isReminderSent: (reasonCode) => sentReasonCodes.has(String(reasonCode || '').trim().toLowerCase()),
    });

    if (!stage) {
      await connection.rollback();
      return { handled: false, reason: 'stage_not_due' };
    }

    notificationBooking = await getBookingLifecycleNotificationContext(connection, normalizedBookingId);

    if (stage.type === 'reminder') {
      await insertBookingStatusHistory(connection, {
        bookingId: normalizedBookingId,
        fromServiceStatus: booking.service_status,
        toServiceStatus: booking.service_status,
        fromSettlementStatus: booking.settlement_status,
        toSettlementStatus: booking.settlement_status,
        reasonCode: stage.reasonCode,
        note: `Aviso automático ${stage.key || ''} por reserva accepted sin actividad tras el inicio previsto.`.trim(),
      });

      await connection.commit();
      return { handled: true, type: 'reminder', stage, booking: notificationBooking };
    }

    autoCancelRefundRequest = (
      await transitionBookingWithOptionalDepositRefund(connection, booking, {
        nextServiceStatus: 'canceled',
        nextSettlementStatus: 'none',
        changedByUserId: null,
        reasonCode: stage.reasonCode,
        note: 'Cancelación automática tras 7 días en accepted sin actividad.',
        extraPatch: {
          canceled_by_user_id: null,
          cancellation_reason_code: stage.reasonCode,
          client_approval_deadline_at: null,
        },
        requestDepositRefund: true,
      })
    ).refundRequest;

    await incrementUserStrikeCount(connection, booking.provider_user_id_snapshot, 1);
    await connection.commit();

    return {
      handled: true,
      type: 'auto_cancel',
      stage,
      booking: notificationBooking,
      refundRequest: autoCancelRefundRequest,
    };
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error processing accepted booking inactivity:', {
      bookingId: normalizedBookingId,
      error: error.message,
    });
    return { handled: false, reason: 'processing_failed', error };
  } finally {
    connection.release();
  }
}

function getMinimumChargeAmountCentsForCurrency(currency = 'EUR') {
  const minimumChargeAmount = convertAmount(1, 'EUR', normalizeCurrencyCode(currency, 'EUR'));
  return Math.max(0, toMinorUnits(minimumChargeAmount, currency));
}

function buildClosureFinancialSnapshot({
  booking,
  proposedBaseAmountCents,
  proposedFinalDurationMinutes = null,
  zeroChargeMode = false,
}) {
  const bookingCurrency = normalizeCurrencyCode(booking?.service_currency_snapshot, 'EUR');
  const priceTypeSnapshot = String(booking?.price_type_snapshot || '').trim().toLowerCase();
  const estimatedTotalAmountCents = Number(booking?.estimated_total_amount_cents || 0);
  const estimatedDurationMinutes = normalizeNullableInteger(booking?.requested_duration_minutes);
  const normalizedProposedBaseAmountCents = Math.max(0, Number(proposedBaseAmountCents || 0));
  const normalizedProposedFinalDurationMinutes = normalizeNullableInteger(proposedFinalDurationMinutes);
  const proposalPricing = computeBookingPricingSnapshot({
    priceType: priceTypeSnapshot === 'budget' ? 'budget' : 'fix',
    unitPrice: fromMinorUnits(normalizedProposedBaseAmountCents, bookingCurrency),
    durationMinutes: normalizedProposedFinalDurationMinutes || 0,
    currency: bookingCurrency,
  });
  const proposedCommissionAmountCents = toMinorUnits(proposalPricing.commission || 0, bookingCurrency);
  const proposedTotalAmountCents = proposalPricing.final === null
    ? 0
    : toMinorUnits(proposalPricing.final, bookingCurrency);
  const depositAlreadyPaidAmountCents = Number(booking?.deposit_amount_cents_snapshot || 0);
  const settlementAmounts = computeSettlementAmounts({
    depositAlreadyPaidAmountCents,
    proposedTotalAmountCents,
    providerPayoutAmountCents: normalizedProposedBaseAmountCents,
    minimumChargeAmountCents: getMinimumChargeAmountCentsForCurrency(bookingCurrency),
  });
  const autoChargeDecision = evaluateAutoChargeEligibility({
    priceType: priceTypeSnapshot,
    estimatedTotalAmountCents,
    proposedTotalAmountCents,
    estimatedDurationMinutes,
    proposedFinalDurationMinutes: normalizedProposedFinalDurationMinutes,
    zeroChargeMode,
    toleranceFactor: AUTO_CHARGE_TOLERANCE_FACTOR,
  });

  return {
    priceTypeSnapshot,
    estimatedDurationMinutes,
    estimatedTotalAmountCents,
    proposedBaseAmountCents: normalizedProposedBaseAmountCents,
    proposedCommissionAmountCents,
    proposedTotalAmountCents,
    proposedFinalDurationMinutes: normalizedProposedFinalDurationMinutes,
    depositAlreadyPaidAmountCents,
    amountDueFromClientCents: settlementAmounts.amountDueFromClientCents,
    amountToRefundCents: settlementAmounts.amountToRefundCents,
    providerPayoutAmountCents: settlementAmounts.providerPayoutAmountCents,
    platformAmountCents: settlementAmounts.platformAmountCents,
    zeroChargeMode: zeroChargeMode ? 1 : 0,
    autoChargeEligible: autoChargeDecision.eligible ? 1 : 0,
    autoChargeScheduledAt: buildClientApprovalDeadline(),
    autoChargeDecision,
  };
}

async function upsertActiveClosureProposal(connection, {
  booking,
  createdByUserId,
  proposedBaseAmountCents,
  proposedFinalDurationMinutes = null,
  zeroChargeMode = false,
}) {
  const financialSnapshot = buildClosureFinancialSnapshot({
    booking,
    proposedBaseAmountCents,
    proposedFinalDurationMinutes,
    zeroChargeMode,
  });

  const [[existingProposal]] = await connection.query(
    `SELECT id
     FROM booking_closure_proposal
     WHERE booking_id = ? AND status = 'active'
     ORDER BY id DESC
     LIMIT 1
     FOR UPDATE`,
    [booking.id]
  );

  const queryValues = [
    financialSnapshot.priceTypeSnapshot,
    financialSnapshot.estimatedDurationMinutes,
    financialSnapshot.proposedFinalDurationMinutes,
    financialSnapshot.estimatedTotalAmountCents,
    financialSnapshot.proposedBaseAmountCents,
    financialSnapshot.proposedCommissionAmountCents,
    financialSnapshot.proposedTotalAmountCents,
    financialSnapshot.depositAlreadyPaidAmountCents,
    financialSnapshot.amountDueFromClientCents,
    financialSnapshot.amountToRefundCents,
    financialSnapshot.providerPayoutAmountCents,
    financialSnapshot.platformAmountCents,
    financialSnapshot.zeroChargeMode,
    financialSnapshot.autoChargeEligible,
    toDbDateTime(financialSnapshot.autoChargeScheduledAt),
  ];

  if (existingProposal?.id) {
    await connection.query(
      `UPDATE booking_closure_proposal
       SET price_type_snapshot = ?,
           estimated_duration_minutes = ?,
           proposed_final_duration_minutes = ?,
           estimated_total_amount_cents = ?,
           proposed_base_amount_cents = ?,
           proposed_commission_amount_cents = ?,
           proposed_total_amount_cents = ?,
           deposit_already_paid_amount_cents = ?,
           amount_due_from_client_cents = ?,
           amount_to_refund_cents = ?,
           provider_payout_amount_cents = ?,
           platform_amount_cents = ?,
           zero_charge_mode = ?,
           auto_charge_eligible = ?,
           auto_charge_scheduled_at = ?,
           sent_at = UTC_TIMESTAMP(),
           revoked_at = NULL,
           accepted_at = NULL,
           rejected_at = NULL
       WHERE id = ?`,
      [...queryValues, existingProposal.id]
    );
    return existingProposal.id;
  }

  const [insertResult] = await connection.query(
    `INSERT INTO booking_closure_proposal
      (booking_id, created_by_user_id, status, price_type_snapshot, estimated_duration_minutes, proposed_final_duration_minutes, estimated_total_amount_cents, proposed_base_amount_cents, proposed_commission_amount_cents, proposed_total_amount_cents, deposit_already_paid_amount_cents, amount_due_from_client_cents, amount_to_refund_cents, provider_payout_amount_cents, platform_amount_cents, zero_charge_mode, auto_charge_eligible, auto_charge_scheduled_at)
     VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      booking.id,
      createdByUserId,
      ...queryValues,
    ]
  );

  return insertResult.insertId;
}

const CLIENT_APPROVAL_DEADLINE_MS = 48 * 60 * 60 * 1000;
const SECONDARY_CLIENT_APPROVAL_DEADLINE_MS = 72 * 60 * 60 * 1000;

function buildClientApprovalDeadline(now = new Date()) {
  const normalizedNow = parseDateTimeInput(now) || new Date();
  return new Date(normalizedNow.getTime() + CLIENT_APPROVAL_DEADLINE_MS);
}

function buildSecondaryClientApprovalDeadline(now = new Date()) {
  const normalizedNow = parseDateTimeInput(now) || new Date();
  return new Date(normalizedNow.getTime() + SECONDARY_CLIENT_APPROVAL_DEADLINE_MS);
}

function isSecondaryClientApprovalWindowActive(booking, now = new Date()) {
  const currentDeadline = parseDateTimeInput(booking?.client_approval_deadline_at);
  const initialDeadline = parseDateTimeInput(booking?.auto_charge_scheduled_at);
  const normalizedNow = parseDateTimeInput(now) || new Date();

  if (!currentDeadline || !initialDeadline) {
    return false;
  }

  return currentDeadline.getTime() > initialDeadline.getTime()
    && normalizedNow.getTime() >= initialDeadline.getTime();
}

async function getLatestClosureProposal(connection, bookingId, { forUpdate = false } = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return null;
  }

  const [rows] = await connection.query(
    `
    SELECT
      id,
      booking_id,
      status,
      price_type_snapshot,
      proposed_base_amount_cents,
      proposed_commission_amount_cents,
      proposed_total_amount_cents,
      proposed_final_duration_minutes,
      deposit_already_paid_amount_cents,
      amount_due_from_client_cents,
      amount_to_refund_cents,
      provider_payout_amount_cents,
      platform_amount_cents,
      zero_charge_mode,
      auto_charge_eligible,
      auto_charge_scheduled_at,
      sent_at,
      accepted_at,
      rejected_at,
      revoked_at
    FROM booking_closure_proposal
    WHERE booking_id = ?
    ORDER BY id DESC
    LIMIT 1
    ${forUpdate ? 'FOR UPDATE' : ''}
    `,
    [normalizedBookingId]
  );

  return rows[0] || null;
}

async function updateClosureProposalStatus(connection, proposalId, nextStatus, now = new Date()) {
  const normalizedProposalId = normalizeNullableInteger(proposalId);
  if (!normalizedProposalId) {
    return;
  }

  const normalizedNow = parseDateTimeInput(now) || new Date();
  const acceptedAt = nextStatus === 'accepted' ? toDbDateTime(normalizedNow) : null;
  const rejectedAt = nextStatus === 'rejected' ? toDbDateTime(normalizedNow) : null;
  const revokedAt = nextStatus === 'revoked' ? toDbDateTime(normalizedNow) : null;

  await connection.query(
    `
    UPDATE booking_closure_proposal
    SET status = ?,
        accepted_at = ?,
        rejected_at = ?,
        revoked_at = ?
    WHERE id = ?
    `,
    [nextStatus, acceptedAt, rejectedAt, revokedAt, normalizedProposalId]
  );
}

// Garantiza un Customer y lo persiste en user_account.stripe_customer_id
async function ensureStripeCustomerId(conn, { userId, email }) {
  const [[row]] = await conn.query(
    `SELECT stripe_customer_id FROM user_account WHERE id = ? FOR UPDATE`,
    [userId]
  );
  let customerId = row?.stripe_customer_id || null;

  if (!customerId) {
    const customer = await stripe.customers.create({ email });
    customerId = customer.id;
    await conn.query(
      `UPDATE user_account SET stripe_customer_id = ? WHERE id = ?`,
      [customerId, userId]
    );
  }
  return customerId;
}

async function getDefaultSavedCustomerPaymentMethod(conn, userId, { forUpdate = false } = {}) {
  const normalizedUserId = normalizeNullableInteger(userId);
  if (!normalizedUserId) {
    return null;
  }

  const [rows] = await conn.query(
    `
    SELECT
      id,
      user_id,
      payment_type AS customer_payment_method_stripe_id,
      provider,
      brand,
      card_number,
      expiry_date,
      is_safed,
      is_default
    FROM payment_method
    WHERE user_id = ?
      AND is_safed = 1
      AND payment_type IS NOT NULL
      AND payment_type LIKE 'pm_%'
    ORDER BY is_default DESC, id DESC
    LIMIT 1
    ${forUpdate ? 'FOR UPDATE' : ''}
    `,
    [normalizedUserId]
  );

  return rows[0] || null;
}

async function getStoredCustomerPaymentMethodByStripeId(conn, userId, stripePaymentMethodId, { forUpdate = false } = {}) {
  const normalizedUserId = normalizeNullableInteger(userId);
  const normalizedStripePaymentMethodId = typeof stripePaymentMethodId === 'string'
    ? stripePaymentMethodId.trim()
    : '';

  if (!normalizedUserId || !normalizedStripePaymentMethodId) {
    return null;
  }

  const [rows] = await conn.query(
    `
    SELECT
      id,
      user_id,
      payment_type AS customer_payment_method_stripe_id,
      provider,
      brand,
      card_number,
      expiry_date,
      is_safed,
      is_default
    FROM payment_method
    WHERE user_id = ?
      AND payment_type = ?
    ORDER BY id DESC
    LIMIT 1
    ${forUpdate ? 'FOR UPDATE' : ''}
    `,
    [normalizedUserId, normalizedStripePaymentMethodId]
  );

  return rows[0] || null;
}

async function upsertStoredCustomerPaymentMethod(conn, {
  userId,
  stripePaymentMethodId,
  last4,
  brand = null,
  expMonth,
  expYear,
  saveForFuture = false,
}) {
  const normalizedUserId = normalizeNullableInteger(userId);
  const normalizedStripePaymentMethodId = typeof stripePaymentMethodId === 'string'
    ? stripePaymentMethodId.trim()
    : '';
  if (!normalizedUserId || !normalizedStripePaymentMethodId) {
    return null;
  }

  const existingRow = await getStoredCustomerPaymentMethodByStripeId(
    conn,
    normalizedUserId,
    normalizedStripePaymentMethodId,
    { forUpdate: true }
  );

  const expiryLabel = formatStoredCardExpiry(expMonth, expYear) || (existingRow?.expiry_date || '');
  const maskedLast4 = typeof last4 === 'string' && last4.trim()
    ? last4.trim().slice(-4)
    : (existingRow?.card_number ? String(existingRow.card_number).slice(-4) : '0000');
  const normalizedBrand = typeof brand === 'string' && brand.trim().length > 0
    ? brand.trim()
    : (existingRow?.brand || null);
  const shouldRemainSaved = saveForFuture || existingRow?.is_safed === 1;
  const shouldBeDefault = saveForFuture || existingRow?.is_default === 1;

  if (shouldBeDefault) {
    await conn.query(
      'UPDATE payment_method SET is_default = 0 WHERE user_id = ?',
      [normalizedUserId]
    );
  }

  if (existingRow?.id) {
    await conn.query(
      `
      UPDATE payment_method
      SET provider = 'STRIPE',
          brand = ?,
          card_number = ?,
          expiry_date = ?,
          is_safed = ?,
          is_default = ?
      WHERE id = ?
      `,
      [
        normalizedBrand,
        maskedLast4,
        expiryLabel || '00/00',
        shouldRemainSaved ? 1 : 0,
        shouldBeDefault ? 1 : 0,
        existingRow.id,
      ]
    );

    return existingRow.id;
  }

  const [insertResult] = await conn.query(
    `
    INSERT INTO payment_method
      (user_id, payment_type, provider, brand, card_number, expiry_date, is_safed, is_default)
    VALUES (?, ?, 'STRIPE', ?, ?, ?, ?, ?)
    `,
    [
      normalizedUserId,
      normalizedStripePaymentMethodId,
      normalizedBrand,
      maskedLast4,
      expiryLabel || '00/00',
      shouldRemainSaved ? 1 : 0,
      shouldBeDefault ? 1 : 0,
    ]
  );

  return insertResult.insertId;
}

async function syncBookingSelectedPaymentMethod(conn, {
  bookingId,
  userId,
  stripePaymentMethodId,
  paymentMethodDetails = null,
  saveForFuture = false,
}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  const normalizedUserId = normalizeNullableInteger(userId);
  const normalizedStripePaymentMethodId = typeof stripePaymentMethodId === 'string'
    ? stripePaymentMethodId.trim()
    : '';

  if (!normalizedBookingId || !normalizedUserId || !normalizedStripePaymentMethodId) {
    return null;
  }

  let effectivePaymentMethodDetails = paymentMethodDetails;
  if (!effectivePaymentMethodDetails || typeof effectivePaymentMethodDetails !== 'object') {
    effectivePaymentMethodDetails = await stripe.paymentMethods.retrieve(normalizedStripePaymentMethodId);
  }

  const paymentMethodRecordId = await upsertStoredCustomerPaymentMethod(conn, {
    userId: normalizedUserId,
    stripePaymentMethodId: normalizedStripePaymentMethodId,
    last4: effectivePaymentMethodDetails?.card?.last4 || null,
    brand: effectivePaymentMethodDetails?.card?.brand || null,
    expMonth: effectivePaymentMethodDetails?.card?.exp_month || null,
    expYear: effectivePaymentMethodDetails?.card?.exp_year || null,
    saveForFuture,
  });

  if (!paymentMethodRecordId) {
    return null;
  }

  await conn.query(
    'UPDATE booking SET selected_customer_payment_method_id = ? WHERE id = ?',
    [paymentMethodRecordId, normalizedBookingId]
  );

  return paymentMethodRecordId;
}

async function syncBookingSelectedPaymentMethodFromIntent(conn, {
  bookingId,
  userId,
  intent,
  paymentMethodFallback = null,
  saveForFuture = false,
}) {
  const paymentPersistence = await resolvePaymentIntentPersistence(intent, {
    paymentMethodFallback,
  });
  const resolvedIntent = paymentPersistence.intent || intent;
  const stripePaymentMethodId = paymentPersistence.paymentMethodId || null;
  const paymentMethodDetails = (
    resolvedIntent?.payment_method && typeof resolvedIntent.payment_method === 'object'
      ? resolvedIntent.payment_method
      : paymentMethodFallback
  ) || null;

  if (!stripePaymentMethodId) {
    return null;
  }

  return syncBookingSelectedPaymentMethod(conn, {
    bookingId,
    userId,
    stripePaymentMethodId,
    paymentMethodDetails,
    saveForFuture,
  });
}

async function getBookingSelectedStripePaymentMethodId(conn, bookingId, { forUpdate = false } = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return null;
  }

  const [bookingRows] = await conn.query(
    `
    SELECT
      pm.payment_type AS customer_payment_method_stripe_id
    FROM booking b
    LEFT JOIN payment_method pm ON pm.id = b.selected_customer_payment_method_id
    WHERE b.id = ?
    LIMIT 1
    ${forUpdate ? 'FOR UPDATE' : ''}
    `,
    [normalizedBookingId]
  );

  const selectedStripePaymentMethodId = bookingRows[0]?.customer_payment_method_stripe_id;
  if (typeof selectedStripePaymentMethodId === 'string' && selectedStripePaymentMethodId.startsWith('pm_')) {
    return selectedStripePaymentMethodId;
  }

  const [paymentRows] = await conn.query(
    `
    SELECT payment_method_id
    FROM payments
    WHERE booking_id = ?
      AND payment_method_id IS NOT NULL
      AND payment_method_id LIKE 'pm_%'
    ORDER BY
      CASE type
        WHEN 'final' THEN 0
        WHEN 'deposit' THEN 1
        ELSE 2
      END,
      id DESC
    LIMIT 1
    ${forUpdate ? 'FOR UPDATE' : ''}
    `,
    [normalizedBookingId]
  );

  return paymentRows[0]?.payment_method_id || null;
}

function isBookingClosedForEphemeralPaymentCleanup(booking) {
  const normalizedServiceStatus = normalizeServiceStatus(booking?.service_status, 'pending_deposit');
  const normalizedSettlementStatus = normalizeSettlementStatus(booking?.settlement_status, 'none');

  if (['canceled', 'expired'].includes(normalizedServiceStatus)) {
    return true;
  }

  return ['paid', 'refunded', 'payment_failed'].includes(normalizedSettlementStatus);
}

async function releaseEphemeralBookingPaymentMethodsIfClosed(bookingId) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return { released: 0, skipped: true };
  }

  const connection = await pool.promise().getConnection();
  let booking = null;
  let ephemeralRows = [];

  try {
    await connection.beginTransaction();

    const [[bookingRow]] = await connection.query(
      `
      SELECT
        id,
        client_user_id,
        service_status,
        settlement_status
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [normalizedBookingId]
    );

    if (!bookingRow || !isBookingClosedForEphemeralPaymentCleanup(bookingRow)) {
      await connection.rollback();
      return { released: 0, skipped: true };
    }

    booking = bookingRow;

    const [rows] = await connection.query(
      `
      SELECT DISTINCT
        pm.id,
        pm.payment_type AS customer_payment_method_stripe_id
      FROM payment_method pm
      WHERE pm.user_id = ?
        AND pm.is_safed = 0
        AND pm.payment_type IS NOT NULL
        AND pm.payment_type LIKE 'pm_%'
        AND (
          pm.id = (
            SELECT b.selected_customer_payment_method_id
            FROM booking b
            WHERE b.id = ?
            LIMIT 1
          )
          OR pm.payment_type IN (
            SELECT p.payment_method_id
            FROM payments p
            WHERE p.booking_id = ?
              AND p.payment_method_id IS NOT NULL
          )
        )
      FOR UPDATE
      `,
      [bookingRow.client_user_id, normalizedBookingId, normalizedBookingId]
    );

    ephemeralRows = rows;
    await connection.commit();
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error preparing ephemeral payment method cleanup:', {
      bookingId: normalizedBookingId,
      error: error.message,
    });
    return { released: 0, skipped: false, error };
  } finally {
    connection.release();
  }

  const paymentMethodIdsToDetach = [...new Set(
    ephemeralRows
      .map((row) => row.customer_payment_method_stripe_id)
      .filter((value) => typeof value === 'string' && value.startsWith('pm_'))
  )];

  if (paymentMethodIdsToDetach.length === 0) {
    return { released: 0, skipped: false };
  }

  const detachedPaymentMethodIds = [];
  for (const paymentMethodId of paymentMethodIdsToDetach) {
    try {
      await stripe.paymentMethods.detach(paymentMethodId);
      detachedPaymentMethodIds.push(paymentMethodId);
    } catch (detachError) {
      const detachCode = typeof detachError?.code === 'string' ? detachError.code : '';
      const detachMessage = typeof detachError?.message === 'string' ? detachError.message : '';
      const canIgnoreDetachError = detachCode === 'resource_missing'
        || detachCode === 'payment_method_unexpected_state'
        || /already detached/i.test(detachMessage)
        || /not attached/i.test(detachMessage);

      if (canIgnoreDetachError) {
        detachedPaymentMethodIds.push(paymentMethodId);
        continue;
      }

      console.error('Error detaching ephemeral Stripe payment method:', {
        bookingId: normalizedBookingId,
        paymentMethodId,
        error: detachError.message,
      });
    }
  }

  if (detachedPaymentMethodIds.length === 0 || !booking?.client_user_id) {
    return { released: 0, skipped: false };
  }

  const cleanupConnection = await pool.promise().getConnection();
  try {
    await cleanupConnection.beginTransaction();
    await cleanupConnection.query(
      `
      UPDATE payment_method
      SET is_default = 0
      WHERE user_id = ?
        AND is_safed = 0
        AND payment_type IN (${detachedPaymentMethodIds.map(() => '?').join(', ')})
      `,
      [booking.client_user_id, ...detachedPaymentMethodIds]
    );
    await cleanupConnection.commit();
  } catch (cleanupError) {
    try { await cleanupConnection.rollback(); } catch {}
    console.error('Error updating detached ephemeral payment methods in DB:', {
      bookingId: normalizedBookingId,
      error: cleanupError.message,
    });
  } finally {
    cleanupConnection.release();
  }

  return {
    released: detachedPaymentMethodIds.length,
    skipped: false,
  };
}

async function resolvePaymentIntentPersistence(intentLike, { paymentMethodFallback = null } = {}) {
  let intent = intentLike || null;
  if (!intent || !intent.id) {
    return {
      intent,
      paymentMethodId: paymentMethodFallback?.id || null,
      paymentMethodLast4: paymentMethodFallback?.card?.last4 || null,
      lastErrorCode: null,
      lastErrorMessage: null,
    };
  }

  const needsExpandedIntent =
    typeof intent.payment_method === 'string' ||
    typeof intent.latest_charge === 'string';

  if (needsExpandedIntent) {
    try {
      intent = await stripe.paymentIntents.retrieve(intent.id, {
        expand: ['payment_method', 'latest_charge.payment_method_details'],
      });
    } catch (expandErr) {
      console.error('No se pudo rehidratar el PaymentIntent para persistencia:', {
        paymentIntentId: intent.id,
        error: expandErr.message,
      });
    }
  }

  const paymentMethodId =
    (intent?.payment_method && typeof intent.payment_method === 'object' ? intent.payment_method.id : intent?.payment_method) ||
    intent?.latest_charge?.payment_method ||
    paymentMethodFallback?.id ||
    null;

  const paymentMethod =
    (intent?.payment_method && typeof intent.payment_method === 'object' ? intent.payment_method : null) ||
    (intent?.last_payment_error?.payment_method && typeof intent.last_payment_error.payment_method === 'object'
      ? intent.last_payment_error.payment_method
      : null) ||
    paymentMethodFallback ||
    null;

  const lastError = intent?.last_payment_error || intent?.latest_charge?.last_payment_error || null;

  return {
    intent,
    paymentMethodId,
    paymentMethodLast4:
      paymentMethod?.card?.last4 ||
      intent?.latest_charge?.payment_method_details?.card?.last4 ||
      intent?.last_payment_error?.payment_method?.card?.last4 ||
      null,
    lastErrorCode: lastError?.code || lastError?.decline_code || null,
    lastErrorMessage: lastError?.message || null,
  };
}

async function getPaymentRow(conn, bookingId, type) {
  const [rows] = await conn.query(
    `SELECT id, booking_id, type, payment_intent_id, amount_cents, status, currency,
            payment_method_id, payment_method_last4,
            provider_payout_amount_cents,
            provider_payout_status,
            provider_payout_eligible_at,
            provider_payout_released_at,
            provider_payout_transfer_id,
            created_at,
            updated_at
     FROM payments
     WHERE booking_id = ? AND type = ?
     ORDER BY id DESC
     LIMIT 1
     FOR UPDATE`,
    [bookingId, type]
  );
  return rows[0] || null;
}

function getPaymentReferenceDateTime(paymentRow, fallback = new Date()) {
  return parseDateTimeInput(
    paymentRow?.updated_at
    ?? paymentRow?.created_at
    ?? fallback
  ) || (parseDateTimeInput(fallback) || new Date());
}

function resolveProviderPayoutReferenceDateTime({
  depositPayment,
  finalPayment,
  settlementSnapshot = null,
  now = new Date(),
} = {}) {
  const normalizedNow = parseDateTimeInput(now) || new Date();
  const shouldPreferFinalPayment = settlementSnapshot
    && Number(settlementSnapshot.amountDueFromClientCents || 0) > 0;
  const preferredPayment = shouldPreferFinalPayment
    ? (finalPayment || depositPayment)
    : (depositPayment || finalPayment);

  return getPaymentReferenceDateTime(preferredPayment, normalizedNow);
}

async function setPaymentProviderPayoutState(conn, paymentId, {
  amountCents = 0,
  status = 'none',
  eligibleAt = null,
  releasedAt = null,
  transferId = null,
} = {}) {
  const normalizedPaymentId = normalizeNullableInteger(paymentId);
  if (!normalizedPaymentId) {
    return;
  }

  const normalizedAmountCents = Math.max(0, Math.round(Number(amountCents || 0)));
  const normalizedStatus = normalizedAmountCents > 0 ? String(status || 'pending_release').trim().toLowerCase() : 'none';

  await conn.query(
    `
    UPDATE payments
    SET provider_payout_amount_cents = ?,
        provider_payout_status = ?,
        provider_payout_eligible_at = ?,
        provider_payout_released_at = ?,
        provider_payout_transfer_id = ?
    WHERE id = ?
    `,
    [
      normalizedAmountCents > 0 ? normalizedAmountCents : null,
      normalizedStatus,
      normalizedAmountCents > 0 ? toDbDateTime(eligibleAt) : null,
      normalizedAmountCents > 0 ? toDbDateTime(releasedAt) : null,
      normalizedAmountCents > 0 ? (transferId || null) : null,
      normalizedPaymentId,
    ]
  );
}

async function upsertPaymentRow(conn, { bookingId, type, amountCents, commissionSnapshotCents, finalPriceSnapshotCents, status, currency }) {
  await conn.query(
    `INSERT INTO payments (booking_id, type, amount_cents, commission_snapshot_cents, final_price_snapshot_cents, status, currency)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE amount_cents = VALUES(amount_cents), commission_snapshot_cents = VALUES(commission_snapshot_cents), final_price_snapshot_cents = VALUES(final_price_snapshot_cents), status = VALUES(status), currency = COALESCE(VALUES(currency), currency)`,
    [bookingId, type, amountCents, commissionSnapshotCents, finalPriceSnapshotCents, status, normalizeCurrencyCode(currency, 'EUR')]
  );
}

// UPSERT helper para la tabla payments
async function upsertPayment(conn, { bookingId, type, paymentIntentId, amountCents, commissionSnapshotCents, finalPriceSnapshotCents, status, currency, transferGroup, paymentMethodId, paymentMethodLast4, lastErrorCode, lastErrorMessage }) {
  await conn.query(`
    INSERT INTO payments (booking_id, type, payment_intent_id, amount_cents, commission_snapshot_cents, final_price_snapshot_cents, status, currency, transfer_group, payment_method_id, payment_method_last4, last_error_code, last_error_message, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
    ON DUPLICATE KEY UPDATE
      payment_intent_id = VALUES(payment_intent_id),
      amount_cents = VALUES(amount_cents),
      commission_snapshot_cents = COALESCE(VALUES(commission_snapshot_cents), commission_snapshot_cents),
      final_price_snapshot_cents = COALESCE(VALUES(final_price_snapshot_cents), final_price_snapshot_cents),
      status       = VALUES(status),
      currency = COALESCE(VALUES(currency), currency),
      transfer_group = COALESCE(VALUES(transfer_group), transfer_group),
      payment_method_id = COALESCE(VALUES(payment_method_id), payment_method_id),
      payment_method_last4 = COALESCE(VALUES(payment_method_last4), payment_method_last4),
      last_error_code = VALUES(last_error_code),
      last_error_message = VALUES(last_error_message)
  `, [bookingId, type, paymentIntentId, amountCents ?? 0, commissionSnapshotCents ?? null, finalPriceSnapshotCents ?? null, status, normalizeCurrencyCode(currency, 'EUR'), transferGroup || null, paymentMethodId || null, paymentMethodLast4 || null, lastErrorCode ?? null, lastErrorMessage ?? null]);
}

async function getBookingSettlementContext(connection, bookingId, { forUpdate = false } = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return null;
  }

  const [rows] = await connection.query(
    `
    SELECT
      b.id,
      b.client_user_id,
      b.provider_user_id_snapshot,
      b.service_status,
      b.settlement_status,
      b.requested_start_datetime,
      b.requested_end_datetime,
      b.requested_duration_minutes,
      b.price_type_snapshot,
      b.service_currency_snapshot,
      b.estimated_base_amount_cents,
      b.estimated_commission_amount_cents,
      b.estimated_total_amount_cents,
      b.deposit_amount_cents_snapshot,
      b.deposit_currency_snapshot,
      b.client_approval_deadline_at,
      COALESCE(s.user_id, b.provider_user_id_snapshot) AS effective_provider_user_id,
      cp.id AS closure_proposal_id,
      cp.status AS closure_status,
      cp.proposed_base_amount_cents,
      cp.proposed_commission_amount_cents,
      cp.proposed_total_amount_cents,
      cp.proposed_final_duration_minutes,
      cp.amount_due_from_client_cents,
      cp.amount_to_refund_cents,
      cp.provider_payout_amount_cents,
      cp.platform_amount_cents,
      cp.zero_charge_mode,
      cp.auto_charge_eligible,
      cp.auto_charge_scheduled_at,
      cust.email AS customer_email,
      cust.stripe_customer_id AS customer_id,
      cust.currency AS customer_currency,
      provider.stripe_account_id
    FROM booking b
    LEFT JOIN service s ON s.id = b.service_id
    LEFT JOIN user_account provider ON provider.id = COALESCE(b.provider_user_id_snapshot, s.user_id)
    LEFT JOIN user_account cust ON cust.id = b.client_user_id
    LEFT JOIN booking_closure_proposal cp ON cp.id = (
      SELECT cp2.id
      FROM booking_closure_proposal cp2
      WHERE cp2.booking_id = b.id
      ORDER BY cp2.id DESC
      LIMIT 1
    )
    WHERE b.id = ?
    LIMIT 1
    ${forUpdate ? 'FOR UPDATE' : ''}
    `,
    [normalizedBookingId]
  );

  return rows[0] || null;
}

function buildActualBookingSettlementSnapshot({
  booking,
  depositPayment,
}) {
  const bookingCurrency = normalizeCurrencyCode(booking?.service_currency_snapshot, 'EUR');
  const chargeCurrency = normalizeCurrencyCode(
    depositPayment?.currency,
    normalizeCurrencyCode(booking?.customer_currency, bookingCurrency)
  );
  const totalAmountInBookingCurrency = Number(
    booking?.proposed_total_amount_cents
    ?? booking?.estimated_total_amount_cents
    ?? 0
  );
  const providerPayoutAmountInBookingCurrency = Number(
    booking?.provider_payout_amount_cents
    ?? booking?.proposed_base_amount_cents
    ?? booking?.estimated_base_amount_cents
    ?? 0
  );
  const totalAmountInChargeCurrency = toMinorUnits(
    convertAmount(
      fromMinorUnits(totalAmountInBookingCurrency, bookingCurrency),
      bookingCurrency,
      chargeCurrency
    ),
    chargeCurrency
  );
  const providerPayoutAmountInChargeCurrency = toMinorUnits(
    convertAmount(
      fromMinorUnits(providerPayoutAmountInBookingCurrency, bookingCurrency),
      bookingCurrency,
      chargeCurrency
    ),
    chargeCurrency
  );
  const settlementAmounts = computeSettlementAmounts({
    depositAlreadyPaidAmountCents: Number(depositPayment?.amount_cents || 0),
    proposedTotalAmountCents: totalAmountInChargeCurrency,
    providerPayoutAmountCents: providerPayoutAmountInChargeCurrency,
    minimumChargeAmountCents: getMinimumChargeAmountCentsForCurrency(chargeCurrency),
  });

  return {
    bookingCurrency,
    chargeCurrency,
    totalAmountInBookingCurrency,
    providerPayoutAmountInBookingCurrency,
    totalAmountInChargeCurrency,
    providerPayoutAmountInChargeCurrency,
    effectiveTotalAmountCents: settlementAmounts.effectiveTotalAmountCents,
    amountDueFromClientCents: settlementAmounts.amountDueFromClientCents,
    amountToRefundCents: settlementAmounts.amountToRefundCents,
    providerPayoutAmountCents: settlementAmounts.providerPayoutAmountCents,
    platformAmountCents: settlementAmounts.platformAmountCents,
  };
}

async function assertProviderTransferReady(accountId) {
  if (!accountId || !String(accountId).startsWith('acct_')) {
    const error = new Error('La cuenta conectada no está lista para transferencias.');
    error.code = 'provider_transfer_account_invalid';
    throw error;
  }

  const account = await stripe.accounts.retrieve(accountId);
  const canTransfers = account?.capabilities?.transfers === 'active';
  const canPayouts = !!account?.payouts_enabled;
  if (!canTransfers || !canPayouts) {
    const error = new Error('La cuenta conectada no está lista para transferencias.');
    error.code = 'provider_transfer_account_not_ready';
    throw error;
  }
}

async function markBookingSettlementForReview(bookingId, {
  changedByUserId = null,
  nextSettlementStatus = 'manual_review_required',
  reasonCode = 'manual_review_required',
  details = 'Booking settlement requires manual review.',
  issueType = 'payment_dispute',
} = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return false;
  }

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();

    const booking = await getBookingSettlementContext(connection, normalizedBookingId, { forUpdate: true });
    if (!booking) {
      await connection.rollback();
      return false;
    }

    await upsertBookingIssueReport(connection, {
      bookingId: normalizedBookingId,
      reportedByUserId: changedByUserId,
      reportedAgainstUserId: booking.provider_user_id_snapshot || booking.effective_provider_user_id || null,
      issueType,
      status: 'open',
      details,
    });
    await transitionBookingStateRecord(connection, booking, {
      nextServiceStatus: normalizeServiceStatus(booking.service_status, 'pending_deposit') === 'finished'
        ? 'finished'
        : booking.service_status,
      nextSettlementStatus,
      changedByUserId,
      reasonCode,
      extraPatch: {
        client_approval_deadline_at: null,
      },
    });

    await connection.commit();

    try {
      await sendBookingSupportAlertEmail({
        bookingId: normalizedBookingId,
        headline: 'Reserva en revisión manual',
        details,
        category: issueType,
      });
    } catch (emailError) {
      console.error('Error sending booking support alert email:', {
        bookingId: normalizedBookingId,
        error: emailError.message,
      });
    }

    return true;
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error marking booking settlement for review:', {
      bookingId: normalizedBookingId,
      error: error.message,
    });
    return false;
  } finally {
    connection.release();
  }
}

async function finalizeBookingSettlementAfterSuccessfulPayment(bookingId, {
  changedByUserId = null,
  reasonCode = 'final_payment_settled',
} = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return { settled: false, reason: 'invalid_booking_id' };
  }

  let booking = null;
  let depositPayment = null;
  let finalPayment = null;
  let settlementSnapshot = null;
  let transferGroup = null;

  const initialConnection = await pool.promise().getConnection();
  try {
    await initialConnection.beginTransaction();

    booking = await getBookingSettlementContext(initialConnection, normalizedBookingId, { forUpdate: true });
    if (!booking) {
      await initialConnection.rollback();
      return { settled: false, reason: 'booking_not_found' };
    }

    depositPayment = await getPaymentRow(initialConnection, normalizedBookingId, 'deposit');
    finalPayment = await getPaymentRow(initialConnection, normalizedBookingId, 'final');
    if (!depositPayment) {
      await initialConnection.rollback();
      return { settled: false, reason: 'deposit_missing' };
    }

    settlementSnapshot = buildActualBookingSettlementSnapshot({
      booking,
      depositPayment,
    });
    transferGroup = finalPayment?.transfer_group || depositPayment?.transfer_group || `booking-${normalizedBookingId}`;
    const normalizedSettlementStatus = normalizeSettlementStatus(booking.settlement_status, 'none');
    if (
      settlementSnapshot.amountDueFromClientCents > 0
      && !['succeeded', 'partially_refunded', 'refunded'].includes(String(finalPayment?.status || '').trim().toLowerCase())
      && normalizedSettlementStatus !== 'paid'
    ) {
      await initialConnection.rollback();
      return { settled: false, reason: 'final_payment_not_succeeded' };
    }

    await initialConnection.commit();
  } catch (error) {
    try { await initialConnection.rollback(); } catch {}
    console.error('Error loading booking settlement context:', {
      bookingId: normalizedBookingId,
      error: error.message,
    });
    return { settled: false, reason: 'load_failed', error };
  } finally {
    initialConnection.release();
  }

  try {
    if (settlementSnapshot.amountToRefundCents > 0 && depositPayment?.payment_intent_id) {
      await ensureRefundForPaymentIntent({
        paymentIntentId: depositPayment.payment_intent_id,
        amountCents: settlementSnapshot.amountToRefundCents,
        transferGroup,
        metadata: {
          booking_id: String(normalizedBookingId),
          source: 'booking_settlement_refund',
        },
      });
    }
  } catch (externalError) {
    console.error('Error finalizing booking settlement externally:', {
      bookingId: normalizedBookingId,
      error: externalError.message,
    });
    await markBookingSettlementForReview(normalizedBookingId, {
      changedByUserId,
      nextSettlementStatus: 'manual_review_required',
      reasonCode: 'booking_settlement_manual_review',
      details: `Booking settlement could not be completed automatically: ${externalError.message}`,
    });
    return { settled: false, reason: 'external_processing_failed', error: externalError };
  }

  const finalizeConnection = await pool.promise().getConnection();
  try {
    await finalizeConnection.beginTransaction();

    const currentBooking = await getBookingSettlementContext(finalizeConnection, normalizedBookingId, { forUpdate: true });
    const currentFinalPayment = await getPaymentRow(finalizeConnection, normalizedBookingId, 'final');
    if (!currentBooking) {
      await finalizeConnection.rollback();
      return { settled: false, reason: 'booking_not_found_after_processing' };
    }

    await upsertPayment(finalizeConnection, {
      bookingId: normalizedBookingId,
      type: 'final',
      paymentIntentId: currentFinalPayment?.payment_intent_id || null,
      amountCents: settlementSnapshot.amountDueFromClientCents,
      commissionSnapshotCents: settlementSnapshot.platformAmountCents,
      finalPriceSnapshotCents: settlementSnapshot.effectiveTotalAmountCents,
      status: 'succeeded',
      currency: settlementSnapshot.chargeCurrency,
      transferGroup,
      paymentMethodId: currentFinalPayment?.payment_method_id || null,
      paymentMethodLast4: currentFinalPayment?.payment_method_last4 || null,
      lastErrorCode: null,
      lastErrorMessage: null,
    });
    const payoutPayment = await getPaymentRow(finalizeConnection, normalizedBookingId, 'final');
    if (payoutPayment?.id) {
      if (settlementSnapshot.providerPayoutAmountCents > 0) {
        await setPaymentProviderPayoutState(finalizeConnection, payoutPayment.id, {
          amountCents: settlementSnapshot.providerPayoutAmountCents,
          status: 'pending_release',
          eligibleAt: deriveProviderPayoutEligibleAt(
            resolveProviderPayoutReferenceDateTime({
              depositPayment,
              finalPayment: payoutPayment,
              settlementSnapshot,
            })
          ),
        });
      } else {
        await setPaymentProviderPayoutState(finalizeConnection, payoutPayment.id, {
          amountCents: 0,
          status: 'none',
        });
      }
    }
    await transitionBookingStateRecord(finalizeConnection, currentBooking, {
      nextServiceStatus: 'finished',
      nextSettlementStatus: 'paid',
      changedByUserId,
      reasonCode,
      extraPatch: {
        client_approval_deadline_at: null,
      },
    });

    await finalizeConnection.commit();
  } catch (error) {
    try { await finalizeConnection.rollback(); } catch {}
    console.error('Error persisting finalized booking settlement:', {
      bookingId: normalizedBookingId,
      error: error.message,
    });
    return { settled: false, reason: 'persist_failed', error };
  } finally {
    finalizeConnection.release();
  }

  try {
    await releaseEphemeralBookingPaymentMethodsIfClosed(normalizedBookingId);
  } catch (cleanupError) {
    console.error('Error releasing ephemeral payment methods after booking settlement:', {
      bookingId: normalizedBookingId,
      error: cleanupError.message,
    });
  }

  return {
    settled: true,
    settlementSnapshot,
    transferGroup,
  };
}

async function openBookingIssueDispute(bookingId, {
  issueType,
  reportedByUserId = null,
  reportedAgainstUserId = null,
  details = null,
  reasonCode = 'booking_issue_opened',
} = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  const normalizedIssueType = String(issueType || '').trim().toLowerCase();
  if (!normalizedBookingId || !normalizedIssueType) {
    return { opened: false, reason: 'invalid_input' };
  }

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();

    const booking = await getBookingSettlementContext(connection, normalizedBookingId, { forUpdate: true });
    if (!booking) {
      await connection.rollback();
      return { opened: false, reason: 'booking_not_found' };
    }

    const normalizedReportedAgainstUserId = normalizeNullableInteger(reportedAgainstUserId)
      || (
        normalizedIssueType === 'no_show_provider'
          ? normalizeNullableInteger(booking.provider_user_id_snapshot || booking.effective_provider_user_id)
          : normalizedIssueType === 'no_show_client'
            ? normalizeNullableInteger(booking.client_user_id)
            : null
      );

    const normalizedDetails = normalizeNullableText(details) || 'La reserva requiere revisión manual de soporte.';
    const issueReportId = await upsertBookingIssueReport(connection, {
      bookingId: normalizedBookingId,
      reportedByUserId,
      reportedAgainstUserId: normalizedReportedAgainstUserId,
      issueType: normalizedIssueType,
      status: 'open',
      details: normalizedDetails,
      resolvedAt: null,
    });

    await transitionBookingStateRecord(connection, booking, {
      nextServiceStatus: booking.service_status,
      nextSettlementStatus: 'in_dispute',
      changedByUserId: reportedByUserId,
      reasonCode,
      extraPatch: {
        client_approval_deadline_at: null,
      },
    });

    await connection.commit();

    try {
      await sendBookingSupportAlertEmail({
        bookingId: normalizedBookingId,
        headline: 'Reserva enviada a disputa',
        details: normalizedDetails,
        category: normalizedIssueType,
      });
    } catch (emailError) {
      console.error('Error sending booking dispute support alert:', {
        bookingId: normalizedBookingId,
        issueType: normalizedIssueType,
        error: emailError.message,
      });
    }

    return {
      opened: true,
      issueReportId,
    };
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error opening booking issue dispute:', {
      bookingId: normalizedBookingId,
      issueType: normalizedIssueType,
      error: error.message,
    });
    return { opened: false, reason: 'open_failed', error };
  } finally {
    connection.release();
  }
}

async function processCanceledBookingIssueOutcome(bookingId, {
  issueType,
  changedByUserId = null,
  details = null,
  resolution = null,
} = {}) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  const normalizedIssueType = String(issueType || '').trim().toLowerCase();
  const normalizedResolution = String(resolution || '').trim().toLowerCase();
  if (!normalizedBookingId || !normalizedIssueType) {
    return { handled: false, reason: 'invalid_input' };
  }

  const providerFailure = normalizedResolution === 'refund_client_full'
    || normalizedIssueType === 'no_show_provider'
    || normalizedIssueType === 'last_minute_provider';
  const clientFailure = normalizedResolution === 'keep_deposit'
    || normalizedIssueType === 'no_show_client'
    || normalizedIssueType === 'last_minute_client';
  if (!providerFailure && !clientFailure) {
    return { handled: false, reason: 'unsupported_issue_type' };
  }

  let booking = null;
  let depositPayment = null;

  const initialConnection = await pool.promise().getConnection();
  try {
    await initialConnection.beginTransaction();

    booking = await getBookingSettlementContext(initialConnection, normalizedBookingId, { forUpdate: true });
    if (!booking) {
      await initialConnection.rollback();
      return { handled: false, reason: 'booking_not_found' };
    }

    depositPayment = await getPaymentRow(initialConnection, normalizedBookingId, 'deposit');

    await upsertBookingIssueReport(initialConnection, {
      bookingId: normalizedBookingId,
      reportedByUserId: changedByUserId,
      reportedAgainstUserId: providerFailure
        ? (booking.provider_user_id_snapshot || booking.effective_provider_user_id || null)
        : booking.client_user_id,
      issueType: normalizedIssueType,
      status: 'resolved',
      details: details || (
        providerFailure
          ? 'Se ha cancelado la reserva y se devolverá el depósito al cliente.'
          : 'Se ha cancelado la reserva y el depósito pasa al profesional como compensación.'
      ),
      resolvedAt: new Date(),
    });

    if (providerFailure) {
      await incrementUserStrikeCount(
        initialConnection,
        booking.provider_user_id_snapshot || booking.effective_provider_user_id || null,
        1
      );
    }

    await transitionBookingStateRecord(initialConnection, booking, {
      nextServiceStatus: 'canceled',
      nextSettlementStatus: providerFailure && depositPayment?.payment_intent_id ? 'refund_pending' : booking.settlement_status,
      changedByUserId,
      reasonCode: normalizedIssueType,
      extraPatch: {
        canceled_by_user_id: changedByUserId,
        cancellation_reason_code: normalizedIssueType,
        client_approval_deadline_at: null,
      },
    });

    if (providerFailure && depositPayment?.id && canInitiateDepositRefund(depositPayment)) {
      await initialConnection.query(
        'UPDATE payments SET status = ? WHERE id = ?',
        ['refund_pending', depositPayment.id]
      );
    } else if (clientFailure && depositPayment?.id && Number(depositPayment.amount_cents || 0) > 0) {
      await setPaymentProviderPayoutState(initialConnection, depositPayment.id, {
        amountCents: Number(depositPayment.amount_cents || 0),
        status: 'pending_release',
        eligibleAt: deriveProviderPayoutEligibleAt(
          getPaymentReferenceDateTime(depositPayment)
        ),
      });
    }

    await initialConnection.commit();
  } catch (error) {
    try { await initialConnection.rollback(); } catch {}
    console.error('Error preparing canceled booking issue outcome:', {
      bookingId: normalizedBookingId,
      issueType: normalizedIssueType,
      error: error.message,
    });
    return { handled: false, reason: 'prepare_failed', error };
  } finally {
    initialConnection.release();
  }

  try {
    if (providerFailure && depositPayment?.payment_intent_id) {
      await ensureRefundForPaymentIntent({
        paymentIntentId: depositPayment.payment_intent_id,
        metadata: {
          booking_id: String(normalizedBookingId),
          source: normalizedIssueType,
        },
      });
    }
  } catch (externalError) {
    console.error('Error processing canceled booking issue outcome externally:', {
      bookingId: normalizedBookingId,
      issueType: normalizedIssueType,
      error: externalError.message,
    });
    await markBookingSettlementForReview(normalizedBookingId, {
      changedByUserId,
      nextSettlementStatus: 'manual_review_required',
      reasonCode: `${normalizedIssueType}_manual_review`,
      details: `No se pudo completar automáticamente la resolución de ${normalizedIssueType}: ${externalError.message}`,
    });
    return { handled: false, reason: 'external_processing_failed', error: externalError };
  }

  if (clientFailure) {
    const finalizeConnection = await pool.promise().getConnection();
    try {
      await finalizeConnection.beginTransaction();
      const currentBooking = await getBookingSettlementContext(finalizeConnection, normalizedBookingId, { forUpdate: true });
      if (currentBooking) {
        await transitionBookingStateRecord(finalizeConnection, currentBooking, {
          nextServiceStatus: 'canceled',
          nextSettlementStatus: 'paid',
          changedByUserId,
          reasonCode: `${normalizedIssueType}_settled`,
        });
      }
      await finalizeConnection.commit();
    } catch (error) {
      try { await finalizeConnection.rollback(); } catch {}
      console.error('Error marking client failure booking as settled:', {
        bookingId: normalizedBookingId,
        issueType: normalizedIssueType,
        error: error.message,
      });
      return { handled: false, reason: 'persist_failed', error };
    } finally {
      finalizeConnection.release();
    }
  }

  try {
    await releaseEphemeralBookingPaymentMethodsIfClosed(normalizedBookingId);
  } catch (cleanupError) {
    console.error('Error releasing ephemeral payment methods after canceled booking issue outcome:', {
      bookingId: normalizedBookingId,
      issueType: normalizedIssueType,
      error: cleanupError.message,
    });
  }

  return {
    handled: true,
    issueType: normalizedIssueType,
    providerFailure,
    clientFailure,
  };
}

async function processPendingClosureAutoCharge(bookingId) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return { handled: false, reason: 'invalid_booking_id' };
  }

  let booking = null;
  let depositPayment = null;
  let finalPayment = null;
  let selectedPaymentMethodId = null;
  let customerId = null;
  let settlementSnapshot = null;
  let autoChargeDecision = null;
  let transferGroup = `booking-${normalizedBookingId}`;
  let isSecondaryReminderWindow = false;
  let shouldSendToleranceEmail = false;

  async function scheduleClosureFollowUpWindow({
    details,
    reasonCode = 'auto_charge_follow_up_scheduled',
  }) {
    const followUpConnection = await pool.promise().getConnection();
    try {
      await followUpConnection.beginTransaction();

      const currentBooking = await getBookingSettlementContext(
        followUpConnection,
        normalizedBookingId,
        { forUpdate: true }
      );
      if (!currentBooking) {
        await followUpConnection.rollback();
        return false;
      }

      if (normalizeSettlementStatus(currentBooking.settlement_status, 'none') !== 'pending_client_approval') {
        await followUpConnection.rollback();
        return false;
      }

      await transitionBookingStateRecord(followUpConnection, currentBooking, {
        nextServiceStatus: currentBooking.service_status,
        nextSettlementStatus: 'pending_client_approval',
        reasonCode,
        extraPatch: {
          client_approval_deadline_at: buildSecondaryClientApprovalDeadline(),
        },
      });

      await followUpConnection.commit();
    } catch (error) {
      try { await followUpConnection.rollback(); } catch {}
      throw error;
    } finally {
      followUpConnection.release();
    }

    try {
      await sendClosureAutoChargeNotificationEmail({
        bookingId: normalizedBookingId,
        mode: 'approval_follow_up',
      });
    } catch (emailError) {
      console.error('Error sending closure follow-up email:', {
        bookingId: normalizedBookingId,
        error: emailError.message,
      });
    }

    return true;
  }

  async function escalateClosureToDispute({
    details,
    reasonCode = 'auto_charge_requires_manual_approval',
  }) {
    const disputeConnection = await pool.promise().getConnection();
    try {
      await disputeConnection.beginTransaction();

      const currentBooking = await getBookingSettlementContext(
        disputeConnection,
        normalizedBookingId,
        { forUpdate: true }
      );
      if (!currentBooking) {
        await disputeConnection.rollback();
        return false;
      }

      await upsertBookingIssueReport(disputeConnection, {
        bookingId: normalizedBookingId,
        reportedAgainstUserId: currentBooking.provider_user_id_snapshot || currentBooking.effective_provider_user_id || null,
        issueType: 'payment_dispute',
        status: 'open',
        details,
      });

      await transitionBookingStateRecord(disputeConnection, currentBooking, {
        nextServiceStatus: currentBooking.service_status,
        nextSettlementStatus: 'in_dispute',
        reasonCode,
        extraPatch: {
          client_approval_deadline_at: null,
        },
      });

      await disputeConnection.commit();
    } catch (error) {
      try { await disputeConnection.rollback(); } catch {}
      throw error;
    } finally {
      disputeConnection.release();
    }

    try {
      await sendClosureAutoChargeNotificationEmail({
        bookingId: normalizedBookingId,
        mode: 'manual_review_required',
      });
    } catch (emailError) {
      console.error('Error sending manual approval required email:', {
        bookingId: normalizedBookingId,
        error: emailError.message,
      });
    }

    return true;
  }

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();

    booking = await getBookingSettlementContext(connection, normalizedBookingId, { forUpdate: true });
    if (!booking) {
      await connection.rollback();
      return { handled: false, reason: 'booking_not_found' };
    }

    if (normalizeSettlementStatus(booking.settlement_status, 'none') !== 'pending_client_approval') {
      await connection.rollback();
      return { handled: false, reason: 'not_pending_client_approval' };
    }

    isSecondaryReminderWindow = isSecondaryClientApprovalWindowActive(booking);
    const deadline = parseDateTimeInput(booking.client_approval_deadline_at);
    if (deadline && deadline.getTime() > Date.now()) {
      await connection.rollback();
      return { handled: false, reason: 'deadline_not_reached' };
    }

    depositPayment = await getPaymentRow(connection, normalizedBookingId, 'deposit');
    if (!depositPayment) {
      await connection.rollback();
      await markBookingSettlementForReview(normalizedBookingId, {
        nextSettlementStatus: 'in_dispute',
        reasonCode: 'auto_charge_missing_deposit',
        details: 'La reserva no tiene un depósito confirmado para poder cerrar automáticamente.',
      });
      return { handled: false, reason: 'missing_deposit' };
    }

    finalPayment = await getPaymentRow(connection, normalizedBookingId, 'final');
    const normalizedFinalPaymentStatus = String(finalPayment?.status || '').trim().toLowerCase();
    if (normalizedFinalPaymentStatus === 'succeeded') {
      await connection.commit();
      return finalizeBookingSettlementAfterSuccessfulPayment(normalizedBookingId, {
        reasonCode: 'auto_charge_existing_payment_succeeded',
      });
    }
    if (normalizedFinalPaymentStatus === 'processing') {
      await connection.rollback();
      return { handled: true, reason: 'processing' };
    }

    autoChargeDecision = evaluateAutoChargeEligibility({
      priceType: booking.price_type_snapshot,
      estimatedTotalAmountCents: booking.estimated_total_amount_cents,
      proposedTotalAmountCents: booking.proposed_total_amount_cents,
      estimatedDurationMinutes: booking.requested_duration_minutes,
      proposedFinalDurationMinutes: booking.proposed_final_duration_minutes,
      zeroChargeMode: booking.zero_charge_mode === 1 || booking.zero_charge_mode === true,
      toleranceFactor: AUTO_CHARGE_TOLERANCE_FACTOR,
    });

    if (!autoChargeDecision.eligible) {
      const details = `El importe final requiere respuesta del cliente antes de cerrar la reserva (${autoChargeDecision.reason}).`;
      if (!isSecondaryReminderWindow) {
        await transitionBookingStateRecord(connection, booking, {
          nextServiceStatus: booking.service_status,
          nextSettlementStatus: 'pending_client_approval',
          reasonCode: 'auto_charge_follow_up_scheduled',
          extraPatch: {
            client_approval_deadline_at: buildSecondaryClientApprovalDeadline(),
          },
        });
        await connection.commit();

        try {
          await sendClosureAutoChargeNotificationEmail({
            bookingId: normalizedBookingId,
            mode: 'approval_follow_up',
          });
        } catch (emailError) {
          console.error('Error sending closure follow-up email:', {
            bookingId: normalizedBookingId,
            error: emailError.message,
          });
        }

        return { handled: true, reason: 'secondary_reminder_scheduled' };
      }

      await upsertBookingIssueReport(connection, {
        bookingId: normalizedBookingId,
        reportedAgainstUserId: booking.provider_user_id_snapshot || booking.effective_provider_user_id || null,
        issueType: 'payment_dispute',
        status: 'open',
        details,
      });
      await transitionBookingStateRecord(connection, booking, {
        nextServiceStatus: booking.service_status,
        nextSettlementStatus: 'in_dispute',
        reasonCode: 'auto_charge_requires_manual_approval',
        extraPatch: {
          client_approval_deadline_at: null,
        },
      });
      await connection.commit();

      try {
        await sendClosureAutoChargeNotificationEmail({
          bookingId: normalizedBookingId,
          mode: 'manual_review_required',
        });
      } catch (emailError) {
        console.error('Error sending manual approval required email:', {
          bookingId: normalizedBookingId,
          error: emailError.message,
        });
      }

      return { handled: true, reason: 'manual_review_required' };
    }

    if (booking.closure_proposal_id && booking.closure_status === 'active') {
      await updateClosureProposalStatus(connection, booking.closure_proposal_id, 'accepted', new Date());
    }

    customerId = booking.customer_id || await ensureStripeCustomerId(connection, {
      userId: booking.client_user_id,
      email: booking.customer_email,
    });
    settlementSnapshot = buildActualBookingSettlementSnapshot({
      booking,
      depositPayment,
    });
    transferGroup = depositPayment.transfer_group || transferGroup;
    shouldSendToleranceEmail = autoChargeDecision.needsAdjustmentNotice === true;

    if (settlementSnapshot.amountDueFromClientCents <= 0) {
      await connection.commit();
      if (shouldSendToleranceEmail) {
        try {
          await sendClosureAutoChargeNotificationEmail({
            bookingId: normalizedBookingId,
            mode: 'within_tolerance',
          });
        } catch (emailError) {
          console.error('Error sending auto-charge tolerance email:', {
            bookingId: normalizedBookingId,
            error: emailError.message,
          });
        }
      }

      return finalizeBookingSettlementAfterSuccessfulPayment(normalizedBookingId, {
        reasonCode: 'auto_charge_covered_by_deposit',
      });
    }

    selectedPaymentMethodId = await getBookingSelectedStripePaymentMethodId(connection, normalizedBookingId, { forUpdate: true });
    if (!selectedPaymentMethodId) {
      const details = 'No hay un método de pago disponible para cerrar automáticamente la reserva.';
      if (!isSecondaryReminderWindow) {
        await transitionBookingStateRecord(connection, booking, {
          nextServiceStatus: booking.service_status,
          nextSettlementStatus: 'pending_client_approval',
          reasonCode: 'auto_charge_follow_up_scheduled',
          extraPatch: {
            client_approval_deadline_at: buildSecondaryClientApprovalDeadline(),
          },
        });
        await connection.commit();

        try {
          await sendClosureAutoChargeNotificationEmail({
            bookingId: normalizedBookingId,
            mode: 'approval_follow_up',
          });
        } catch (emailError) {
          console.error('Error sending closure follow-up email:', {
            bookingId: normalizedBookingId,
            error: emailError.message,
          });
        }

        return { handled: true, reason: 'missing_payment_method_follow_up' };
      }

      await upsertBookingIssueReport(connection, {
        bookingId: normalizedBookingId,
        reportedAgainstUserId: booking.provider_user_id_snapshot || booking.effective_provider_user_id || null,
        issueType: 'payment_dispute',
        status: 'open',
        details,
      });
      await transitionBookingStateRecord(connection, booking, {
        nextServiceStatus: booking.service_status,
        nextSettlementStatus: 'in_dispute',
        reasonCode: 'auto_charge_missing_payment_method',
        extraPatch: {
          client_approval_deadline_at: null,
        },
      });
      await connection.commit();

      try {
        await sendClosureAutoChargeNotificationEmail({
          bookingId: normalizedBookingId,
          mode: 'manual_review_required',
        });
      } catch (emailError) {
        console.error('Error sending manual approval required email:', {
          bookingId: normalizedBookingId,
          error: emailError.message,
        });
      }

      return { handled: true, reason: 'missing_payment_method_disputed' };
    }

    await connection.commit();
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error preparing pending closure auto-charge:', {
      bookingId: normalizedBookingId,
      error: error.message,
    });
    return { handled: false, reason: 'prepare_failed', error };
  } finally {
    connection.release();
  }

  if (shouldSendToleranceEmail) {
    try {
      await sendClosureAutoChargeNotificationEmail({
        bookingId: normalizedBookingId,
        mode: 'within_tolerance',
      });
    } catch (emailError) {
      console.error('Error sending auto-charge tolerance email:', {
        bookingId: normalizedBookingId,
        error: emailError.message,
      });
    }
  }

  const persistConnection = await pool.promise().getConnection();
  let payment = null;
  try {
    await persistConnection.beginTransaction();
    await upsertPaymentRow(persistConnection, {
      bookingId: normalizedBookingId,
      type: 'final',
      amountCents: settlementSnapshot.amountDueFromClientCents,
      commissionSnapshotCents: settlementSnapshot.platformAmountCents,
      finalPriceSnapshotCents: settlementSnapshot.effectiveTotalAmountCents,
      status: 'creating',
      currency: settlementSnapshot.chargeCurrency,
    });
    payment = await getPaymentRow(persistConnection, normalizedBookingId, 'final');
    await persistConnection.commit();
  } catch (error) {
    try { await persistConnection.rollback(); } catch {}
    return { handled: false, reason: 'payment_persist_failed', error };
  } finally {
    persistConnection.release();
  }

  try {
    let paymentMethod = await stripe.paymentMethods.retrieve(selectedPaymentMethodId);
    if (paymentMethod.customer && paymentMethod.customer !== customerId) {
      throw new Error('payment_method_id pertenece a otro customer.');
    }
    if (!paymentMethod.customer) {
      paymentMethod = await stripe.paymentMethods.attach(selectedPaymentMethodId, { customer: customerId });
    }

    let intent = null;
    if (payment?.payment_intent_id) {
      intent = await stripe.paymentIntents.retrieve(payment.payment_intent_id, {
        expand: ['payment_method', 'latest_charge.payment_method_details'],
      });
      if (intent.status === 'requires_payment_method') {
        await stripe.paymentIntents.update(intent.id, {
          payment_method: selectedPaymentMethodId,
          customer: customerId,
        });
        intent = await stripe.paymentIntents.confirm(intent.id, { off_session: true });
      }
    } else {
      intent = await stripe.paymentIntents.create(
        {
          amount: settlementSnapshot.amountDueFromClientCents,
          currency: toStripeCurrencyCode(settlementSnapshot.chargeCurrency),
          customer: customerId,
          payment_method: selectedPaymentMethodId,
          confirm: true,
          off_session: true,
          receipt_email: booking.customer_email || undefined,
          automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
          transfer_group: transferGroup,
          metadata: {
            booking_id: String(normalizedBookingId),
            type: 'final',
            auto_charge: '1',
          },
        },
        {
          idempotencyKey: stableKey(['auto_charge', normalizedBookingId, settlementSnapshot.amountDueFromClientCents, selectedPaymentMethodId]),
        }
      );
    }

    const paymentPersistence = await resolvePaymentIntentPersistence(intent, {
      paymentMethodFallback: paymentMethod,
    });
    intent = paymentPersistence.intent || intent;

    const finalizePaymentConnection = await pool.promise().getConnection();
    try {
      await finalizePaymentConnection.beginTransaction();
      await upsertPayment(finalizePaymentConnection, {
        bookingId: normalizedBookingId,
        type: 'final',
        paymentIntentId: intent.id,
        amountCents: settlementSnapshot.amountDueFromClientCents,
        commissionSnapshotCents: settlementSnapshot.platformAmountCents,
        finalPriceSnapshotCents: settlementSnapshot.effectiveTotalAmountCents,
        status: mapStatus(intent.status),
        currency: settlementSnapshot.chargeCurrency,
        transferGroup,
        paymentMethodId: paymentPersistence.paymentMethodId || selectedPaymentMethodId,
        paymentMethodLast4: paymentPersistence.paymentMethodLast4 || null,
        lastErrorCode: paymentPersistence.lastErrorCode,
        lastErrorMessage: paymentPersistence.lastErrorMessage,
      });
      if (paymentPersistence.paymentMethodId) {
        await syncBookingSelectedPaymentMethodFromIntent(finalizePaymentConnection, {
          bookingId: normalizedBookingId,
          userId: booking.client_user_id,
          intent,
          paymentMethodFallback: paymentMethod,
          saveForFuture: false,
        });
      }
      await finalizePaymentConnection.commit();
    } catch (error) {
      try { await finalizePaymentConnection.rollback(); } catch {}
      throw error;
    } finally {
      finalizePaymentConnection.release();
    }

    if (intent.status === 'succeeded') {
      return finalizeBookingSettlementAfterSuccessfulPayment(normalizedBookingId, {
        reasonCode: 'auto_charge_succeeded',
      });
    }

    if (intent.status === 'processing') {
      return { handled: true, reason: 'processing' };
    }

    const failureDetails = `El cobro automático ha quedado en estado ${intent.status}.`;
    if (!isSecondaryReminderWindow) {
      await scheduleClosureFollowUpWindow({
        details: failureDetails,
        reasonCode: 'auto_charge_follow_up_scheduled',
      });
      return { handled: true, reason: `intent_${intent.status}_follow_up` };
    }

    await escalateClosureToDispute({
      details: failureDetails,
      reasonCode: 'auto_charge_failed',
    });
    return { handled: false, reason: `intent_${intent.status}` };
  } catch (error) {
    console.error('Error processing closure auto-charge:', {
      bookingId: normalizedBookingId,
      error: error.message,
    });
    const failureDetails = `El cobro automático ha fallado: ${error.message}`;
    if (!isSecondaryReminderWindow) {
      try {
        await scheduleClosureFollowUpWindow({
          details: failureDetails,
          reasonCode: 'auto_charge_follow_up_scheduled',
        });
        return { handled: true, reason: 'auto_charge_failed_follow_up' };
      } catch (followUpError) {
        console.error('Error scheduling closure follow-up after auto-charge failure:', {
          bookingId: normalizedBookingId,
          error: followUpError.message,
        });
      }
    }

    await escalateClosureToDispute({
      details: failureDetails,
      reasonCode: 'auto_charge_failed',
    });
    return { handled: false, reason: 'auto_charge_failed', error };
  }
}

// Muestreo ponderado SIN reemplazo (ruleta recalculando pesos)
function weightedSampleWithoutReplacement(arr, k) {
  const pool = arr.slice();
  const out = [];
  for (let i = 0; i < k && pool.length > 0; i++) {
    const total = pool.reduce((s, x) => s + x.weight, 0);
    let r = Math.random() * total;
    let idx = 0;
    while (idx < pool.length && r > pool[idx].weight) {
      r -= pool[idx].weight;
      idx++;
    }
    out.push(pool.splice(Math.min(idx, pool.length - 1), 1)[0]);
  }
  return out;
}

// helper para renderizar el correo
function renderEmail({
  termsUrl = 'https://example.com/terms',
  privacyUrl = 'https://example.com/privacy',
  effectiveDate = 'September 7, 2025',
  productName = 'Wisdom',
  iconUrl = 'https://storage.googleapis.com/wisdom-images/app_icon.png',
  preheader = `A smoother experience from search to booking—come take a look.`
} = {}) {
  const subject = `We've released a new Wisdom beta`;

  const text = `Hi there,

We're writing to let you know that we're updating our ${productName} Terms of Service and Privacy Policy. These changes do three things: keep us aligned with current laws and regulations, support new features we've introduced, and bring more clarity to how ${productName} works.

These updates will take effect on ${effectiveDate}. By continuing to use ${productName} after that date, you'll be agreeing to the new terms and privacy policy.

With gratitude,
The ${productName} Team`;

  const html = `<!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <meta name="color-scheme" content="light dark">
      <meta name="supported-color-schemes" content="light dark">
      <title>We’ve released a new Wisdom beta</title>
      <style>
        .content { font-family: Inter, Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:15px; line-height:1.65; }
        @media (max-width:480px) { .content { font-size:16px !important; line-height:1.7 !important; } }
        a { text-decoration: underline; color: inherit; }
        body, table, td, p { margin:0; }
      </style>
    </head>
    <body style="margin:0; padding:0; background:none !important;">
      <!-- preheader -->
      <div style="display:none; font-size:1px; line-height:1px; max-height:0; max-width:0; opacity:0; overflow:hidden;">
        A smoother experience from search to booking—come take a look.
      </div>

      <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
        <tr>
          <td align="center" style="padding:0 16px;">
            <table role="presentation" width="640" border="0" cellspacing="0" cellpadding="0" style="width:100%; max-width:640px; border-collapse:collapse;">

              <!-- Logo left -->
              <tr>
                <td align="left" style="padding:24px 8px 8px 8px;">
                  <img src="https://storage.googleapis.com/wisdom-images/app_icon.png" width="36" height="36" alt="Wisdom"
                      style="display:block; border:0; outline:none; text-decoration:none; width:36px; height:36px;">
                </td>
              </tr>

              <!-- Body -->
              <tr>
                <td class="content" style="padding:8px 8px 24px 8px;">
                  <p style="margin:0 0 16px;">Hi there,</p>

                  <p style="margin:0 0 16px;">
                    We're excited to let you know we've released a new update of the Wisdom beta.
                  </p>

                  <p style="margin:0 0 16px;">
                    Open Wisdom to explore a smoother, cleaner experience from search to booking—everything feels more profesional,
                    clearer, and more delightful throughout.
                  </p>

                  <p style="margin:0 0 16px;">
                    If anything feels off, just reply to this email — we're listening.
                  </p>

                  <p style="margin:0 0 16px;">
                    Jump in: book a service you need or offer your own to the community.
                  </p>

                  <p style="margin:0;">
                    With gratitude,<br>
                    <em>The Wisdom Team</em>
                  </p>
                </td>
              </tr>

            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>`;

  return { subject, text, html };
}

function renderDepositReservationEmail({ booking, depositAmountCents, currency = 'EUR', finalPriceSnapshotCents = null }) {
  if (!booking) {
    return {
      subject: 'Se ha confirmado una reserva',
      text: 'Hola equipo, se ha confirmado una reserva.',
      html: '<p>Hola equipo, se ha confirmado una reserva.</p>'
    };
  }

  const notificationCurrency = normalizeCurrencyCode(currency, 'EUR');
  const depositAmount = fromMinorUnits(depositAmountCents, notificationCurrency);
  const depositFormatted = formatCurrencyAmount(depositAmount, notificationCurrency);
  const finalPriceNumber = Number.isFinite(Number(finalPriceSnapshotCents))
    ? fromMinorUnits(finalPriceSnapshotCents, notificationCurrency)
    : (booking.finalPrice != null ? Number(booking.finalPrice) : null);
  const finalPriceFormatted = finalPriceNumber != null && Number.isFinite(finalPriceNumber)
    ? formatCurrencyAmount(finalPriceNumber, notificationCurrency)
    : 'No disponible';

  const professionalName = composeDisplayName(booking.professional || {});
  const clientName = composeDisplayName(booking.client || {});
  const serviceTitle = booking.serviceTitle || 'Servicio sin título';
  const bookingIdText = `#${booking.id}`;
  const startText = formatDateTimeEs(booking.start);
  const endText = booking.end ? formatDateTimeEs(booking.end) : null;
  const rangeText = endText && endText !== 'No especificada'
    ? `${startText} · ${endText}`
    : startText;

  const detailsRows = [
    ['Reserva', bookingIdText],
    ['Servicio', serviceTitle],
    ['Profesional', professionalName],
    ['Cliente', clientName],
    ['Fecha', rangeText],
    ['Depósito', depositFormatted],
    ['Precio total', finalPriceFormatted],
  ];

  const preheader = `Se ha cobrado el depósito de ${depositFormatted} para la reserva ${bookingIdText}.`;
  const subject = `Se ha confirmado una reserva ${bookingIdText}`;
  const textLines = [
    'Hola equipo,',
    '',
    `Se ha cobrado correctamente el depósito de ${depositFormatted} para la reserva ${bookingIdText}.`,
    '',
    `Servicio: ${serviceTitle}`,
    `Profesional: ${professionalName}`,
    `Cliente: ${clientName}`,
    `Fecha: ${rangeText}`,
    `Precio total: ${finalPriceFormatted}`,
    '',
    '— Equipo Wisdom'
  ];
  const text = textLines.join('\n');

  const rowsHtml = detailsRows.map(([label, value]) => `
      <tr>
        <td style="padding:6px 0; font-weight:600; color:#111827;">${escapeHtml(label)}</td>
        <td style="padding:6px 0; color:#374151;">${escapeHtml(value)}</td>
      </tr>
    `).join('');

  const html = `<!doctype html>
    <html lang="es">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <meta name="color-scheme" content="light dark">
      <meta name="supported-color-schemes" content="light dark">
      <title>${escapeHtml(subject)}</title>
      <style>
        .content { font-family: Inter, Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:15px; line-height:1.65; }
        @media (max-width:480px) { .content { font-size:16px !important; line-height:1.7 !important; } }
        a { text-decoration: underline; color: inherit; }
        body, table, td, p { margin:0; }
      </style>
    </head>
    <body style="margin:0; padding:0; background:none !important;">
      <div style="display:none; font-size:1px; line-height:1px; max-height:0; max-width:0; opacity:0; overflow:hidden;">
        ${escapeHtml(preheader)}
      </div>

      <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
        <tr>
          <td align="center" style="padding:0 16px;">
            <table role="presentation" width="640" border="0" cellspacing="0" cellpadding="0" style="width:100%; max-width:640px; border-collapse:collapse;">
              <tr>
                <td align="left" style="padding:24px 8px 8px 8px;">
                  <img src="https://storage.googleapis.com/wisdom-images/app_icon.png" width="36" height="36" alt="Wisdom"
                       style="display:block; border:0; outline:none; text-decoration:none; width:36px; height:36px;">
                </td>
              </tr>
              <tr>
                <td class="content" style="padding:8px 8px 24px 8px;">
                  <p style="margin:0 0 16px;">Hola equipo,</p>
                  <p style="margin:0 0 16px;">Se ha cobrado correctamente el depósito de ${escapeHtml(depositFormatted)} para la reserva ${escapeHtml(bookingIdText)}.</p>
                  <table role="presentation" cellspacing="0" cellpadding="0" style="width:100%; max-width:480px; border-collapse:collapse; margin:24px 0;">
                    ${rowsHtml}
                  </table>
                  <p style="margin:0;">— Equipo Wisdom</p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>`;

  return { subject, text, html };
}

function renderBookingLifecycleEmail({ kind, booking }) {
  const normalizedKind = String(kind || '').trim().toLowerCase();
  const bookingIdText = `#${booking?.id || '—'}`;
  const serviceTitle = booking?.serviceTitle || 'Servicio sin título';
  const professionalName = composeDisplayName(booking?.professional || {});
  const clientName = composeDisplayName(booking?.client || {});
  const startText = formatDateTimeEs(booking?.start);
  const eventLabel = normalizedKind === 'started' ? 'ha iniciado el servicio' : 'ha aceptado tu reserva';
  const subject = normalizedKind === 'started'
    ? `Tu servicio ${bookingIdText} ya ha empezado`
    : `Tu reserva ${bookingIdText} ha sido aceptada`;
  const preheader = normalizedKind === 'started'
    ? `El profesional ya ha iniciado el servicio de la reserva ${bookingIdText}.`
    : `El profesional ha aceptado la reserva ${bookingIdText}.`;
  const headline = normalizedKind === 'started'
    ? `El profesional ${eventLabel}.`
    : `El profesional ${eventLabel}.`;

  const detailsRows = [
    ['Reserva', bookingIdText],
    ['Servicio', serviceTitle],
    ['Profesional', professionalName],
    ['Cliente', clientName],
    ['Inicio previsto', startText],
  ];

  const text = [
    `Hola ${clientName},`,
    '',
    `${professionalName} ${eventLabel}.`,
    '',
    `Reserva: ${bookingIdText}`,
    `Servicio: ${serviceTitle}`,
    `Inicio previsto: ${startText}`,
    '',
    normalizedKind === 'started'
      ? 'Puedes abrir la app para seguir la reserva o reportar una incidencia si algo va mal.'
      : 'La reserva queda aceptada y el profesional deberá pulsar "Iniciar servicio" cuando realmente empiece.',
    '',
    '— Equipo Wisdom',
  ].join('\n');

  const rowsHtml = detailsRows.map(([label, value]) => `
      <tr>
        <td style="padding:6px 0; font-weight:600; color:#111827;">${escapeHtml(label)}</td>
        <td style="padding:6px 0; color:#374151;">${escapeHtml(value)}</td>
      </tr>
    `).join('');

  const html = `<!doctype html>
    <html lang="es">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <meta name="color-scheme" content="light dark">
      <meta name="supported-color-schemes" content="light dark">
      <title>${escapeHtml(subject)}</title>
      <style>
        .content { font-family: Inter, Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:15px; line-height:1.65; }
        @media (max-width:480px) { .content { font-size:16px !important; line-height:1.7 !important; } }
        a { text-decoration: underline; color: inherit; }
        body, table, td, p { margin:0; }
      </style>
    </head>
    <body style="margin:0; padding:0; background:none !important;">
      <div style="display:none; font-size:1px; line-height:1px; max-height:0; max-width:0; opacity:0; overflow:hidden;">
        ${escapeHtml(preheader)}
      </div>

      <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
        <tr>
          <td align="center" style="padding:0 16px;">
            <table role="presentation" width="640" border="0" cellspacing="0" cellpadding="0" style="width:100%; max-width:640px; border-collapse:collapse;">
              <tr>
                <td align="left" style="padding:24px 8px 8px 8px;">
                  <img src="https://storage.googleapis.com/wisdom-images/app_icon.png" width="36" height="36" alt="Wisdom"
                       style="display:block; border:0; outline:none; text-decoration:none; width:36px; height:36px;">
                </td>
              </tr>
              <tr>
                <td class="content" style="padding:8px 8px 24px 8px;">
                  <p style="margin:0 0 16px;">Hola ${escapeHtml(clientName)},</p>
                  <p style="margin:0 0 16px;">${escapeHtml(headline)}</p>
                  <table role="presentation" cellspacing="0" cellpadding="0" style="width:100%; max-width:480px; border-collapse:collapse; margin:24px 0;">
                    ${rowsHtml}
                  </table>
                  <p style="margin:0 0 16px;">
                    ${escapeHtml(
                      normalizedKind === 'started'
                        ? 'Puedes abrir la app para seguir la reserva o reportar una incidencia si algo va mal.'
                        : 'La reserva queda aceptada y el profesional deberá pulsar "Iniciar servicio" cuando realmente empiece.'
                    )}
                  </p>
                  <p style="margin:0;">— Equipo Wisdom</p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>`;

  return { subject, text, html };
}

async function sendBookingLifecycleNotificationEmail({ kind, booking }) {
  const recipientEmail = typeof booking?.client?.email === 'string'
    ? booking.client.email.trim()
    : '';
  if (!recipientEmail) {
    return false;
  }

  const { subject, text, html } = renderBookingLifecycleEmail({ kind, booking });
  await sendEmail({
    to: recipientEmail,
    subject,
    text,
    html,
  });
  return true;
}

function getAcceptedBookingInactivityCopy(kind, recipientRole = 'client') {
  const normalizedKind = String(kind || '').trim().toLowerCase();
  const normalizedRecipientRole = String(recipientRole || '').trim().toLowerCase();

  if (normalizedKind === ACCEPTED_BOOKING_INACTIVITY_AUTO_CANCEL_REASON_CODE) {
    return normalizedRecipientRole === 'professional'
      ? {
        subjectPrefix: 'Reserva cancelada automáticamente por inactividad',
        intro: 'Hemos cancelado automáticamente esta reserva porque han pasado 7 días desde la hora de inicio prevista sin ninguna acción registrada.',
        action: 'Además, se ha aplicado un strike automático a tu cuenta por abandono de la reserva.',
      }
      : {
        subjectPrefix: 'Tu reserva se ha cancelado automáticamente',
        intro: 'Hemos cancelado automáticamente esta reserva porque han pasado 7 días desde la hora de inicio prevista sin ninguna acción registrada.',
        action: 'Hemos solicitado el reembolso íntegro del depósito al método de pago original.',
      };
  }

  const reminderLabelByCode = {
    accepted_inactivity_reminder_1h: '1 hora',
    accepted_inactivity_reminder_24h: '24 horas',
    accepted_inactivity_reminder_72h: '72 horas',
  };
  const reminderLabel = reminderLabelByCode[normalizedKind] || 'varias horas';

  return {
    subjectPrefix: 'Recordatorio de reserva pendiente',
    intro: `Esta reserva sigue en estado aceptado ${reminderLabel} después de la hora de inicio prevista y todavía no se ha registrado ninguna acción.`,
    action: 'Si el servicio ya ha empezado, inicia el servicio o reporta la incidencia correspondiente desde la app para evitar una cancelación automática.',
  };
}

function renderAcceptedBookingInactivityEmail({ kind, booking, recipientRole = 'client' }) {
  const recipient = recipientRole === 'professional'
    ? booking?.professional
    : booking?.client;
  const recipientName = composeDisplayName(recipient || {});
  const counterpartName = composeDisplayName(
    recipientRole === 'professional' ? booking?.client || {} : booking?.professional || {}
  );
  const bookingIdText = `#${booking?.id || '—'}`;
  const serviceTitle = booking?.serviceTitle || 'Servicio sin título';
  const startText = formatDateTimeEs(booking?.start);
  const copy = getAcceptedBookingInactivityCopy(kind, recipientRole);
  const subject = `${copy.subjectPrefix} ${bookingIdText}`;
  const preheader = `${copy.subjectPrefix} en ${serviceTitle}.`;
  const detailsRows = [
    ['Reserva', bookingIdText],
    ['Servicio', serviceTitle],
    ['Inicio previsto', startText],
    [recipientRole === 'professional' ? 'Cliente' : 'Profesional', counterpartName],
  ];

  const text = [
    `Hola ${recipientName},`,
    '',
    copy.intro,
    '',
    `Reserva: ${bookingIdText}`,
    `Servicio: ${serviceTitle}`,
    `Inicio previsto: ${startText}`,
    recipientRole === 'professional'
      ? `Cliente: ${counterpartName}`
      : `Profesional: ${counterpartName}`,
    '',
    copy.action,
    '',
    '— Equipo Wisdom',
  ].join('\n');

  const rowsHtml = detailsRows.map(([label, value]) => `
      <tr>
        <td style="padding:6px 0; font-weight:600; color:#111827;">${escapeHtml(label)}</td>
        <td style="padding:6px 0; color:#374151;">${escapeHtml(value)}</td>
      </tr>
    `).join('');

  const html = `<!doctype html>
    <html lang="es">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <meta name="color-scheme" content="light dark">
      <meta name="supported-color-schemes" content="light dark">
      <title>${escapeHtml(subject)}</title>
      <style>
        .content { font-family: Inter, Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:15px; line-height:1.65; }
        @media (max-width:480px) { .content { font-size:16px !important; line-height:1.7 !important; } }
        a { text-decoration: underline; color: inherit; }
        body, table, td, p { margin:0; }
      </style>
    </head>
    <body style="margin:0; padding:0; background:none !important;">
      <div style="display:none; font-size:1px; line-height:1px; max-height:0; max-width:0; opacity:0; overflow:hidden;">
        ${escapeHtml(preheader)}
      </div>
      <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
        <tr>
          <td align="center" style="padding:0 16px;">
            <table role="presentation" width="640" border="0" cellspacing="0" cellpadding="0" style="width:100%; max-width:640px; border-collapse:collapse;">
              <tr>
                <td align="left" style="padding:24px 8px 8px 8px;">
                  <img src="https://storage.googleapis.com/wisdom-images/app_icon.png" width="36" height="36" alt="Wisdom"
                       style="display:block; border:0; outline:none; text-decoration:none; width:36px; height:36px;">
                </td>
              </tr>
              <tr>
                <td class="content" style="padding:8px 8px 24px 8px;">
                  <p style="margin:0 0 16px;">Hola ${escapeHtml(recipientName)},</p>
                  <p style="margin:0 0 16px;">${escapeHtml(copy.intro)}</p>
                  <table role="presentation" cellspacing="0" cellpadding="0" style="width:100%; max-width:480px; border-collapse:collapse; margin:24px 0;">
                    ${rowsHtml}
                  </table>
                  <p style="margin:0 0 16px;">${escapeHtml(copy.action)}</p>
                  <p style="margin:0;">— Equipo Wisdom</p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>`;

  return { subject, text, html };
}

async function sendAcceptedBookingInactivityEmail({ kind, booking, recipientRole = 'client' }) {
  const recipientEmail = recipientRole === 'professional'
    ? booking?.professional?.email
    : booking?.client?.email;
  const normalizedRecipientEmail = typeof recipientEmail === 'string' ? recipientEmail.trim() : '';
  if (!normalizedRecipientEmail) {
    return false;
  }

  const { subject, text, html } = renderAcceptedBookingInactivityEmail({
    kind,
    booking,
    recipientRole,
  });
  await sendEmail({
    to: normalizedRecipientEmail,
    subject,
    text,
    html,
  });
  return true;
}

function formatBookingChangeRequestAddress(addressSnapshot = null) {
  if (!addressSnapshot || typeof addressSnapshot !== 'object') {
    return 'Sin dirección';
  }

  const line1 = [
    addressSnapshot.address_1,
    addressSnapshot.street_number,
  ].filter(Boolean).join(' ');
  const line2 = [
    addressSnapshot.postal_code,
    addressSnapshot.city,
    addressSnapshot.state,
    addressSnapshot.country,
  ].filter(Boolean).join(', ');

  return [line1, addressSnapshot.address_2, line2].filter(Boolean).join(' · ') || 'Sin dirección';
}

function buildBookingChangeRequestSummaryLines(changes = {}) {
  const lines = [];

  if (Object.prototype.hasOwnProperty.call(changes, 'requested_start_datetime')) {
    lines.push(`Inicio solicitado: ${changes.requested_start_datetime || 'Sin fecha ni hora'}`);
  }
  if (Object.prototype.hasOwnProperty.call(changes, 'requested_duration_minutes')) {
    lines.push(
      `Duración solicitada: ${
        changes.requested_duration_minutes === null || changes.requested_duration_minutes === undefined
          ? 'Sin duración definida'
          : `${changes.requested_duration_minutes} minutos`
      }`
    );
  }
  if (Object.prototype.hasOwnProperty.call(changes, 'address_id')) {
    lines.push(`Dirección solicitada: ${formatBookingChangeRequestAddress(changes.address_snapshot)}`);
  }
  if (Object.prototype.hasOwnProperty.call(changes, 'description')) {
    lines.push(`Descripción: ${changes.description || 'Sin descripción'}`);
  }

  return lines;
}

async function getBookingChangeRequestNotificationContext(changeRequestId) {
  const normalizedChangeRequestId = normalizeNullableInteger(changeRequestId);
  if (!normalizedChangeRequestId) {
    return null;
  }

  const connection = await pool.promise().getConnection();
  try {
    const [rows] = await connection.query(
      `
      SELECT
        bcr.id,
        bcr.booking_id,
        bcr.requested_by_user_id,
        bcr.target_user_id,
        bcr.status,
        bcr.changes_json,
        bcr.message,
        bcr.created_at,
        bcr.resolved_at,
        COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
        requester.email AS requester_email,
        requester.first_name AS requester_first_name,
        requester.surname AS requester_surname,
        requester.username AS requester_username,
        target.email AS target_email,
        target.first_name AS target_first_name,
        target.surname AS target_surname,
        target.username AS target_username
      FROM booking_change_request bcr
      INNER JOIN booking b ON b.id = bcr.booking_id
      LEFT JOIN service s ON s.id = b.service_id
      LEFT JOIN user_account requester ON requester.id = bcr.requested_by_user_id
      LEFT JOIN user_account target ON target.id = bcr.target_user_id
      WHERE bcr.id = ?
      LIMIT 1
      `,
      [normalizedChangeRequestId]
    );

    const row = rows[0] || null;
    if (!row) {
      return null;
    }

    return {
      id: row.id,
      bookingId: row.booking_id,
      serviceTitle: row.service_title || 'Servicio sin título',
      status: normalizeBookingChangeRequestStatus(row.status, 'pending'),
      changes: parseJsonObject(row.changes_json) || {},
      message: normalizeNullableText(row.message),
      requester: {
        email: row.requester_email,
        name: composeDisplayName({
          firstName: row.requester_first_name,
          surname: row.requester_surname,
          username: row.requester_username,
          email: row.requester_email,
        }),
      },
      target: {
        email: row.target_email,
        name: composeDisplayName({
          firstName: row.target_first_name,
          surname: row.target_surname,
          username: row.target_username,
          email: row.target_email,
        }),
      },
    };
  } finally {
    connection.release();
  }
}

function renderBookingChangeRequestEmail({ mode, context }) {
  const normalizedMode = String(mode || '').trim().toLowerCase();
  const summaryLines = buildBookingChangeRequestSummaryLines(context?.changes);
  const summaryHtml = summaryLines.length > 0
    ? `<ul>${summaryLines.map((line) => `<li>${escapeHtml(line)}</li>`).join('')}</ul>`
    : '<p>La solicitud no incluye un resumen legible.</p>';
  const summaryText = summaryLines.length > 0
    ? summaryLines.map((line) => `- ${line}`).join('\n')
    : '- Sin resumen disponible';
  const bookingIdText = `#${context?.bookingId || '—'}`;
  const serviceTitle = context?.serviceTitle || 'Servicio sin título';
  const requesterName = context?.requester?.name || 'La otra parte';

  if (normalizedMode === 'created') {
    return {
      subject: `Nueva solicitud de modificación para la reserva ${bookingIdText}`,
      text: [
        `Hola ${context?.target?.name || 'usuario'},`,
        '',
        `${requesterName} ha enviado una solicitud de modificación para la reserva ${bookingIdText}.`,
        `Servicio: ${serviceTitle}`,
        '',
        summaryText,
        '',
        'Abre Wisdom para aceptarla o rechazarla.',
        '',
        '— Equipo Wisdom',
      ].join('\n'),
      html: `<p>Hola ${escapeHtml(context?.target?.name || 'usuario')},</p>
        <p><strong>${escapeHtml(requesterName)}</strong> ha enviado una solicitud de modificación para la reserva <strong>${escapeHtml(bookingIdText)}</strong>.</p>
        <p><strong>Servicio:</strong> ${escapeHtml(serviceTitle)}</p>
        ${summaryHtml}
        <p>Abre Wisdom para aceptarla o rechazarla.</p>
        <p>— Equipo Wisdom</p>`,
    };
  }

  const resultLabels = {
    accepted: 'aceptada',
    rejected: 'rechazada',
    canceled: 'cancelada',
    expired: 'caducada',
  };
  const resolvedLabel = resultLabels[normalizedMode] || 'actualizada';

  return {
    subject: `Tu solicitud de modificación ${bookingIdText} ha sido ${resolvedLabel}`,
    text: [
      `Hola ${context?.requester?.name || 'usuario'},`,
      '',
      `Tu solicitud de modificación para la reserva ${bookingIdText} ha sido ${resolvedLabel}.`,
      `Servicio: ${serviceTitle}`,
      '',
      summaryText,
      '',
      normalizedMode === 'accepted'
        ? 'Los cambios ya se han aplicado a la reserva.'
        : 'Puedes revisar la reserva en la app para ver su estado actual.',
      '',
      '— Equipo Wisdom',
    ].join('\n'),
    html: `<p>Hola ${escapeHtml(context?.requester?.name || 'usuario')},</p>
      <p>Tu solicitud de modificación para la reserva <strong>${escapeHtml(bookingIdText)}</strong> ha sido <strong>${escapeHtml(resolvedLabel)}</strong>.</p>
      <p><strong>Servicio:</strong> ${escapeHtml(serviceTitle)}</p>
      ${summaryHtml}
      <p>${escapeHtml(
        normalizedMode === 'accepted'
          ? 'Los cambios ya se han aplicado a la reserva.'
          : 'Puedes revisar la reserva en la app para ver su estado actual.'
      )}</p>
      <p>— Equipo Wisdom</p>`,
  };
}

async function sendBookingChangeRequestNotificationEmail({ changeRequestId, mode }) {
  const context = await getBookingChangeRequestNotificationContext(changeRequestId);
  if (!context) {
    return false;
  }

  const normalizedMode = String(mode || '').trim().toLowerCase();
  const recipientEmail = normalizedMode === 'created'
    ? context.target?.email
    : context.requester?.email;
  if (!recipientEmail) {
    return false;
  }

  const { subject, text, html } = renderBookingChangeRequestEmail({ mode: normalizedMode, context });
  await sendEmail({
    to: recipientEmail,
    subject,
    text,
    html,
  });
  return true;
}

async function getBookingSupportAlertContext(bookingId) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return null;
  }

  const connection = await pool.promise().getConnection();
  try {
    const [rows] = await connection.query(
      `
      SELECT
        b.id,
        b.service_status,
        b.settlement_status,
        COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
        client.email AS client_email,
        client.first_name AS client_first_name,
        client.surname AS client_surname,
        client.username AS client_username,
        provider.email AS provider_email,
        provider.first_name AS provider_first_name,
        provider.surname AS provider_surname,
        provider.username AS provider_username
      FROM booking b
      LEFT JOIN service s ON s.id = b.service_id
      LEFT JOIN user_account client ON client.id = b.client_user_id
      LEFT JOIN user_account provider ON provider.id = b.provider_user_id_snapshot
      WHERE b.id = ?
      LIMIT 1
      `,
      [normalizedBookingId]
    );

    const row = rows[0] || null;
    if (!row) {
      return null;
    }

    return {
      id: row.id,
      serviceStatus: normalizeServiceStatus(row.service_status, 'pending_deposit'),
      settlementStatus: normalizeSettlementStatus(row.settlement_status, 'none'),
      serviceTitle: row.service_title || 'Servicio sin título',
      clientName: composeDisplayName({
        firstName: row.client_first_name,
        surname: row.client_surname,
        username: row.client_username,
        email: row.client_email,
      }),
      providerName: composeDisplayName({
        firstName: row.provider_first_name,
        surname: row.provider_surname,
        username: row.provider_username,
        email: row.provider_email,
      }),
    };
  } finally {
    connection.release();
  }
}

async function sendBookingSupportAlertEmail({
  bookingId,
  headline,
  details = null,
  category = 'booking_support_case',
}) {
  if (!bookingSupportEmail) {
    return false;
  }

  const context = await getBookingSupportAlertContext(bookingId);
  if (!context) {
    return false;
  }

  const bookingIdText = `#${context.id}`;
  const subject = `[Wisdom][Booking] ${headline} ${bookingIdText}`;
  const detailText = normalizeNullableText(details) || 'Sin detalles adicionales.';
  const text = [
    `Caso de soporte para la reserva ${bookingIdText}.`,
    `Motivo: ${headline}`,
    `Categoría: ${category}`,
    `Servicio: ${context.serviceTitle}`,
    `Cliente: ${context.clientName}`,
    `Profesional: ${context.providerName}`,
    `Estado servicio: ${context.serviceStatus}`,
    `Estado liquidación: ${context.settlementStatus}`,
    `Detalles: ${detailText}`,
  ].join('\n');
  const html = `<p><strong>Caso de soporte para la reserva ${escapeHtml(bookingIdText)}</strong></p>
    <p><strong>Motivo:</strong> ${escapeHtml(headline)}<br/>
    <strong>Categoría:</strong> ${escapeHtml(category)}<br/>
    <strong>Servicio:</strong> ${escapeHtml(context.serviceTitle)}<br/>
    <strong>Cliente:</strong> ${escapeHtml(context.clientName)}<br/>
    <strong>Profesional:</strong> ${escapeHtml(context.providerName)}<br/>
    <strong>Estado servicio:</strong> ${escapeHtml(context.serviceStatus)}<br/>
    <strong>Estado liquidación:</strong> ${escapeHtml(context.settlementStatus)}<br/>
    <strong>Detalles:</strong> ${escapeHtml(detailText)}</p>`;

  await sendEmail({
    to: bookingSupportEmail,
    subject,
    text,
    html,
  });
  return true;
}

async function getClosureAutoChargeEmailContext(bookingId) {
  const normalizedBookingId = normalizeNullableInteger(bookingId);
  if (!normalizedBookingId) {
    return null;
  }

  const connection = await pool.promise().getConnection();
  try {
    const [rows] = await connection.query(
      `
      SELECT
        b.id,
        b.service_currency_snapshot,
        b.estimated_total_amount_cents,
        COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
        client.email AS client_email,
        client.first_name AS client_first_name,
        client.surname AS client_surname,
        cp.proposed_total_amount_cents,
        cp.amount_due_from_client_cents,
        cp.amount_to_refund_cents
      FROM booking b
      LEFT JOIN service s ON s.id = b.service_id
      LEFT JOIN user_account client ON client.id = b.client_user_id
      LEFT JOIN booking_closure_proposal cp ON cp.id = (
        SELECT cp2.id
        FROM booking_closure_proposal cp2
        WHERE cp2.booking_id = b.id
        ORDER BY cp2.id DESC
        LIMIT 1
      )
      WHERE b.id = ?
      LIMIT 1
      `,
      [normalizedBookingId]
    );
    const row = rows[0] || null;
    if (!row) {
      return null;
    }

    const currency = normalizeCurrencyCode(row.service_currency_snapshot, 'EUR');
    return {
      id: row.id,
      serviceTitle: row.service_title || 'Servicio sin título',
      clientEmail: row.client_email,
      clientName: composeDisplayName({
        firstName: row.client_first_name,
        surname: row.client_surname,
      }),
      currency,
      estimatedTotal: row.estimated_total_amount_cents != null
        ? formatCurrencyAmount(fromMinorUnits(row.estimated_total_amount_cents, currency), currency)
        : null,
      proposedTotal: row.proposed_total_amount_cents != null
        ? formatCurrencyAmount(fromMinorUnits(row.proposed_total_amount_cents, currency), currency)
        : null,
      amountDue: row.amount_due_from_client_cents != null
        ? formatCurrencyAmount(fromMinorUnits(row.amount_due_from_client_cents, currency), currency)
        : null,
      amountToRefund: row.amount_to_refund_cents != null
        ? formatCurrencyAmount(fromMinorUnits(row.amount_to_refund_cents, currency), currency)
        : null,
    };
  } finally {
    connection.release();
  }
}

function renderClosureAutoChargeEmail({ mode, booking }) {
  const normalizedMode = String(mode || '').trim().toLowerCase();
  const bookingIdText = `#${booking?.id || '—'}`;
  const clientName = booking?.clientName || 'cliente';
  const totalText = booking?.proposedTotal || 'No disponible';
  const estimatedText = booking?.estimatedTotal || 'No disponible';
  const amountDueText = booking?.amountDue || '0,00 €';
  const refundText = booking?.amountToRefund || '0,00 €';

  if (normalizedMode === 'within_tolerance') {
    const subject = `Actualización automática del importe final de tu reserva ${bookingIdText}`;
    const text = [
      `Hola ${clientName},`,
      '',
      `El profesional ha cerrado la reserva ${bookingIdText} con un pequeño ajuste dentro del margen permitido.`,
      `Servicio: ${booking?.serviceTitle || 'Servicio sin título'}`,
      `Importe estimado: ${estimatedText}`,
      `Importe final: ${totalText}`,
      `Pendiente por cobrar automáticamente: ${amountDueText}`,
      `Reembolso previsto: ${refundText}`,
      '',
      'Si no haces nada, Wisdom intentará cobrar o cerrar la reserva automáticamente al cumplirse las 48 horas.',
      '',
      '— Equipo Wisdom',
    ].join('\n');
    const html = `<p>Hola ${escapeHtml(clientName)},</p>
      <p>El profesional ha cerrado la reserva <strong>${escapeHtml(bookingIdText)}</strong> con un pequeño ajuste dentro del margen permitido.</p>
      <p><strong>Servicio:</strong> ${escapeHtml(booking?.serviceTitle || 'Servicio sin título')}<br/>
      <strong>Importe estimado:</strong> ${escapeHtml(estimatedText)}<br/>
      <strong>Importe final:</strong> ${escapeHtml(totalText)}<br/>
      <strong>Pendiente por cobrar automáticamente:</strong> ${escapeHtml(amountDueText)}<br/>
      <strong>Reembolso previsto:</strong> ${escapeHtml(refundText)}</p>
      <p>Si no haces nada, Wisdom intentará cerrar la reserva automáticamente al cumplirse las 48 horas.</p>
      <p>— Equipo Wisdom</p>`;
    return { subject, text, html };
  }

  if (normalizedMode === 'approval_follow_up') {
    const subject = `Necesitamos tu respuesta para cerrar la reserva ${bookingIdText}`;
    const text = [
      `Hola ${clientName},`,
      '',
      `La reserva ${bookingIdText} no se ha podido cerrar automáticamente todavía.`,
      `Servicio: ${booking?.serviceTitle || 'Servicio sin título'}`,
      `Importe estimado: ${estimatedText}`,
      `Importe final propuesto: ${totalText}`,
      `Pendiente por pagar: ${amountDueText}`,
      `Reembolso previsto: ${refundText}`,
      '',
      'Tienes 72 horas adicionales para aceptar el cierre o abrir una disputa desde la app.',
      'Si no haces nada dentro de ese plazo, Wisdom enviará la reserva automáticamente a revisión.',
      '',
      '— Equipo Wisdom',
    ].join('\n');
    const html = `<p>Hola ${escapeHtml(clientName)},</p>
      <p>La reserva <strong>${escapeHtml(bookingIdText)}</strong> no se ha podido cerrar automáticamente todavía.</p>
      <p><strong>Servicio:</strong> ${escapeHtml(booking?.serviceTitle || 'Servicio sin título')}<br/>
      <strong>Importe estimado:</strong> ${escapeHtml(estimatedText)}<br/>
      <strong>Importe final propuesto:</strong> ${escapeHtml(totalText)}<br/>
      <strong>Pendiente por pagar:</strong> ${escapeHtml(amountDueText)}<br/>
      <strong>Reembolso previsto:</strong> ${escapeHtml(refundText)}</p>
      <p>Tienes <strong>72 horas adicionales</strong> para aceptar el cierre o abrir una disputa desde la app.</p>
      <p>Si no haces nada dentro de ese plazo, Wisdom enviará la reserva automáticamente a revisión.</p>
      <p>— Equipo Wisdom</p>`;
    return { subject, text, html };
  }

  const subject = `Tu reserva ${bookingIdText} requiere aprobación manual`;
  const text = [
    `Hola ${clientName},`,
    '',
    `La reserva ${bookingIdText} necesita una revisión manual antes de poder cerrarse.`,
    `Servicio: ${booking?.serviceTitle || 'Servicio sin título'}`,
    `Importe estimado: ${estimatedText}`,
    `Importe final propuesto: ${totalText}`,
    '',
    'Wisdom ha bloqueado el cierre automático y el equipo de soporte revisará el caso.',
    '',
    '— Equipo Wisdom',
  ].join('\n');
  const html = `<p>Hola ${escapeHtml(clientName)},</p>
    <p>La reserva <strong>${escapeHtml(bookingIdText)}</strong> necesita una revisión manual antes de poder cerrarse.</p>
    <p><strong>Servicio:</strong> ${escapeHtml(booking?.serviceTitle || 'Servicio sin título')}<br/>
    <strong>Importe estimado:</strong> ${escapeHtml(estimatedText)}<br/>
    <strong>Importe final propuesto:</strong> ${escapeHtml(totalText)}</p>
    <p>Wisdom ha bloqueado el cierre automático y el equipo de soporte revisará el caso.</p>
    <p>— Equipo Wisdom</p>`;
  return { subject, text, html };
}

async function sendClosureAutoChargeNotificationEmail({ bookingId, mode }) {
  const booking = await getClosureAutoChargeEmailContext(bookingId);
  if (!booking?.clientEmail) {
    return false;
  }

  const { subject, text, html } = renderClosureAutoChargeEmail({ mode, booking });
  await sendEmail({
    to: booking.clientEmail,
    subject,
    text,
    html,
  });
  return true;
}
// Envía el correo de actualización de términos a todos los usuarios
async function sendEmailToAll(pool, transporter, options = {}) {
  const {
    termsUrl,
    privacyUrl,
    effectiveDate,
    productName,
    dryRun = false,
    testEmail = null,
    limit = null
  } = options;

  const { subject, text, html } = renderEmail({ termsUrl, privacyUrl, effectiveDate, productName });

  let sql = 'SELECT email FROM user_account WHERE email IS NOT NULL';
  const params = [];
  const numericLimit = Number(limit);
  if (Number.isFinite(numericLimit) && numericLimit > 0) {
    sql += ' LIMIT ?';
    params.push(Math.floor(numericLimit));
  }

  const [rows] = await pool.promise().query(sql, params);

  const seen = new Set();
  const recipients = [];
  for (const row of rows) {
    const raw = typeof row.email === 'string' ? row.email.trim() : '';
    if (!raw) continue;
    const key = raw.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    recipients.push(raw);
  }

  const targetRecipients = testEmail ? [testEmail] : recipients;
  const summary = {
    totalRecipients: recipients.length,
    targetedRecipients: targetRecipients.length,
    dryRun: Boolean(dryRun),
    sent: 0,
    failed: 0,
    errors: []
  };

  if (dryRun) {
    summary.previewRecipients = targetRecipients.slice(0, 10);
    if (summary.errors.length === 0) {
      delete summary.errors;
    }
    return summary;
  }

  for (const email of targetRecipients) {
    try {
      await sendEmail({
        from: '"Wisdom" <wisdom.helpcontact@gmail.com>',
        to: email,
        subject,
        text,
        html,
        headers: { 'X-Entity-Ref-ID': 'policy-update-2025-09-07' }
      });
      summary.sent += 1;
      await new Promise(r => setTimeout(r, 150));
    } catch (e) {
      summary.failed += 1;
      summary.errors.push({ email, message: e.message });
      console.error('Error enviando a', email, e);
    }
  }

  if (summary.errors.length === 0) {
    delete summary.errors;
  }

  return summary;
}
//--------------------------------------------

// Configuración del pool de conexiones a la base de datos a // JSON.parse(process.env.GOOGLE_CREDENTIALS)..
const pool = mysql.createPool({
  host: process.env.DB_HOST, //process.env.HOST process.env.USER process.env.PASSWORD process.env.DATABASE
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT,
  timezone: 'Z',
  //socketPath: `/cloudsql/${process.env.INSTANCE_CONNECTION_NAME}`,
  waitForConnections: true,
  connectionLimit: 20,  // Número máximo de conexiones en el pool
  acquireTimeout: 20000,  // Tiempo máximo para adquirir una conexión
  connectTimeout: 20000,     // Tiempo máximo que una conexión puede estar inactiva antes de ser liberada.
});


pool.on('connection', (connection) => {
  connection.query("SET time_zone = '+00:00'", (error) => {
    if (error) {
      console.error('Error setting MySQL session time zone to UTC:', error);
    }
  });
});

const promisePool = pool.promise();
refreshExchangeRates({ force: true }).catch((error) => {
  console.error('Initial exchange-rates refresh failed:', error?.message || error);
});
const exchangeRatesRefreshInterval = setInterval(() => {
  refreshExchangeRates({ force: true }).catch((error) => {
    console.error('Scheduled exchange-rates refresh failed:', error?.message || error);
  });
}, EXCHANGE_RATES_CACHE_TTL_MS);
exchangeRatesRefreshInterval.unref?.();

const GOOGLE_WEB_CLIENT_ID = process.env.GOOGLE_WEB_CLIENT_ID || '232292898356-u0584r99cq2hckjlj6i0q2v4gchmuqsg.apps.googleusercontent.com';
const googleAuthClient = new OAuth2Client(GOOGLE_WEB_CLIENT_ID);

function escapeRegex(value) {
  return String(value).replace(/[.*+?^$()|[\]\\]/g, '\\$&');
}

function normalizeGeneratedUsernamePart(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildGeneratedUsername(firstName, surname) {
  const base = [firstName, surname]
    .map(normalizeGeneratedUsernamePart)
    .filter(Boolean)
    .join(' ')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

  return base || 'user';
}

async function generateUniqueUsername(connection, firstName, surname) {
  const base = buildGeneratedUsername(firstName, surname);
  const usernamePattern = '^' + escapeRegex(base) + '[0-9]+$';

  const [rows] = await connection.query(
    'SELECT username FROM user_account WHERE username = ? OR username REGEXP ?',
    [base, usernamePattern]
  );

  const usedUsernames = new Set(rows.map((row) => String(row.username || '')));
  if (!usedUsernames.has(base)) {
    return base;
  }

  let suffix = 2;
  while (usedUsernames.has(base + suffix)) {
    suffix += 1;
  }

  return base + suffix;
}
function conflictInputError(code) {
  const err = new Error(code);
  err.status = 409;
  return err;
}

function invalidInputError(code) {
  const err = new Error(code);
  err.status = 400;
  return err;
}

function sanitizeUserRecord(user) {
  if (!user) return null;
  const sanitizedUser = { ...user };
  delete sanitizedUser.password;
  return sanitizedUser;
}

function buildPublicUserRecord(user) {
  if (!user) return null;
  return {
    id: user.id,
    username: user.username,
    first_name: user.first_name,
    surname: user.surname,
    profile_picture: user.profile_picture,
    is_professional: user.is_professional,
    language: user.language,
    joined_datetime: user.joined_datetime,
  };
}

function parseRequestedUserId(rawUserId) {
  const userId = parseInt(rawUserId, 10);
  return Number.isInteger(userId) ? userId : null;
}

function ensureSameUserOrRespond(req, res, rawUserId = req.params?.id) {
  const requestedUserId = parseRequestedUserId(rawUserId);
  if (!requestedUserId) {
    res.status(400).json({ error: 'invalid_user_id' });
    return null;
  }

  if (!req.user || Number(req.user.id) !== requestedUserId) {
    res.status(403).json({ error: 'Acceso denegado' });
    return null;
  }

  return requestedUserId;
}

function isPasswordTooShort(password) {
  return typeof password !== 'string' || password.length < 8;
}

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function normalizeEmail(value) {
  return typeof value === 'string' ? value.trim().toLowerCase() : '';
}

function normalizeCountryCode(value) {
  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim().toUpperCase();
  return /^[A-Z]{2}$/.test(normalized) ? normalized : null;
}

function normalizeCurrencyCode(value, fallback = null) {
  if (typeof value !== 'string') {
    return fallback;
  }

  const normalized = value.trim().toUpperCase();
  const aliased = CURRENCY_ALIASES[normalized] || normalized;
  return /^[A-Z]{3}$/.test(aliased) ? aliased : fallback;
}

async function resolveUserCurrency(userId, fallback = 'EUR', connection = promisePool) {
  const numericUserId = Number(userId);
  if (!Number.isFinite(numericUserId) || numericUserId <= 0) {
    return normalizeCurrencyCode(fallback, 'EUR');
  }

  try {
    const [rows] = await connection.query(
      'SELECT currency FROM user_account WHERE id = ? LIMIT 1',
      [numericUserId]
    );
    return normalizeCurrencyCode(rows?.[0]?.currency, fallback);
  } catch (error) {
    console.error('Error resolving user currency:', error);
    return normalizeCurrencyCode(fallback, 'EUR');
  }
}

function isValidEmail(email) {
  return EMAIL_REGEX.test(normalizeEmail(email));
}

function resolveGoogleNameParts(payload) {
  let first_name = typeof payload?.given_name === 'string' ? payload.given_name.trim() : '';
  let surname = typeof payload?.family_name === 'string' ? payload.family_name.trim() : '';
  const displayName = typeof payload?.name === 'string' ? payload.name.trim() : '';

  if (!first_name || !surname) {
    const parts = displayName.split(/\s+/).filter(Boolean);
    if (!first_name && parts.length > 0) {
      first_name = parts[0];
    }
    if (!surname && parts.length > 1) {
      surname = parts.slice(1).join(' ');
    }
  }

  if (!first_name) {
    const emailLocalPart = typeof payload?.email === 'string' ? payload.email.split('@')[0].trim() : '';
    first_name = emailLocalPart || 'Google';
  }

  if (!surname) {
    surname = first_name;
  }

  return { first_name, surname };
}

function resolveAppleNameParts({ first_name, surname, email }) {
  let resolvedFirstName = typeof first_name === 'string' ? first_name.trim() : '';
  let resolvedSurname = typeof surname === 'string' ? surname.trim() : '';

  if (!resolvedFirstName) {
    const emailLocalPart = typeof email === 'string' ? email.split('@')[0].trim() : '';
    resolvedFirstName = emailLocalPart || 'Apple';
  }

  if (!resolvedSurname) {
    resolvedSurname = resolvedFirstName;
  }

  return {
    first_name: resolvedFirstName,
    surname: resolvedSurname,
  };
}

function getAppleClientSecret() {
  if (!APPLE_TEAM_ID || !APPLE_KEY_ID || (!APPLE_PRIVATE_KEY && !APPLE_PRIVATE_KEY_PATH)) {
    const err = new Error('APPLE_REVOKE_CONFIG_MISSING');
    err.status = 500;
    throw err;
  }

  return appleSigninAuth.getClientSecret({
    clientID: APPLE_CLIENT_ID,
    teamID: APPLE_TEAM_ID,
    keyIdentifier: APPLE_KEY_ID,
    ...(APPLE_PRIVATE_KEY_PATH ? { privateKeyPath: APPLE_PRIVATE_KEY_PATH } : { privateKey: APPLE_PRIVATE_KEY }),
    expAfter: 15777000,
  });
}

async function exchangeAppleAuthorizationCode(authorizationCode) {
  const clientSecret = getAppleClientSecret();
  const tokenResponse = await appleSigninAuth.getAuthorizationToken(authorizationCode, {
    clientID: APPLE_CLIENT_ID,
    clientSecret,
  });

  if (tokenResponse?.error) {
    const err = new Error(tokenResponse.error);
    err.status = 400;
    err.code = 'APPLE_TOKEN_EXCHANGE_FAILED';
    err.details = tokenResponse;
    throw err;
  }

  return { clientSecret, tokenResponse };
}

async function revokeAppleSessionWithAuthorizationCode(authorizationCode) {
  const { clientSecret, tokenResponse } = await exchangeAppleAuthorizationCode(authorizationCode);
  const tokenToRevoke = tokenResponse?.refresh_token || tokenResponse?.access_token || '';
  const tokenTypeHint = tokenResponse?.refresh_token ? 'refresh_token' : 'access_token';

  if (!tokenToRevoke) {
    const err = new Error('APPLE_REVOKE_TOKEN_MISSING');
    err.status = 400;
    throw err;
  }

  const revokeResponse = await appleSigninAuth.revokeAuthorizationToken(tokenToRevoke, {
    clientID: APPLE_CLIENT_ID,
    clientSecret,
    tokenTypeHint,
  });

  if (revokeResponse?.error) {
    const err = new Error(revokeResponse.error);
    err.status = 400;
    err.code = 'APPLE_TOKEN_REVOKE_FAILED';
    err.details = revokeResponse;
    throw err;
  }
}

async function createUserAccountWithDefaults(connection, {
  email,
  password = null,
  first_name,
  surname,
  phone = null,
  language = 'en',
  country = null,
  currency = 'EUR',
  allow_notis = null,
  profile_picture = null,
  is_verified = 0,
  is_professional = 0,
  auth_provider = 'email',
  provider_id = null,
  platform = 'ios',
}) {
  let userId;
  let username;
  let inserted = false;
  const normalizedCountry = normalizeCountryCode(country);
  const normalizedCurrency = normalizeCurrencyCode(currency, 'EUR');

  for (let attempt = 0; attempt < 5; attempt += 1) {
    username = await generateUniqueUsername(connection, first_name, surname);

    try {
      const [result] = await connection.query(
        'INSERT INTO user_account (email, username, password, first_name, surname, phone, joined_datetime, language, country, allow_notis, currency, profile_picture, is_verified, is_professional, auth_provider, provider_id, platform) VALUES (?, ?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [email, username, password, first_name, surname, phone, language, normalizedCountry, allow_notis, normalizedCurrency, profile_picture, is_verified, is_professional, auth_provider, provider_id, platform]
      );

      userId = result.insertId;
      inserted = true;
      break;
    } catch (insertErr) {
      if (insertErr.code === 'ER_DUP_ENTRY') {
        const duplicateMessage = String(insertErr.sqlMessage || '');
        if (duplicateMessage.includes('email')) {
          throw conflictInputError('EMAIL_EXISTS');
        }
        if (duplicateMessage.includes('provider_id')) {
          throw conflictInputError('PROVIDER_ID_EXISTS');
        }
        if (duplicateMessage.includes('username')) {
          continue;
        }
      }
      throw insertErr;
    }
  }

  if (!inserted) {
    throw new Error('USERNAME_GENERATION_FAILED');
  }

  await connection.query(
    'INSERT INTO service_list (list_name, user_id) VALUES (?, ?)',
    ['Recently seen', userId]
  );

  return { userId, username };
}

async function fetchSanitizedUserById(userId) {
  const [rows] = await promisePool.query(
    'SELECT * FROM user_account WHERE id = ? LIMIT 1',
    [userId]
  );

  return sanitizeUserRecord(rows[0] || null);
}

async function issueAuthTokens(userId, req) {
  const access_token = signAccessToken({ id: userId });
  const refresh_token = generateRefreshToken();
  await persistRefreshToken(userId, refresh_token, req);

  return {
    token: access_token,
    access_token,
    refresh_token,
  };
}

function parseBooleanInput(value, defaultValue, fieldName) {
  if (value === undefined) return defaultValue;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (value === 1 || value === 0) return Boolean(value);
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1') return true;
    if (normalized === 'false' || normalized === '0') return false;
  }
  throw invalidInputError(`invalid_boolean_${fieldName}`);
}

function parseNumberInput(value, defaultValue, fieldName, { allowNull = true, integer = false } = {}) {
  if (value === undefined) return defaultValue;
  if (value === null || value === '') {
    if (!allowNull) {
      throw invalidInputError(`invalid_number_${fieldName}`);
    }
    return null;
  }
  const num = Number(value);
  if (!Number.isFinite(num)) {
    throw invalidInputError(`invalid_number_${fieldName}`);
  }
  if (integer && !Number.isInteger(num)) {
    throw invalidInputError(`invalid_integer_${fieldName}`);
  }
  return num;
}

function parseIntegerInput(value, defaultValue, fieldName) {
  return parseNumberInput(value, defaultValue, fieldName, { allowNull: false, integer: true });
}

function parseExperienceYearsInput(value, defaultValue) {
  const parsedValue = parseIntegerInput(value, defaultValue, 'experience_years');
  if (![0, 1, 3, 5, 10].includes(parsedValue)) {
    throw invalidInputError('invalid_experience_years');
  }
  return parsedValue;
}

function parseMinimumNoticePolicyInput(value, defaultValue = 1440) {
  const parsedValue = parseIntegerInput(
    value,
    defaultValue === null || defaultValue === undefined ? 1440 : defaultValue,
    'minimum_notice_policy'
  );

  if (![0, 120, 1440, 2880, 10080].includes(parsedValue)) {
    throw invalidInputError('invalid_minimum_notice_policy');
  }

  return parsedValue;
}

function toMySQLDatetime(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

function mapOwnershipError(code) {
  switch (code) {
    case 'not_found':
      return { status: 404, body: { error: 'service_not_found' } };
    case 'not_owner':
      return { status: 403, body: { error: 'service_not_owned' } };
    case 'not_professional':
      return { status: 403, body: { error: 'service_owner_not_professional' } };
    default:
      return { status: 500, body: { error: 'service_ownership_error' } };
  }
}

async function fetchOwnedService(connection, serviceId, userId, { lock = false, includePrice = false, includeConsult = false } = {}) {
  const selectParts = ['s.*', 'ua.is_professional', 'ua.is_verified'];
  const joins = ['JOIN user_account ua ON s.user_id = ua.id'];

  if (includePrice) {
    selectParts.push('p.price AS current_price', 'p.price_type AS current_price_type', 'p.currency AS current_price_currency');
    joins.push('JOIN price p ON s.price_id = p.id');
  }

  if (includeConsult) {
    selectParts.push('cv.provider AS consult_provider', 'cv.username AS consult_username', 'cv.url AS consult_url');
    joins.push('LEFT JOIN consult_via cv ON s.consult_via_id = cv.id');
  }

  const lockClause = lock ? 'FOR UPDATE' : '';
  const [rows] = await connection.query(
    `SELECT ${selectParts.join(', ')} FROM service s ${joins.join(' ')} WHERE s.id = ? ${lockClause}`,
    [serviceId]
  );

  if (rows.length === 0) {
    return { error: 'not_found' };
  }

  const service = rows[0];
  if (Number(service.user_id) !== Number(userId)) {
    return { error: 'not_owner' };
  }

  if (!service.is_professional) {
    return { error: 'not_professional' };
  }

  return { service };
}

async function releasePendingProviderPayouts({
  now = new Date(),
  limit = 500,
} = {}) {
  const normalizedNow = parseDateTimeInput(now) || new Date();
  const connection = await pool.promise().getConnection();

  try {
    await connection.beginTransaction();

    const [rows] = await connection.query(
      `
      SELECT
        p.id AS payment_id,
        p.booking_id,
        p.currency,
        p.provider_payout_amount_cents,
        p.provider_payout_status,
        p.provider_payout_eligible_at,
        COALESCE(b.provider_user_id_snapshot, s.user_id) AS provider_user_id,
        provider.stripe_account_id
      FROM payments p
      INNER JOIN booking b ON b.id = p.booking_id
      LEFT JOIN service s ON s.id = b.service_id
      LEFT JOIN user_account provider ON provider.id = COALESCE(b.provider_user_id_snapshot, s.user_id)
      WHERE p.provider_payout_status = 'pending_release'
        AND p.provider_payout_amount_cents IS NOT NULL
        AND p.provider_payout_amount_cents > 0
        AND p.provider_payout_eligible_at IS NOT NULL
        AND p.provider_payout_eligible_at <= ?
      ORDER BY provider_user_id ASC, p.provider_payout_eligible_at ASC, p.id ASC
      LIMIT ?
      FOR UPDATE
      `,
      [toDbDateTime(normalizedNow), Math.max(1, Number(limit) || 500)]
    );

    if (!rows || rows.length === 0) {
      await connection.rollback();
      return { releasedPayments: 0, releasedAmountCents: 0, skippedPayments: 0 };
    }

    let releasedPayments = 0;
    let releasedAmountCents = 0;
    let skippedPayments = 0;

    const groupedRows = new Map();
    for (const row of rows) {
      if (!row?.stripe_account_id || !String(row.stripe_account_id).startsWith('acct_')) {
        skippedPayments += 1;
        continue;
      }

      const groupKey = [
        row.provider_user_id || 'unknown',
        normalizeCurrencyCode(row.currency, 'EUR'),
        row.stripe_account_id,
      ].join(':');

      if (!groupedRows.has(groupKey)) {
        groupedRows.set(groupKey, {
          providerUserId: row.provider_user_id,
          destinationAccountId: row.stripe_account_id,
          currency: normalizeCurrencyCode(row.currency, 'EUR'),
          payments: [],
        });
      }

      groupedRows.get(groupKey).payments.push(row);
    }
    for (const [, group] of groupedRows) {
      if (!group.payments.length) {
        continue;
      }

      const paymentIds = group.payments.map((payment) => payment.payment_id);
      const totalAmountCents = group.payments.reduce(
        (sum, payment) => sum + Math.max(0, Math.round(Number(payment.provider_payout_amount_cents || 0))),
        0
      );

      if (totalAmountCents <= 0) {
        skippedPayments += group.payments.length;
        continue;
      }

      try {
        await assertProviderTransferReady(group.destinationAccountId);
        const transfer = await stripe.transfers.create(
          {
            amount: totalAmountCents,
            currency: toStripeCurrencyCode(group.currency),
            destination: group.destinationAccountId,
            transfer_group: `provider-payout-${group.providerUserId}-${normalizedNow.getTime()}`,
            metadata: {
              source: 'provider_payout_release',
              provider_user_id: String(group.providerUserId || ''),
              payment_count: String(group.payments.length),
            },
          },
          {
            idempotencyKey: stableKey([
              'provider_payout_release',
              group.providerUserId,
              group.currency,
              ...paymentIds,
            ]),
          }
        );

        const placeholders = paymentIds.map(() => '?').join(', ');
        await connection.query(
          `
          UPDATE payments
          SET provider_payout_status = 'released',
              provider_payout_released_at = ?,
              provider_payout_transfer_id = ?
          WHERE id IN (${placeholders})
          `,
          [
            toDbDateTime(normalizedNow),
            transfer.id,
            ...paymentIds,
          ]
        );

        releasedPayments += group.payments.length;
        releasedAmountCents += totalAmountCents;
      } catch (error) {
        skippedPayments += group.payments.length;
        console.error('Error releasing provider payout batch:', {
          providerUserId: group.providerUserId,
          destinationAccountId: group.destinationAccountId,
          paymentIds,
          error: error.message,
        });
      }
    }

    await connection.commit();
    return {
      releasedPayments,
      releasedAmountCents,
      skippedPayments,
    };
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error releasing pending provider payouts:', error);
    return {
      releasedPayments: 0,
      releasedAmountCents: 0,
      skippedPayments: 0,
      error,
    };
  } finally {
    connection.release();
  }
}

const cron = require('node-cron');
const ACCESS_TOKEN_TTL = process.env.ACCESS_TOKEN_TTL || '15m';
const REFRESH_TOKEN_TTL_DAYS = parseInt(process.env.REFRESH_TOKEN_TTL_DAYS || '30', 10);
const GUEST_ACCESS_TOKEN_TTL_MINUTES = (() => {
  const value = Number(process.env.GUEST_ACCESS_TOKEN_TTL_MINUTES || '90');
  if (Number.isFinite(value) && value > 0) {
    return Math.min(Math.floor(value), 90);
  }
  return 90;
})();
const GUEST_ACCESS_TOKEN_TTL_SECONDS = GUEST_ACCESS_TOKEN_TTL_MINUTES * 60;

function signAccessToken(payload) {
  return jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: ACCESS_TOKEN_TTL });
}

function signGuestAccessToken(payload = {}) {
  return jwt.sign(
    {
      ...payload,
      guest: true,
      token_type: 'guest',
      scope: 'browse:read',
    },
    process.env.JWT_SECRET,
    { expiresIn: GUEST_ACCESS_TOKEN_TTL_SECONDS }
  );
}

function issueGuestSession() {
  const guestSessionId = typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : crypto.randomBytes(16).toString('hex');
  const expiresAt = new Date(Date.now() + GUEST_ACCESS_TOKEN_TTL_SECONDS * 1000);
  const accessToken = signGuestAccessToken({ guest_session_id: guestSessionId });

  return {
    guest: true,
    access_token: accessToken,
    token: accessToken,
    expires_at: expiresAt.toISOString(),
    expires_in_seconds: GUEST_ACCESS_TOKEN_TTL_SECONDS,
    guest_session_id: guestSessionId,
  };
}

function generateRefreshToken() {
  // Node 18+ soporta base64url
  return crypto.randomBytes(48).toString('base64url');
}

function hashToken(token) {
  return crypto.createHash('sha256').update(token).digest('hex');
}

async function persistRefreshToken(userId, refreshToken, req) {
  const refreshHash = hashToken(refreshToken);
  const expiresAt = new Date(Date.now() + REFRESH_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000);
  const ua = req.headers['user-agent'] || null;
  // req.ip funciona; si estás detrás de proxy, considera app.set('trust proxy', 1)
  const ip = (req.headers['x-forwarded-for'] || req.ip || '').toString().substring(0, 45) || null;

  await pool.promise().query(
    `INSERT INTO auth_session (user_id, refresh_token_hash, user_agent, ip, expires_at)
     VALUES (?, ?, ?, ?, ?)`,
    [userId, refreshHash, ua, ip, expiresAt]
  );
}

async function rotateRefreshToken(oldToken) {
  const oldHash = hashToken(oldToken);

  // Busca la sesión vigente por hash
  const [rows] = await pool.promise().query(
    `SELECT id, user_id
       FROM auth_session
      WHERE refresh_token_hash = ?
        AND revoked_at IS NULL
        AND expires_at > NOW()
      LIMIT 1`,
    [oldHash]
  );
  if (!rows.length) return null; // token inválido/revocado/expirado

  const session = rows[0];
  const newRefresh = generateRefreshToken();
  const newHash = hashToken(newRefresh);

  // Ventana deslizante: empuja SIEMPRE 30 días desde ahora
  const days = Number(process.env.REFRESH_TOKEN_TTL_DAYS || 30);

  const [result] = await pool.promise().query(
    `UPDATE auth_session
        SET refresh_token_hash = ?,
            last_used_at = NOW(),
            expires_at = NOW() + INTERVAL ${days} DAY
      WHERE id = ?`,
    [newHash, session.id]
  );

  if (result.affectedRows !== 1) {
    // Log defensivo para detectar carreras u otros problemas
    console.error('rotateRefreshToken: no row updated', { sessionId: session.id });
    return null;
  }

  return { userId: session.user_id, refreshToken: newRefresh };
}

async function revokeRefreshToken(token) {
  const hash = hashToken(token);
  await pool.promise().query(
    `UPDATE auth_session SET revoked_at = NOW() WHERE refresh_token_hash = ?`,
    [hash]
  );
}

async function revokeAllUserSessions(userId) {
  await pool.promise().query(
    `UPDATE auth_session SET revoked_at = NOW()
     WHERE user_id = ? AND revoked_at IS NULL`,
    [userId]
  );
}


// cron diario a las 3:00 AM
cron.schedule('0 3 * * *', async () => {
  try {
    await pool.promise().query(`
      DELETE b FROM booking b
      LEFT JOIN payments p
        ON p.booking_id = b.id AND p.type = 'deposit'
      WHERE b.service_status = 'pending_deposit'
        AND b.order_datetime < (NOW() - INTERVAL 24 HOUR)
        AND (p.id IS NULL OR p.status IN ('requires_payment_method','canceled','payment_failed'));
    `);
    console.log('[CRON] Limpieza de reservas pending_deposit ejecutada');
  } catch (e) {
    console.error('Error en cron cleanup:', e);
  }
});

// cron cada 10 minutos para expirar solicitudes pendientes de aceptación
cron.schedule('*/10 * * * *', async () => {
  try {
    const [rows] = await pool.promise().query(
      `
      SELECT id
      FROM booking
      WHERE service_status = 'requested'
        AND expires_at IS NOT NULL
        AND expires_at <= UTC_TIMESTAMP()
      ORDER BY expires_at ASC
      LIMIT 100
      `
    );

    for (const row of rows || []) {
      await expireRequestedBookingByIdIfNeeded(row.id);
    }

    if ((rows || []).length > 0) {
      console.log(`[CRON] Expiradas ${rows.length} solicitudes de reserva`);
    }
  } catch (error) {
    console.error('Error en cron de expiración de reservas:', error);
  }
});

// cron cada 10 minutos para expirar solicitudes de modificación pendientes
cron.schedule('*/10 * * * *', async () => {
  try {
    const [rows] = await pool.promise().query(
      `
      SELECT DISTINCT booking_id
      FROM booking_change_request
      WHERE status = 'pending'
      ORDER BY booking_id ASC
      LIMIT 100
      `
    );

    let expiredCount = 0;
    const expiredIdsToNotify = [];

    for (const row of rows || []) {
      const connection = await pool.promise().getConnection();
      try {
        await connection.beginTransaction();
        const [[booking]] = await connection.query(
          `
          SELECT
            id,
            service_status,
            settlement_status
          FROM booking
          WHERE id = ?
          LIMIT 1
          FOR UPDATE
          `,
          [row.booking_id]
        );

        if (!booking) {
          await connection.rollback();
          continue;
        }

        const expiredIds = await expirePendingBookingChangeRequestsForBooking(connection, booking, {
          now: new Date(),
        });
        await connection.commit();

        if (expiredIds.length > 0) {
          expiredCount += expiredIds.length;
          expiredIdsToNotify.push(...expiredIds);
        }
      } catch (error) {
        try { await connection.rollback(); } catch {}
        console.error('Error en cron de expiración de solicitudes de modificación:', error);
      } finally {
        connection.release();
      }
    }

    for (const changeRequestId of expiredIdsToNotify) {
      try {
        await sendBookingChangeRequestNotificationEmail({
          changeRequestId,
          mode: 'expired',
        });
      } catch (emailError) {
        console.error('Error sending expired booking change request email:', {
          changeRequestId,
          error: emailError.message,
        });
      }
    }

    if (expiredCount > 0) {
      console.log(`[CRON] Expiradas ${expiredCount} solicitudes de modificación`);
    }
  } catch (error) {
    console.error('Error en cron de solicitudes de modificación:', error);
  }
});

// cron cada 10 minutos para recordar y caducar reservas accepted sin actividad tras la hora prevista
cron.schedule('*/10 * * * *', async () => {
  try {
    const [rows] = await pool.promise().query(
      `
      SELECT id
      FROM booking
      WHERE service_status = 'accepted'
        AND settlement_status = 'none'
        AND requested_start_datetime IS NOT NULL
        AND requested_start_datetime <= (UTC_TIMESTAMP() - INTERVAL 1 HOUR)
      ORDER BY requested_start_datetime ASC
      LIMIT 100
      `
    );

    let reminderCount = 0;
    let autoCanceledCount = 0;

    for (const row of rows || []) {
      const result = await processAcceptedBookingInactivity(row.id);
      if (!result?.handled || !result?.stage || !result?.booking) {
        continue;
      }

      if (result.type === 'reminder') {
        try {
          await Promise.allSettled([
            sendAcceptedBookingInactivityEmail({
              kind: result.stage.reasonCode,
              booking: result.booking,
              recipientRole: 'client',
            }),
            sendAcceptedBookingInactivityEmail({
              kind: result.stage.reasonCode,
              booking: result.booking,
              recipientRole: 'professional',
            }),
          ]);
        } catch (emailError) {
          console.error('Error sending accepted booking inactivity reminder emails:', {
            bookingId: row.id,
            reasonCode: result.stage.reasonCode,
            error: emailError.message,
          });
        }
        reminderCount += 1;
        continue;
      }

      if (result.refundRequest?.paymentIntentId) {
        try {
          await triggerStripeRefundForPaymentIntent(result.refundRequest.paymentIntentId, {
            booking_id: String(result.refundRequest.bookingId),
            source: 'accepted_inactivity_auto_canceled',
          });
        } catch (refundError) {
          console.error('Error refunding deposit after accepted inactivity auto-cancel:', {
            bookingId: result.refundRequest.bookingId,
            paymentIntentId: result.refundRequest.paymentIntentId,
            error: refundError.message,
          });
        }
      } else {
        try {
          await releaseEphemeralBookingPaymentMethodsIfClosed(row.id);
        } catch (cleanupError) {
          console.error('Error releasing ephemeral payment methods after inactivity auto-cancel:', {
            bookingId: row.id,
            error: cleanupError.message,
          });
        }
      }

      try {
        await Promise.allSettled([
          sendAcceptedBookingInactivityEmail({
            kind: result.stage.reasonCode,
            booking: result.booking,
            recipientRole: 'client',
          }),
          sendAcceptedBookingInactivityEmail({
            kind: result.stage.reasonCode,
            booking: result.booking,
            recipientRole: 'professional',
          }),
        ]);
      } catch (emailError) {
        console.error('Error sending accepted booking inactivity auto-cancel emails:', {
          bookingId: row.id,
          reasonCode: result.stage.reasonCode,
          error: emailError.message,
        });
      }

      autoCanceledCount += 1;
    }

    if (reminderCount > 0 || autoCanceledCount > 0) {
      console.log(`[CRON] Reservas accepted sin actividad: ${reminderCount} avisos y ${autoCanceledCount} cancelaciones automáticas`);
    }
  } catch (error) {
    console.error('Error en cron de reservas accepted sin actividad:', error);
  }
});

// cron cada 10 minutos para autocobrar cierres pendientes o enviarlos a revisión
cron.schedule('*/10 * * * *', async () => {
  try {
    const [rows] = await pool.promise().query(
      `
      SELECT id
      FROM booking
      WHERE settlement_status = 'pending_client_approval'
        AND client_approval_deadline_at IS NOT NULL
        AND client_approval_deadline_at <= UTC_TIMESTAMP()
      ORDER BY client_approval_deadline_at ASC
      LIMIT 100
      `
    );

    for (const row of rows || []) {
      await processPendingClosureAutoCharge(row.id);
    }

    if ((rows || []).length > 0) {
      console.log(`[CRON] Procesados ${rows.length} cierres pendientes de autocobro`);
    }
  } catch (error) {
    console.error('Error en cron de autocobro de cierres:', error);
  }
});

// cron semanal para liberar payouts acumulados el miércoles por la mañana tras 7 días de retención
cron.schedule('0 9 * * 3', async () => {
  try {
    const result = await releasePendingProviderPayouts();
    if ((result?.releasedPayments || 0) > 0 || (result?.skippedPayments || 0) > 0) {
      console.log('[CRON] Liberación de payouts ejecutada', result);
    }
  } catch (error) {
    console.error('Error en cron de liberación de payouts:', error);
  }
}, {
  timezone: 'Europe/Madrid',
});

const credentials = JSON.parse(process.env.GCLOUD_KEYFILE_JSON);

// Configura el almacenamiento de Google Cloud
const storage = new Storage({
  projectId: credentials.project_id,
  credentials: credentials,
});
const visionIdDetector = createVisionIdDetector(credentials);

const bucketName = process.env.GCLOUD_BUCKET_NAME;
const bucket = storage.bucket(bucketName);

// Configura Multer para manejar la subida de archivos
const multerMid = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024, // Límite de 10MB por archivo
  },
});

const uploadSignAllowedOrigins = process.env.UPLOAD_SIGN_ALLOWED_ORIGINS
  ? process.env.UPLOAD_SIGN_ALLOWED_ORIGINS.split(',').map(origin => origin.trim()).filter(Boolean)
  : null;

const uploadSignCors = cors({
  origin: uploadSignAllowedOrigins && uploadSignAllowedOrigins.length > 0 ? uploadSignAllowedOrigins : true,
  methods: ['POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type'],
  maxAge: 86400,
});

const uploadSignWindowMs = (() => {
  const value = Number(process.env.UPLOAD_SIGN_RATE_WINDOW_MS);
  if (Number.isFinite(value) && value > 0) {
    return value;
  }
  return 60 * 1000;
})();

const uploadSignMaxRequests = (() => {
  const value = Number(process.env.UPLOAD_SIGN_RATE_LIMIT);
  if (Number.isFinite(value) && value > 0) {
    return value;
  }
  return 10;
})();

const uploadSignLimiter = rateLimit({
  windowMs: uploadSignWindowMs,
  max: uploadSignMaxRequests,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({ error: 'rate_limit_exceeded' });
  },
});

function createAuthRateLimiter({ windowMs, max }) {
  return rateLimit({
    windowMs,
    max,
    standardHeaders: true,
    legacyHeaders: false,
    handler: (req, res) => {
      res.status(429).json({ error: 'AUTH_RATE_LIMIT_EXCEEDED' });
    },
  });
}

const checkEmailLimiter = createAuthRateLimiter({ windowMs: 10 * 60 * 1000, max: 30 });
const loginLimiter = createAuthRateLimiter({ windowMs: 10 * 60 * 1000, max: 10 });
const signupLimiter = createAuthRateLimiter({ windowMs: 30 * 60 * 1000, max: 5 });
const socialAuthLimiter = createAuthRateLimiter({ windowMs: 10 * 60 * 1000, max: 15 });
const forgotPasswordLimiter = createAuthRateLimiter({ windowMs: 15 * 60 * 1000, max: 5 });
const resetPasswordLimiter = createAuthRateLimiter({ windowMs: 15 * 60 * 1000, max: 10 });
const guestSessionLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({ error: 'GUEST_SESSION_RATE_LIMIT_EXCEEDED' });
  },
});



// Ruta de prueba
app.get('/', (req, res) => {
  res.send('El backend está funcionando.');
});

app.get('/api/currency-rates', async (req, res) => {
  const snapshot = await ensureExchangeRatesFresh();
  res.status(200).json({
    base: 'EUR',
    rates: snapshot.rates,
    date: snapshot.effectiveDate,
    fetched_at: snapshot.fetchedAt,
    source: snapshot.source,
    is_fallback: snapshot.isFallback,
  });
});

// Ruta para obtener usuarios
app.get('/api/users', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    connection.query('SELECT * FROM user_account', (err, results) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al obtener usuarios:', err);
        res.status(500).json({ error: 'Error al obtener usuarios.' });
        return;
      }
      res.json(results);
    });
  });
});

// Ruta para verificar si un email ya existe
app.get('/api/check-email', checkEmailLimiter, (req, res) => {
  const email = normalizeEmail(req.query?.email);
  const query = 'SELECT id, auth_provider FROM user_account WHERE email = ? LIMIT 1';

  if (!isValidEmail(email)) {
    return res.status(400).json({ error: 'invalid_email' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexi?n:', err);
      res.status(500).json({ error: 'Error al obtener la conexi?n.' });
      return;
    }

    connection.query(query, [email], (err, results) => {
      connection.release();

      if (err) {
        console.error('Error al verificar el email:', err);
        res.status(500).json({ error: 'Error al verificar el email.' });
        return;
      }

      const existingUser = results[0] || null;
      res.json({
        exists: Boolean(existingUser),
        auth_provider: existingUser?.auth_provider || null,
      });
    });
  });
});

// Ruta para verificar si un usuario ya existe
app.get('/api/check-username', (req, res) => {
  const { username } = req.query;
  const query = 'SELECT COUNT(*) AS count FROM user_account WHERE username = ?';

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    connection.query(query, [username], (err, results) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al verificar el nombre de usuario:', err);
        res.status(500).json({ error: 'Error al verificar el nombre de usuario.' });
        return;
      }
      const count = results[0].count;
      res.json({ exists: count > 0 });
    });
  });
});

// Ruta para verificar el correo electrónico
app.get('/api/verify-email', (req, res) => {
  const { token } = req.query;
  if (!token) {
    return res.status(400).send('Token requerido');
  }
  jwt.verify(token, process.env.JWT_SECRET, (err, decoded) => {
    if (err) {
      return res.status(400).send('Token inválido');
    }
    const userId = decoded.id;
    pool.getConnection((connErr, connection) => {
      if (connErr) {
        console.error('Error al obtener la conexión:', connErr);
        return res.status(500).send('Error de conexión');
      }
      connection.query('UPDATE user_account SET is_verified = 1 WHERE id = ?', [userId], (updErr) => {
        if (updErr) {
          connection.release();
          console.error('Error al verificar el usuario:', updErr);
          return res.status(500).send('Error al verificar el usuario');
        }

        connection.query('UPDATE service SET is_hidden = 0 WHERE user_id = ? AND is_hidden = 1', [userId], (serviceErr) => {
          connection.release();
          if (serviceErr) {
            console.error('Error al activar los servicios del usuario verificado:', serviceErr);
            return res.status(500).send('Error al actualizar los servicios del usuario');
          }
          res.sendFile(path.join(__dirname, 'public', 'verify-success.html'));
        });
      });
    });
  });
});

// Ruta para hacer login  (Access 15m + Refresh 30d)
app.post('/api/login', loginLimiter, async (req, res) => {
  const usernameOrEmail = typeof req.body?.usernameOrEmail === 'string' ? req.body.usernameOrEmail.trim() : '';
  const password = typeof req.body?.password === 'string' ? req.body.password : '';
  const query = 'SELECT * FROM user_account WHERE username = ? OR email = ?';

  try {
    const [results] = await promisePool.query(query, [usernameOrEmail, usernameOrEmail]);

    if (!results.length) {
      return res.json({ success: false, error: 'INVALID_CREDENTIALS' });
    }

    const user = results[0];

    if (user.auth_provider && user.auth_provider !== 'email') {
      return res.status(409).json({
        error: 'AUTH_PROVIDER_MISMATCH',
        auth_provider: user.auth_provider,
      });
    }

    if (typeof user.password !== 'string' || !user.password) {
      return res.json({ success: false, error: 'INVALID_CREDENTIALS' });
    }

    const match = await bcrypt.compare(password, user.password);
    if (!match) {
      return res.json({ success: false, error: 'INVALID_CREDENTIALS' });
    }

    const tokens = await issueAuthTokens(user.id, req);

    return res.json({
      success: true,
      message: 'Inicio de sesión exitoso.',
      user: sanitizeUserRecord(user),
      ...tokens,
    });
  } catch (err) {
    console.error('Error al iniciar sesión:', err);
    return res.status(500).json({ error: 'Error al iniciar sesión.' });
  }
});
// Ruta para crear un nuevo usuario
app.post('/api/signup', signupLimiter, async (req, res) => {
  const rawEmail = typeof req.body?.email === 'string' ? req.body.email : '';
  const password = typeof req.body?.password === 'string' ? req.body.password : '';
  const rawFirstName = typeof req.body?.first_name === 'string' ? req.body.first_name : '';
  const rawSurname = typeof req.body?.surname === 'string' ? req.body.surname : '';
  const rawLanguage = typeof req.body?.language === 'string' ? req.body.language : 'en';
  const rawCountry = typeof req.body?.country === 'string' ? req.body.country : null;
  const rawCurrency = typeof req.body?.currency === 'string' ? req.body.currency : null;
  const rawPlatform = typeof req.body?.platform === 'string' ? req.body.platform : 'ios';
  const rawPhone = typeof req.body?.phone === 'string' ? req.body.phone.trim() : null;

  const email = normalizeEmail(rawEmail);
  const first_name = rawFirstName.trim();
  const surname = rawSurname.trim();
  const language = rawLanguage.trim() || 'en';
  const country = normalizeCountryCode(rawCountry);
  const currency = normalizeCurrencyCode(rawCurrency, 'EUR');
  const platform = rawPlatform.trim().toLowerCase() === 'android' ? 'android' : 'ios';
  const phone = rawPhone || null;

  if (!email || !password || !first_name || !surname) {
    return res.status(400).json({ error: 'MISSING_SIGNUP_FIELDS' });
  }

  if (!isValidEmail(email)) {
    return res.status(400).json({ error: 'INVALID_EMAIL' });
  }

  if (password.length < 8) {
    return res.status(400).json({ error: 'PASSWORD_TOO_SHORT' });
  }

  let connection;
  let userId;
  let username;

  try {
    const [existingUsers] = await promisePool.query(
      'SELECT id, auth_provider FROM user_account WHERE email = ? LIMIT 1',
      [email]
    );

    if (existingUsers.length > 0) {
      const existingUser = existingUsers[0];
      if (existingUser.auth_provider && existingUser.auth_provider !== 'email') {
        return res.status(409).json({
          error: 'AUTH_PROVIDER_MISMATCH',
          auth_provider: existingUser.auth_provider,
        });
      }
      return res.status(409).json({ error: 'EMAIL_EXISTS' });
    }

    const hashedPassword = await bcrypt.hash(password, 10);
    connection = await promisePool.getConnection();
    await connection.beginTransaction();

    ({ userId, username } = await createUserAccountWithDefaults(connection, {
      email,
      password: hashedPassword,
      first_name,
      surname,
      phone,
      language,
      country,
      currency,
      allow_notis: null,
      profile_picture: null,
      is_verified: 0,
      is_professional: 0,
      auth_provider: 'email',
      provider_id: null,
      platform,
    }));

    await connection.commit();

    const user = (await fetchSanitizedUserById(userId)) || {
      id: userId,
      email,
      username,
      first_name,
      surname,
      phone,
      language,
      country,
      currency,
      auth_provider: 'email',
      provider_id: null,
      platform,
      allow_notis: null,
      profile_picture: null,
      is_verified: 0,
      is_professional: 0,
    };

    try {
      const verifyToken = jwt.sign({ id: userId }, process.env.JWT_SECRET, { expiresIn: '1d' });
      const url = process.env.BASE_URL + '/api/verify-email?token=' + verifyToken;
      await sendEmail({
        from: '"Wisdom" <wisdom.helpcontact@gmail.com>',
        to: email,
        subject: 'Confirm your Wisdom',
        html: `
              <!doctype html>
              <html lang="en" style="background:#ffffff;">
                <head>
                  <meta charset="utf-8">
                  <meta name="color-scheme" content="light only">
                  <meta name="viewport" content="width=device-width,initial-scale=1">
                  <title>Confirm your Wisdom</title>
                </head>
                <body style="margin:0;background:#ffffff;">
                  <div style="max-width:640px;margin:0 auto;padding:48px 24px;font-family:Inter,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827;">
            
                    <div style="font-size:24px;font-weight:600;letter-spacing:.6px;margin-bottom:32px;text-align:center;">
                      WISDOM<sup style="font-size:12px;vertical-align:top;">®</sup>
                    </div>
            
                    <h1 style="font-size:30px;font-weight:500;margin:0 0 16px;text-align:center;">Welcome to Wisdom</h1>
            
                    <p style="font-size:16px;line-height:1.55;max-width:420px;margin:0 auto 32px;text-align:center;">
                      You've successfully sign up on Wisdom. Please confirm your email.
                    </p>
            
                    <div style="text-align:center;margin-bottom:50px;">
                      <a href="${url}"
                         style="display:inline-block;padding:22px 100px;background:#111827;border-radius:14px;text-decoration:none;font-size:14px;font-weight:600;color:#ffffff;">
                        Verify email
                      </a>
                    </div>
            
                    <hr style="border:none;height:1px;background-color:#f3f4f6;margin:70px 0;width:100%;" />
            
                    <table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
                      <tr>
                        <td style="padding:0 10px;">
                          <a href="https://wisdom-web.vercel.app/" aria-label="Wisdom web">
                            <img src="${IMG_WISDOM}" width="37" height="37" alt="Wisdom"
                                 style="display:block;border:0;outline:none;text-decoration:none;" />
                          </a>
                        </td>
                        <td style="padding:0 10px;">
                          <a href="https://www.instagram.com/wisdom__app/" aria-label="Instagram">
                            <img src="${IMG_INSTA}" width="37" height="37" alt="Instagram"
                                 style="display:block;border:0;outline:none;text-decoration:none;" />
                          </a>
                        </td>
                        <td style="padding:0 10px;">
                          <a href="https://x.com/wisdom_entity" aria-label="X">
                            <img src="${IMG_X}" width="37" height="37" alt="X"
                                 style="display:block;border:0;outline:none;text-decoration:none;" />
                          </a>
                        </td>
                      </tr>
                    </table>
            
                    <div style="font-size:12px;color:#6b7280;line-height:1.4;text-align:center;">
                      <a href="#" style="color:#6b7280;text-decoration:none;">Privacy Policy</a>
                      &nbsp;·&nbsp;
                      <a href="#" style="color:#6b7280;text-decoration:none;">Terms of Service</a>
                      <br /><br />
                      Mataró, BCN, 08304
                      <br /><br />
                      This email was sent to ${email}
                    </div>
                  </div>
                </body>
              </html>`
      });
    } catch (mailErr) {
      console.error('Error al enviar el correo de verificación:', mailErr);
      try {
        await promisePool.query('DELETE FROM user_account WHERE id = ?', [userId]);
      } catch (cleanupErr) {
        console.error('Error al limpiar signup tras fallo de correo de verificación:', cleanupErr);
      }
      return res.status(503).json({ error: 'SIGNUP_VERIFICATION_EMAIL_FAILED' });
    }

    const tokens = await issueAuthTokens(userId, req);

    return res.status(201).json({
      message: 'Usuario creado.',
      userId,
      user,
      ...tokens,
    });
  } catch (err) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackErr) {
        console.error('Error al revertir signup:', rollbackErr);
      }
    }

    if (err?.status === 409 && err.message === 'EMAIL_EXISTS') {
      return res.status(409).json({ error: 'EMAIL_EXISTS' });
    }

    console.error('Error al crear el usuario:', err);
    return res.status(500).json({ error: 'Error al crear el usuario.' });
  } finally {
    if (connection) {
      connection.release();
    }
  }
});

app.post('/api/auth/google', socialAuthLimiter, async (req, res) => {
  const idToken = typeof req.body?.idToken === 'string' ? req.body.idToken.trim() : '';
  const rawLanguage = typeof req.body?.language === 'string' ? req.body.language : 'en';
  const rawCountry = typeof req.body?.country === 'string' ? req.body.country : null;
  const rawCurrency = typeof req.body?.currency === 'string' ? req.body.currency : null;
  const rawPlatform = typeof req.body?.platform === 'string' ? req.body.platform : 'ios';
  const language = rawLanguage.trim() || 'en';
  const country = normalizeCountryCode(rawCountry);
  const currency = normalizeCurrencyCode(rawCurrency, 'EUR');
  const platform = rawPlatform.trim().toLowerCase() === 'android' ? 'android' : 'ios';

  if (!idToken) {
    return res.status(400).json({ error: 'GOOGLE_ID_TOKEN_REQUIRED' });
  }

  let connection;
  let providerIdFromToken = '';
  let emailFromToken = '';

  try {
    const ticket = await googleAuthClient.verifyIdToken({
      idToken,
      audience: GOOGLE_WEB_CLIENT_ID,
    });

    const payload = ticket.getPayload();
    emailFromToken = typeof payload?.email === 'string' ? payload.email.trim().toLowerCase() : '';
    providerIdFromToken = typeof payload?.sub === 'string' ? payload.sub.trim() : '';

    if (!emailFromToken || !providerIdFromToken) {
      return res.status(400).json({ error: 'GOOGLE_PROFILE_INCOMPLETE' });
    }

    if (!payload?.email_verified) {
      return res.status(400).json({ error: 'GOOGLE_EMAIL_NOT_VERIFIED' });
    }

    const { first_name, surname } = resolveGoogleNameParts(payload);
    const profile_picture = typeof payload?.picture === 'string' && payload.picture.trim() ? payload.picture.trim() : null;

    const [existingUsers] = await promisePool.query(
      'SELECT * FROM user_account WHERE provider_id = ? OR email = ?',
      [providerIdFromToken, emailFromToken]
    );

    const providerMatch = existingUsers.find((item) => String(item.provider_id || '') === providerIdFromToken) || null;
    const emailMatch = existingUsers.find((item) => String(item.email || '').toLowerCase() === emailFromToken) || null;
    const existingUser = providerMatch || emailMatch;

    if (emailMatch && emailMatch.auth_provider === 'google' && emailMatch.provider_id && emailMatch.provider_id !== providerIdFromToken) {
      return res.status(409).json({ error: 'GOOGLE_PROVIDER_MISMATCH', auth_provider: 'google' });
    }

    if (existingUser) {
      if (existingUser.auth_provider !== 'google') {
        return res.status(409).json({
          error: 'AUTH_PROVIDER_MISMATCH',
          auth_provider: existingUser.auth_provider,
        });
      }

      await promisePool.query(
        'UPDATE user_account SET provider_id = COALESCE(provider_id, ?), platform = ?, profile_picture = COALESCE(profile_picture, ?) WHERE id = ?',
        [providerIdFromToken, platform, profile_picture, existingUser.id]
      );

      const user = (await fetchSanitizedUserById(existingUser.id)) || sanitizeUserRecord(existingUser);
      const tokens = await issueAuthTokens(existingUser.id, req);

      return res.json({
        success: true,
        message: 'Inicio de sesión exitoso.',
        user,
        ...tokens,
      });
    }

    connection = await promisePool.getConnection();
    await connection.beginTransaction();

    const { userId, username } = await createUserAccountWithDefaults(connection, {
      email: emailFromToken,
      password: null,
      first_name,
      surname,
      phone: null,
      language,
      country,
      currency,
      allow_notis: null,
      profile_picture,
      is_verified: 1,
      is_professional: 0,
      auth_provider: 'google',
      provider_id: providerIdFromToken,
      platform,
    });

    await connection.commit();

    const user = (await fetchSanitizedUserById(userId)) || {
      id: userId,
      email: emailFromToken,
      username,
      first_name,
      surname,
      phone: null,
      language,
      country,
      currency,
      auth_provider: 'google',
      provider_id: providerIdFromToken,
      platform,
      allow_notis: null,
      profile_picture,
      is_verified: 1,
      is_professional: 0,
    };
    const tokens = await issueAuthTokens(userId, req);

    return res.status(201).json({
      success: true,
      message: 'Usuario creado.',
      userId,
      user,
      ...tokens,
    });
  } catch (err) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackErr) {
        console.error('Error al revertir signup con Google:', rollbackErr);
      }
    }

    if (err?.status === 409 && err.message === 'EMAIL_EXISTS') {
      const [existingUsers] = await promisePool.query(
        'SELECT auth_provider FROM user_account WHERE email = ? LIMIT 1',
        [emailFromToken]
      );
      return res.status(409).json({
        error: 'AUTH_PROVIDER_MISMATCH',
        auth_provider: existingUsers[0]?.auth_provider || 'email',
      });
    }

    if (err?.status === 409 && err.message === 'PROVIDER_ID_EXISTS' && providerIdFromToken) {
      const [existingUsers] = await promisePool.query(
        'SELECT * FROM user_account WHERE provider_id = ? LIMIT 1',
        [providerIdFromToken]
      );
      if (existingUsers.length > 0) {
        const user = sanitizeUserRecord(existingUsers[0]);
        const tokens = await issueAuthTokens(existingUsers[0].id, req);
        return res.json({ success: true, message: 'Inicio de sesión exitoso.', user, ...tokens });
      }
    }

    console.error('Error al autenticar con Google:', err);
    return res.status(401).json({ error: 'GOOGLE_AUTH_FAILED' });
  } finally {
    if (connection) {
      connection.release();
    }
  }
});

app.post('/api/auth/apple', socialAuthLimiter, async (req, res) => {
  const identityToken = typeof req.body?.identityToken === 'string' ? req.body.identityToken.trim() : '';
  const rawEmail = typeof req.body?.email === 'string' ? req.body.email : '';
  const rawFirstName = typeof req.body?.first_name === 'string' ? req.body.first_name : '';
  const rawSurname = typeof req.body?.surname === 'string' ? req.body.surname : '';
  const rawLanguage = typeof req.body?.language === 'string' ? req.body.language : 'en';
  const rawCountry = typeof req.body?.country === 'string' ? req.body.country : null;
  const rawCurrency = typeof req.body?.currency === 'string' ? req.body.currency : null;
  const rawPlatform = typeof req.body?.platform === 'string' ? req.body.platform : 'ios';
  const language = rawLanguage.trim() || 'en';
  const country = normalizeCountryCode(rawCountry);
  const currency = normalizeCurrencyCode(rawCurrency, 'EUR');
  const platform = rawPlatform.trim().toLowerCase() === 'android' ? 'android' : 'ios';

  if (!identityToken) {
    return res.status(400).json({ error: 'APPLE_ID_TOKEN_REQUIRED' });
  }

  let connection;
  let providerIdFromToken = '';
  let emailFromApple = '';

  try {
    const payload = await appleSigninAuth.verifyIdToken(identityToken, {
      audience: APPLE_CLIENT_ID,
    });

    providerIdFromToken = typeof payload?.sub === 'string' ? payload.sub.trim() : '';
    const emailFromToken = typeof payload?.email === 'string' ? payload.email.trim().toLowerCase() : '';
    const emailFromBody = rawEmail.trim().toLowerCase();
    emailFromApple = emailFromToken || emailFromBody;

    if (!providerIdFromToken) {
      return res.status(400).json({ error: 'APPLE_PROFILE_INCOMPLETE' });
    }

    const query = emailFromApple
      ? 'SELECT * FROM user_account WHERE provider_id = ? OR email = ?'
      : 'SELECT * FROM user_account WHERE provider_id = ?';
    const params = emailFromApple ? [providerIdFromToken, emailFromApple] : [providerIdFromToken];
    const [existingUsers] = await promisePool.query(query, params);

    const providerMatch = existingUsers.find((item) => String(item.provider_id || '') === providerIdFromToken) || null;
    const emailMatch = emailFromApple
      ? existingUsers.find((item) => String(item.email || '').toLowerCase() === emailFromApple) || null
      : null;

    if (providerMatch && emailMatch && providerMatch.id !== emailMatch.id) {
      return res.status(409).json({
        error: 'AUTH_PROVIDER_MISMATCH',
        auth_provider: emailMatch.auth_provider || 'email',
      });
    }

    if (emailMatch && emailMatch.auth_provider === 'apple' && emailMatch.provider_id && emailMatch.provider_id !== providerIdFromToken) {
      return res.status(409).json({ error: 'APPLE_PROVIDER_MISMATCH', auth_provider: 'apple' });
    }

    const existingUser = providerMatch || emailMatch;

    if (existingUser) {
      if (existingUser.auth_provider !== 'apple') {
        return res.status(409).json({
          error: 'AUTH_PROVIDER_MISMATCH',
          auth_provider: existingUser.auth_provider,
        });
      }

      await promisePool.query(
        'UPDATE user_account SET provider_id = COALESCE(provider_id, ?), platform = ? WHERE id = ?',
        [providerIdFromToken, platform, existingUser.id]
      );

      const user = (await fetchSanitizedUserById(existingUser.id)) || sanitizeUserRecord(existingUser);
      const tokens = await issueAuthTokens(existingUser.id, req);

      return res.json({
        success: true,
        message: 'Inicio de sesión exitoso.',
        user,
        ...tokens,
      });
    }

    if (!emailFromApple) {
      return res.status(400).json({ error: 'APPLE_EMAIL_REQUIRED' });
    }

    const { first_name, surname } = resolveAppleNameParts({
      first_name: rawFirstName,
      surname: rawSurname,
      email: emailFromApple,
    });

    connection = await promisePool.getConnection();
    await connection.beginTransaction();

    const { userId, username } = await createUserAccountWithDefaults(connection, {
      email: emailFromApple,
      password: null,
      first_name,
      surname,
      phone: null,
      language,
      country,
      currency,
      allow_notis: null,
      profile_picture: null,
      is_verified: 1,
      is_professional: 0,
      auth_provider: 'apple',
      provider_id: providerIdFromToken,
      platform,
    });

    await connection.commit();

    const user = (await fetchSanitizedUserById(userId)) || {
      id: userId,
      email: emailFromApple,
      username,
      first_name,
      surname,
      phone: null,
      language,
      country,
      currency,
      auth_provider: 'apple',
      provider_id: providerIdFromToken,
      platform,
      allow_notis: null,
      profile_picture: null,
      is_verified: 1,
      is_professional: 0,
    };
    const tokens = await issueAuthTokens(userId, req);

    return res.status(201).json({
      success: true,
      message: 'Usuario creado.',
      userId,
      user,
      ...tokens,
    });
  } catch (err) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackErr) {
        console.error('Error al revertir signup con Apple:', rollbackErr);
      }
    }

    if (err?.status === 409 && err.message === 'EMAIL_EXISTS') {
      const [existingUsers] = await promisePool.query(
        'SELECT auth_provider FROM user_account WHERE email = ? LIMIT 1',
        [emailFromApple]
      );
      return res.status(409).json({
        error: 'AUTH_PROVIDER_MISMATCH',
        auth_provider: existingUsers[0]?.auth_provider || 'email',
      });
    }

    if (err?.status === 409 && err.message === 'PROVIDER_ID_EXISTS' && providerIdFromToken) {
      const [existingUsers] = await promisePool.query(
        'SELECT * FROM user_account WHERE provider_id = ? LIMIT 1',
        [providerIdFromToken]
      );
      if (existingUsers.length > 0) {
        const user = sanitizeUserRecord(existingUsers[0]);
        const tokens = await issueAuthTokens(existingUsers[0].id, req);
        return res.json({ success: true, message: 'Inicio de sesión exitoso.', user, ...tokens });
      }
    }

    console.error('Error al autenticar con Apple:', err);
    return res.status(401).json({ error: 'APPLE_AUTH_FAILED' });
  } finally {
    if (connection) {
      connection.release();
    }
  }
});
// Revoca una sesión concreta por refresh token
app.post('/api/logout', async (req, res) => {
  const { refresh_token } = req.body || {};
  if (!refresh_token) {
    return res.status(400).json({ error: 'refresh_token requerido' });
  }
  try {
    await revokeRefreshToken(refresh_token);
    return res.json({ ok: true });
  } catch (e) {
    console.error('Error en logout:', e);
    return res.status(500).json({ error: 'Error al hacer logout' });
  }
});

// Enviar enlace para restablecer contraseña
app.post('/api/forgot-password', forgotPasswordLimiter, async (req, res) => {
  const emailOrUsername = typeof req.body?.emailOrUsername === 'string' ? req.body.emailOrUsername.trim() : '';
  if (!emailOrUsername) {
    return res.status(400).json({ error: 'Email or username required' });
  }

  try {
    const [results] = await promisePool.query(
      'SELECT id, email, auth_provider FROM user_account WHERE email = ? OR username = ? LIMIT 1',
      [emailOrUsername, emailOrUsername]
    );

    if (results.length === 0) {
      return res.json({ message: 'If the account exists, reset instructions were sent.' });
    }

    const account = results[0];
    if (account.auth_provider && account.auth_provider !== 'email') {
      return res.status(409).json({
        error: 'AUTH_PROVIDER_MISMATCH',
        auth_provider: account.auth_provider,
      });
    }

    const resetCode = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = new Date(Date.now() + 15 * 60 * 1000);

    await promisePool.query(
      'REPLACE INTO password_reset_codes (user_id, code, expires_at) VALUES (?, ?, ?)',
      [account.id, resetCode, expiresAt]
    );

    try {
      await sendEmail({
        from: '"Wisdom" <wisdom.helpcontact@gmail.com>',
        to: account.email,
        subject: 'Reset your password for Wisdom',
        html: `
              <!doctype html>
              <html lang="en" style="background:#ffffff;">
                <head>
                  <meta charset="utf-8">
                  <meta name="color-scheme" content="light only">
                  <meta name="viewport" content="width=device-width,initial-scale=1">
                  <title>Reset your password for Wisdom</title>
                </head>
                <body style="margin:0;background:#ffffff;">
                  <div style="max-width:640px;margin:0 auto;padding:48px 24px;font-family:Inter,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827;">
            
                    <div style="font-size:24px;font-weight:600;letter-spacing:.6px;margin-bottom:32px;text-align:center;">
                      WISDOM<sup style="font-size:12px;vertical-align:top;">®</sup>
                    </div>
            
                    <p style="font-size:16px;line-height:1.55;max-width:420px;margin:0 auto 32px;text-align:center;">
                      It looks like you lost your password. Use the code below to reset it.
                    </p>
            
                    <div style="font-size:30px;font-weight:600;margin-bottom:50px;text-align:center;">
                      ${resetCode}
                    </div>
            
                    <hr style="border:none;height:1px;background-color:#f3f4f6;margin:70px 0;width:100%;" />
            
                    <table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
                      <tr>
                        <td style="padding:0 10px;">
                          <a href="https://wisdom-web.vercel.app/" aria-label="Wisdom web">
                            <img src="${IMG_WISDOM}" width="37" height="37" alt="Wisdom"
                                 style="display:block;border:0;outline:none;text-decoration:none;" />
                          </a>
                        </td>
                        <td style="padding:0 10px;">
                          <a href="https://www.instagram.com/wisdom__app/" aria-label="Instagram">
                            <img src="${IMG_INSTA}" width="37" height="37" alt="Instagram"
                                 style="display:block;border:0;outline:none;text-decoration:none;" />
                          </a>
                        </td>
                        <td style="padding:0 10px;">
                          <a href="https://x.com/wisdom_entity" aria-label="X">
                            <img src="${IMG_X}" width="37" height="37" alt="X"
                                 style="display:block;border:0;outline:none;text-decoration:none;" />
                          </a>
                        </td>
                      </tr>
                    </table>
            
                    <div style="font-size:12px;color:#6b7280;line-height:1.4;text-align:center;">
                      <a href="#" style="color:#6b7280;text-decoration:none;">Privacy Policy</a>
                      &nbsp;·&nbsp;
                      <a href="#" style="color:#6b7280;text-decoration:none;">Terms of Service</a>
                      <br /><br />
                      Mataró, BCN, 08304
                      <br /><br />
                      This email was sent to ${account.email}
                    </div>
                  </div>
                </body>
              </html>`
      });
    } catch (mailErr) {
      console.error('Error al enviar el correo de restablecimiento:', mailErr);
      await promisePool.query('DELETE FROM password_reset_codes WHERE user_id = ?', [account.id]);
      return res.status(503).json({ error: 'PASSWORD_RESET_DELIVERY_FAILED' });
    }

    return res.json({ message: 'If the account exists, reset instructions were sent.' });
  } catch (err) {
    console.error('Error al generar el código de restablecimiento:', err);
    return res.status(500).json({ error: 'Error al generar el código.' });
  }
});

// Restablecer contraseña con token
app.post('/api/verify-reset-code', resetPasswordLimiter, async (req, res) => {
  const emailOrUsername = typeof req.body?.emailOrUsername === 'string' ? req.body.emailOrUsername.trim() : '';
  const code = typeof req.body?.code === 'string' ? req.body.code.trim() : '';

  if (!emailOrUsername || !code) {
    return res.status(400).json({ error: 'Code and user required' });
  }

  try {
    const [userRes] = await promisePool.query(
      'SELECT id, auth_provider FROM user_account WHERE email = ? OR username = ? LIMIT 1',
      [emailOrUsername, emailOrUsername]
    );

    if (userRes.length === 0) {
      return res.status(400).json({ error: 'INVALID_OR_EXPIRED_RESET_CODE' });
    }

    const account = userRes[0];
    if (account.auth_provider && account.auth_provider !== 'email') {
      return res.status(409).json({
        error: 'AUTH_PROVIDER_MISMATCH',
        auth_provider: account.auth_provider,
      });
    }

    const [codeRes] = await promisePool.query(
      'SELECT code, expires_at FROM password_reset_codes WHERE user_id = ?',
      [account.id]
    );

    if (codeRes.length === 0) {
      return res.status(400).json({ error: 'INVALID_OR_EXPIRED_RESET_CODE' });
    }

    const record = codeRes[0];
    if (record.code !== code || new Date(record.expires_at) < new Date()) {
      return res.status(400).json({ error: 'INVALID_OR_EXPIRED_RESET_CODE' });
    }

    return res.json({ valid: true });
  } catch (err) {
    console.error('Error al validar el código de restablecimiento:', err);
    return res.status(500).json({ error: 'Error al validar el código.' });
  }
});

app.post('/api/reset-password', resetPasswordLimiter, async (req, res) => {
  const emailOrUsername = typeof req.body?.emailOrUsername === 'string' ? req.body.emailOrUsername.trim() : '';
  const code = typeof req.body?.code === 'string' ? req.body.code.trim() : '';
  const newPassword = typeof req.body?.newPassword === 'string' ? req.body.newPassword : '';

  if (!emailOrUsername || !code || !newPassword) {
    return res.status(400).json({ error: 'Code, user and new password required' });
  }

  if (isPasswordTooShort(newPassword)) {
    return res.status(400).json({ error: 'PASSWORD_TOO_SHORT' });
  }

  try {
    const [userRes] = await promisePool.query(
      'SELECT id, auth_provider FROM user_account WHERE email = ? OR username = ? LIMIT 1',
      [emailOrUsername, emailOrUsername]
    );

    if (userRes.length === 0) {
      return res.status(400).json({ error: 'INVALID_OR_EXPIRED_RESET_CODE' });
    }

    const account = userRes[0];
    if (account.auth_provider && account.auth_provider !== 'email') {
      return res.status(409).json({
        error: 'AUTH_PROVIDER_MISMATCH',
        auth_provider: account.auth_provider,
      });
    }

    const userId = account.id;
    const [codeRes] = await promisePool.query(
      'SELECT code, expires_at FROM password_reset_codes WHERE user_id = ?',
      [userId]
    );

    if (codeRes.length === 0) {
      return res.status(400).json({ error: 'INVALID_OR_EXPIRED_RESET_CODE' });
    }

    const record = codeRes[0];
    if (record.code !== code || new Date(record.expires_at) < new Date()) {
      return res.status(400).json({ error: 'INVALID_OR_EXPIRED_RESET_CODE' });
    }

    const hashed = await bcrypt.hash(newPassword, 10);
    await promisePool.query('UPDATE user_account SET password = ? WHERE id = ?', [hashed, userId]);
    await promisePool.query('DELETE FROM password_reset_codes WHERE user_id = ?', [userId]);
    await revokeAllUserSessions(userId);

    const [results] = await promisePool.query(
      'SELECT id, email, username, first_name, surname, phone, profile_picture, is_professional, language, auth_provider FROM user_account WHERE id = ? LIMIT 1',
      [userId]
    );

    if (results.length === 0) {
      return res.status(500).json({ error: 'Error al obtener el usuario.' });
    }

    const user = sanitizeUserRecord(results[0]);
    const tokens = await issueAuthTokens(userId, req);

    return res.json({
      message: 'Password reset successfully',
      user,
      ...tokens,
    });
  } catch (err) {
    console.error('Error al restablecer la contraseña:', err);
    return res.status(500).json({ error: 'Error al procesar la solicitud.' });
  }
});
// Intercambia refresh_token por nuevos tokens (rota refresh)
app.post('/api/token/refresh', async (req, res) => {
  const { refresh_token } = req.body || {};
  if (!refresh_token) {
    return res.status(400).json({ error: 'refresh_token requerido' });
  }

  try {
    const rotated = await rotateRefreshToken(refresh_token);
    if (!rotated) {
      return res.status(401).json({ error: 'refresh_token inválido o caducado' });
    }

    const access_token = signAccessToken({ id: rotated.userId });
    return res.json({
      access_token,
      refresh_token: rotated.refreshToken,
      token: access_token // compat
    });
  } catch (e) {
    console.error('Error en refresh:', e);
    return res.status(500).json({ error: 'Error al refrescar tokens' });
  }
});

app.post('/api/guest/session', guestSessionLimiter, async (req, res) => {
  try {
    return res.status(200).json(issueGuestSession());
  } catch (error) {
    console.error('Error creating guest session:', error);
    return res.status(500).json({ error: 'guest_session_failed' });
  }
});

// Nueva ruta para generar URL firmada de subida a Google Cloud Storage
app.options('/api/uploads/sign', uploadSignCors, (req, res) => {
  res.sendStatus(204);
});

app.post('/api/uploads/sign', uploadSignCors, uploadSignLimiter, async (req, res) => {
  try {
    const { name, type, size } = req.body || {};

    if (typeof name !== 'string' || !name.trim()) {
      return res.status(400).json({ error: 'invalid_name' });
    }

    const normalizedType = typeof type === 'string' ? type.toLowerCase() : '';
    const extension = ALLOWED_UPLOAD_IMAGE_TYPES.get(normalizedType);
    if (!extension) {
      return res.status(400).json({ error: 'invalid_type' });
    }

    const numericSize = Number(size);
    if (!Number.isFinite(numericSize) || numericSize <= 0 || numericSize > MAX_UPLOAD_IMAGE_SIZE_BYTES) {
      return res.status(400).json({ error: 'invalid_size' });
    }

    if (!bucketName) {
      console.error('Missing GCLOUD_BUCKET_NAME configuration');
      return res.status(500).json({ error: 'upload_not_configured' });
    }

    const objectKey = createUploadObjectKey(name, extension);
    const expiresInSeconds = DEFAULT_UPLOAD_SIGN_EXPIRATION_SECONDS;
    const expiresAt = Date.now() + expiresInSeconds * 1000;

    const [uploadUrl] = await bucket.file(objectKey).getSignedUrl({
      version: 'v4',
      action: 'write',
      expires: expiresAt,
      contentType: normalizedType,
    });

    const publicUrl = buildPublicUrl(bucket.name, objectKey);

    res.status(200).json({
      uploadUrl,
      publicUrl,
      key: objectKey,
    });
  } catch (error) {
    console.error('Error generating upload signature:', error);
    res.status(500).json({ error: 'signature_failed' });
  }
});



//--------------------------------

// Proteger las rutas siguientes
app.use('/api', authenticateToken);


// Ruta para obtener usuarios
app.get('/api/users', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    connection.query('SELECT * FROM user_account', (err, results) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al obtener usuarios:', err);
        res.status(500).json({ error: 'Error al obtener usuarios.' });
        return;
      }
      res.json(results);
    });
  });
});

// Revoca todas las sesiones del usuario actual (requiere access token)
app.post('/api/logout-all', async (req, res) => {
  try {
    await revokeAllUserSessions(req.user.id);
    return res.json({ ok: true });
  } catch (e) {
    console.error('Error en logout-all:', e);
    return res.status(500).json({ error: 'Error al revocar sesiones' });
  }
});

// Ruta legacy que sube imágenes directamente desde el backend
app.post('/api/upload-image', multerMid.single('file'), async (req, res, next) => {

  console.log('Archivo recibido:', req.file);

  try {

    if (!req.file) {
      res.status(400).send('No se subió ningún archivo.');
      return;
    }

    // Detecta el formato de la imagen
    const image = sharp(req.file.buffer);
    const metadata = await image.metadata();
    let format = metadata.format;

    // Procesa la imagen según el formato
    let compressedImage;
    if (format === 'jpeg' || format === 'jpg') {
      compressedImage = await image
        .resize({ width: 800 })  // Ajusta el tamaño si es necesario
        .jpeg({ quality: 80 })   // Comprime la imagen JPEG
        .toBuffer();
    } else if (format === 'png') {
      compressedImage = await image
        .resize({ width: 800 })
        .png({ quality: 60 })    // Comprime la imagen PNG
        .toBuffer();
    } else if (format === 'webp') {
      compressedImage = await image
        .resize({ width: 800 })
        .webp({ quality: 60 })   // Comprime la imagen WebP
        .toBuffer();
    } else if (format === 'heif') {
      compressedImage = await image
        .resize({ width: 800 })
        .tiff({ quality: 60 })   // Comprime la imagen HEIC
        .toBuffer();
    } else {
      // Si el formato no es compatible, puedes devolver un error
      res.status(415).send('Formato de archivo no soportado.');
      return;
    }

    const objectName = generateObjectName(req.file.originalname);
    const blob = bucket.file(objectName);
    const blobStream = blob.createWriteStream();

    blobStream.on('error', err => {
      next(err);
    });

    blobStream.on('finish', () => {
      const publicUrl = `https://storage.googleapis.com/${bucket.name}/${blob.name}`;
      res.status(200).send({ url: publicUrl, objectName });
    });

    blobStream.end(compressedImage);
  } catch (error) {
    res.status(500).send(error);
  }
});

//Ruta para obtener las listas de un usuario en favorites
app.get('/api/user/:userId/lists', authenticateToken, (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res, req.params.userId);
  if (!requestedUserId) return;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Obtener las listas del usuario en service_list y las listas compartidas en shared_list
    let query = `
      SELECT id, list_name, 'owner' AS role FROM service_list WHERE user_id = ?
      UNION
      SELECT service_list.id, service_list.list_name, 'shared' AS role 
      FROM service_list
      JOIN shared_list ON service_list.id = shared_list.list_id
      WHERE shared_list.user_id = ?;
    `;

    connection.query(query, [requestedUserId, requestedUserId], (err, lists) => {
      if (err) {
        console.error('Error al obtener las listas:', err);
        res.status(500).json({ error: 'Error al obtener las listas.' });
        connection.release();  // Libera la conexión
        return;
      }

      // Iterar sobre las listas para obtener los detalles
      const listsWithDetailsPromises = lists.map(list => {
        const query = (sql, params) => new Promise((resolve, reject) => {
          connection.query(sql, params, (err, results) => {
            if (err) {
              reject(err);
            } else {
              resolve(results);
            }
          });
        });

        return (async () => {
          const itemCountResult = await query('SELECT COUNT(*) as item_count FROM item_list WHERE list_id = ?', [list.id]);
          const lastItemDateResult = await query('SELECT MAX(added_datetime) as last_item_date FROM item_list WHERE list_id = ?', [list.id]);

          // Obtener todos los servicios de la lista ordenados por el id de inserción
          const services = await query('SELECT service_id FROM item_list WHERE list_id = ? ORDER BY id', [list.id]);

          const servicesWithImages = [];

          for (const service of services) {
            if (servicesWithImages.length >= 3) {
              break;
            }

            const images = await query('SELECT image_url, object_name FROM service_image WHERE service_id = ? ORDER BY `order` LIMIT 1', [service.service_id]);

            if (images.length > 0 && images[0].image_url) {
              servicesWithImages.push({
                service_id: service.service_id,
                image_url: images[0].image_url,
                object_name: images[0].object_name
              });
            }
          }

          return {
            id: list.id,
            title: list.list_name,
            role: list.role,  // Rol del usuario en la lista (propietario o compartido)
            item_count: itemCountResult[0].item_count,
            last_item_date: lastItemDateResult[0].last_item_date,
            services: servicesWithImages
          };
        })();
      });

      Promise.all(listsWithDetailsPromises)
        .then(listsWithDetails => {
          res.json(listsWithDetails);
        })
        .catch(error => {
          console.error('Error al obtener los detalles de las listas:', error);
          res.status(500).json({ error: 'Error al obtener los detalles de las listas.' });
        })
        .finally(() => {
          connection.release();  // Libera la conexión
        });
    });
  });
});

//Ruta para actulizar el nombre de una lista
app.put('/api/list/:listId', (req, res) => {
  const { listId } = req.params;
  const { newName } = req.body;

  if (!newName) {
    return res.status(400).json({ error: 'El nuevo nombre de la lista es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Actualizar el nombre de la lista
    connection.query('UPDATE service_list SET list_name = ? WHERE id = ?', [newName, listId], (err, result) => {
      if (err) {
        console.error('Error al actualizar el nombre de la lista:', err);
        connection.release(); // Libera la conexión
        return res.status(500).json({ error: 'Error al actualizar el nombre de la lista.' });
      }

      if (result.affectedRows === 0) {
        connection.release(); // Libera la conexión
        return res.status(404).json({ error: 'Lista no encontrada.' });
      }

      res.json({ message: 'Nombre de la lista actualizado con éxito.' });
      connection.release(); // Libera la conexión
    });
  });
});

// Ruta para borrar una lista desde list
app.delete('/api/list/:listId', (req, res) => {
  const { listId } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Eliminar la lista
    connection.query('DELETE FROM service_list WHERE id = ?', [listId], (err, result) => {
      if (err) {
        console.error('Error al eliminar la lista:', err);
        connection.release(); // Libera la conexión
        return res.status(500).json({ error: 'Error al eliminar la lista.' });
      }

      if (result.affectedRows === 0) {
        connection.release(); // Libera la conexión
        return res.status(404).json({ error: 'Lista no encontrada.' });
      }

      // Opcional: eliminar los items asociados a la lista
      connection.query('DELETE FROM item_list WHERE list_id = ?', [listId], (err) => {
        if (err) {
          console.error('Error al eliminar los items de la lista:', err);
        }
        connection.release(); // Libera la conexión
      });

      res.json({ message: 'Lista eliminada con éxito.' });
    });
  });
});

//Ruta para compartir una lista
app.post('/api/list/share', (req, res) => {
  const { listId, user, permissions } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Verificar si el usuario existe y obtener su ID
    const getUserIdQuery = 'SELECT id FROM user_account WHERE username = ? OR email = ?';
    connection.query(getUserIdQuery, [user, user], (err, results) => {
      if (err) {
        console.error('Error al consultar el usuario:', err);
        connection.release(); // Libera la conexión
        return res.status(500).json({ error: 'Error al consultar el usuario.' });
      }

      if (results.length === 0) {
        connection.release(); // Libera la conexión
        return res.status(201).json({ notFound: true });
      }

      const userId = results[0].id;

      // Insertar una nueva fila en shared_list
      const insertQuery = 'INSERT INTO shared_list (list_id, user_id, permissions) VALUES (?, ?, ?)';
      connection.query(insertQuery, [listId, userId, permissions], (err, result) => {
        if (err) {
          console.error('Error al añadir el usuario a la lista compartida:', err);
          connection.release(); // Libera la conexión
          return res.status(500).json({ error: 'Error al añadir el usuario a la lista compartida.' });
        }

        res.status(201).json({ message: 'Usuario añadido a la lista compartida con éxito.' });
        connection.release(); // Libera la conexión
      });
    });
  });
});

// Ruta para obtener todos los items de una lista por su ID
app.get('/api/lists/:id/items', (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Usar una sola consulta con JOIN para obtener los ítems y datos adicionales de la tabla service, price, user_account y review
    let query = `
      SELECT
        item_list.id AS item_id, 
        item_list.list_id, 
        item_list.service_id, 
        item_list.note, 
        item_list.order, 
        item_list.added_datetime,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.is_hidden,
        service.service_created_datetime,
        service.last_edit_datetime,
        price.price,
        price.currency AS currency,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.password,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.joined_datetime,
        user_account.is_professional,
        user_account.language,
        user_account.allow_notis,
        user_account.currency AS user_currency,
        user_account.money_in_wallet,
        user_account.professional_started_datetime,
        user_account.is_expert,
        user_account.is_verified,
        user_account.strikes_num,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating
      FROM item_list
      JOIN service ON item_list.service_id = service.id
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      LEFT JOIN (
        SELECT 
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE item_list.list_id = ?
        AND service.is_hidden = 0;
    `;


    connection.query(query, [id], (err, itemsWithService) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener los ítems de la lista:', err);
        res.status(500).json({ error: 'Error al obtener los ítems de la lista.' });
        return;
      }

      if (itemsWithService.length > 0) {
        res.status(200).json(itemsWithService);
      } else {
        res.status(200).json({ empty: true, message: 'No se encontraron ítems para esta lista.' });
      }
    });
  });
});

//Ruta para añadir una nota
app.put('/api/items/:id/note', (req, res) => {
  const { id } = req.params;
  const { note } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para actualizar la columna 'note' en la tabla 'item_list'
    const query = `
      UPDATE item_list
      SET note = ?
      WHERE id = ?
    `;

    connection.query(query, [note, id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al actualizar la nota del ítem:', err);
        res.status(500).json({ error: 'Error al actualizar la nota del ítem.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Nota actualizada con éxito.' });
      } else {
        res.status(404).json({ message: 'Ítem no encontrado.' });
      }
    });
  });
});

// Ruta para borrar una lista desde favorites
app.delete('/api/lists/:id', (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    connection.query('DELETE FROM service_list WHERE id = ?', [id], (err, result) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al eliminar la lista:', err);
        res.status(500).json({ error: 'Error al eliminar la lista.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Lista eliminada con éxito' });
      } else {
        res.status(404).json({ message: 'Lista no encontrada' });
      }
    });
  });
});

//Ruta para obtener todas las familias
app.get('/api/service-family', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener todos los registros de la tabla 'service_family'
    const query = 'SELECT id, family_key FROM service_family ORDER BY id ASC';

    connection.query(query, (err, results) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener los valores de la tabla service_family:', err);
        res.status(500).json({ error: 'Error al obtener los valores.' });
        return;
      }

      res.status(200).json(results); // Devolver los resultados
    });
  });
});

//Ruta para obtener todas las categorias de una lista a partir de la id de la lista
app.get('/api/service-family/:id/categories', (req, res) => {
  const { id } = req.params; // ID del service_family

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener las categorías asociadas a un service_family
    const query = `
      SELECT sc.id AS service_category_id, sct.id AS service_category_type_id, sct.category_key
      FROM service_category sc
      JOIN service_category_type sct ON sc.service_category_type_id = sct.id
      WHERE sc.service_family_id = ?
      ORDER BY sc.id ASC
    `;

    connection.query(query, [id], (err, results) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener las categorías:', err);
        res.status(500).json({ error: 'Error al obtener las categorías.' });
        return;
      }

      res.status(200).json(results); // Devolver las categorías
    });
  });
});

//Ruta para mostrar todos los servicios de una categoria
app.get('/api/category/:id/services', async (req, res) => {
  await ensureExchangeRatesFresh();
  const categoryId = Number(req.params.id);
  const viewerId = Number(req.query.viewer_id ?? req.query.user_id ?? null);
  const viewerCurrency = await resolveUserCurrency(viewerId, 'EUR');

  const filters = extractServiceFilters(req.query);
  const filtersClause = buildServiceFilterClause(filters, { targetCurrency: viewerCurrency });

  const query = `
    SELECT
      service.id AS service_id,
      service.service_title,
      service.description,
      service.service_category_id,
      service.price_id,
      service.latitude,
      service.longitude,
      service.action_rate,
      service.user_can_ask,
      service.user_can_consult,
      service.price_consult,
      service.consult_via_id,
      service.is_individual,
      service.is_hidden,
      service.service_created_datetime,
      service.last_edit_datetime,
      price.price,
      price.currency AS currency,
      price.price_type,
      user_account.id AS user_id,
      user_account.email,
      user_account.phone,
      user_account.username,
      user_account.first_name,
      user_account.surname,
      user_account.profile_picture,
      user_account.is_professional,
      user_account.is_verified,
      user_account.language,
      COALESCE(review_data.review_count, 0) AS review_count,
      COALESCE(review_data.average_rating, 0) AS average_rating,
      category_type.category_key,
      family.family_key,
      category_type.category_key AS service_category_name,
      family.family_key AS service_family,
      tags_data.tags,
      images_data.images,
      language_data.languages
    FROM service
    JOIN price ON service.price_id = price.id
    JOIN user_account ON service.user_id = user_account.id
    JOIN service_category category ON service.service_category_id = category.id
    JOIN service_family family ON category.service_family_id = family.id
    JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
    LEFT JOIN (
      SELECT service_id, COUNT(*) AS review_count, AVG(rating) AS average_rating
      FROM review
      GROUP BY service_id
    ) AS review_data ON service.id = review_data.service_id
    LEFT JOIN (
      SELECT service_id, JSON_ARRAYAGG(tag) AS tags
      FROM (
        SELECT service_id, tag
        FROM service_tags
        ORDER BY service_id, tag
      ) AS ordered_tags
      GROUP BY service_id
    ) AS tags_data ON tags_data.service_id = service.id
    LEFT JOIN (
      SELECT service_id, JSON_ARRAYAGG(image_data) AS images
      FROM (
        SELECT
          si.service_id,
          JSON_OBJECT(
            'id', si.id,
            'image_url', si.image_url,
            'object_name', si.object_name,
            'order', si.\`order\`
          ) AS image_data
        FROM service_image si
        ORDER BY si.service_id, si.\`order\`
      ) AS ordered_images
      GROUP BY service_id
    ) AS images_data ON images_data.service_id = service.id
    LEFT JOIN (
      SELECT service_id, JSON_ARRAYAGG(language) AS languages
      FROM service_language
      GROUP BY service_id
    ) AS language_data ON language_data.service_id = service.id
    WHERE service.is_hidden = 0
      AND service.service_category_id = ?${filtersClause.sql};
  `;

  try {
    const params = [categoryId, ...filtersClause.params];
    const [rows] = await promisePool.query(query, params);
    if (rows.length === 0) return res.status(200).json([]);

    let likedServiceIds = new Set();

    // Sólo calculamos likes si nos llega un viewerId válido
    if (Number.isFinite(viewerId)) {
      const serviceIds = rows.map(s => s.service_id).filter(Boolean);
      if (serviceIds.length) {
        const placeholders = serviceIds.map(() => '?').join(', ');
        const likedQuery = `
          SELECT DISTINCT il.service_id
          FROM item_list il
          JOIN service_list sl ON il.list_id = sl.id
          LEFT JOIN shared_list sh ON sh.list_id = il.list_id
          WHERE (sl.user_id = ? OR sh.user_id = ?)
            AND il.service_id IN (${placeholders})
        `;
        const [likedRows] = await promisePool.query(
          likedQuery,
          [viewerId, viewerId, ...serviceIds]
        );
        likedServiceIds = new Set(likedRows.map(r => Number(r.service_id)));
      }
    }

    const withLiked = rows.map(s => ({
      ...s,
      is_liked: likedServiceIds.has(Number(s.service_id)) ? 1 : 0,
    }));

    return res.status(200).json(withLiked);
  } catch (err) {
    console.error('Error al obtener servicios por categoría:', err);
    return res.status(500).json({ error: 'Error al obtener servicios por categoría.' });
  }
});

//Ruta para subir varias fotos (create service)
app.post('/api/upload-images', upload.array('files'), async (req, res, next) => {


  try {
    const results = await Promise.all(req.files.map(async (file, index) => {
      const image = sharp(file.buffer);
      const metadata = await image.metadata();
      let compressedImage;

      if (metadata.format === 'jpeg' || metadata.format === 'jpg') {
        compressedImage = await image
          .resize({ width: 800 })
          .jpeg({ quality: 80 })
          .toBuffer();
      } else if (metadata.format === 'png') {
        compressedImage = await image
          .resize({ width: 800 })
          .png({ quality: 60 })
          .toBuffer();
      } else if (metadata.format === 'webp') {
        compressedImage = await image
          .resize({ width: 800 })
          .webp({ quality: 60 })
          .toBuffer();
      } else if (metadata.format === 'heif') {
        compressedImage = await image
          .resize({ width: 800 })
          .toBuffer();  // Usar toBuffer() para HEIF si no soporta compresión
      } else {
        throw new Error('Formato de archivo no soportado.');
      }

      const objectName = generateObjectName(file.originalname);
      const blob = bucket.file(objectName);
      const blobStream = blob.createWriteStream();

      return new Promise((resolve, reject) => {
        blobStream.on('error', reject);
        blobStream.on('finish', () => {
          const publicUrl = `https://storage.googleapis.com/${bucket.name}/${blob.name}`;
          resolve({ url: publicUrl, objectName, order: index + 1 });
        });
        blobStream.end(compressedImage);
      });
    }));

    res.status(200).send(results);
  } catch (error) {
    console.error('Error en la carga de imágenes:', error);
    res.status(500).send(error.message);
  }
});

//Ruta para crear un servicio
app.post('/api/service', (req, res) => {
  const {
    service_title,
    user_id,
    description,
    service_category_id,
    price,
    price_type,
    latitude,
    longitude,
    action_rate,
    user_can_ask,
    user_can_consult,
    price_consult,
    consult_via_provide,
    consult_via_username,
    consult_via_url,
    minimum_notice_policy,
    is_individual,
    allow_discounts,
    discount_rate,
    experience_years,
    languages,
    tags,
    experiences,
    images,
    hobbies
  } = req.body;
  const normalizedUserCanAsk = user_can_ask === undefined || user_can_ask === null ? true : Boolean(user_can_ask);
  const normalizedUserCanConsult = user_can_consult === undefined || user_can_consult === null ? false : Boolean(user_can_consult);
  const normalizedAllowDiscounts = allow_discounts === undefined || allow_discounts === null ? false : Boolean(allow_discounts);
  const normalizedDiscountRate = normalizedAllowDiscounts ? discount_rate ?? null : null;
  const normalizedPriceConsult = normalizedUserCanConsult ? price_consult ?? null : null;
  const normalizedConsultViaProvide = normalizedUserCanConsult ? consult_via_provide ?? null : null;
  const normalizedConsultViaUsername = normalizedUserCanConsult ? consult_via_username ?? null : null;
  const normalizedConsultViaUrl = normalizedUserCanConsult ? consult_via_url ?? null : null;
  const normalizedIsIndividual = is_individual === undefined || is_individual === null ? true : Boolean(is_individual);
  const normalizedHobbies = typeof hobbies === 'string' && hobbies.trim().length > 0 ? hobbies : null;
  let normalizedExperienceYears;
  let normalizedMinimumNoticePolicy;
  try {
    normalizedExperienceYears = parseExperienceYearsInput(experience_years, 1);
    normalizedMinimumNoticePolicy = parseMinimumNoticePolicyInput(minimum_notice_policy, 1440);
  } catch (parseError) {
    return res.status(parseError.status || 400).json({ error: parseError.message || 'invalid_service_fields' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    connection.beginTransaction(err => {
      if (err) {
        console.error('Error al iniciar la transacción:', err);
        connection.release(); // Liberar conexión en caso de error
        res.status(500).json({ error: 'Error al iniciar la transacción.' });
        return;
      }

      const stripeAccountQuery = 'SELECT is_verified, currency FROM user_account WHERE id = ?';
      connection.query(stripeAccountQuery, [user_id], (accountErr, accountResults) => {
        if (accountErr) {
          return connection.rollback(() => {
            console.error('Error al consultar el estado del profesional:', accountErr);
            connection.release();
            res.status(500).json({ error: 'Error al consultar el estado del profesional.' });
          });
        }

        if (accountResults.length === 0) {
          return connection.rollback(() => {
            connection.release();
            res.status(404).json({ error: 'professional_not_found' });
          });
        }

        const isVerified = Boolean(accountResults[0]?.is_verified);
        const priceCurrency = normalizeCurrencyCode(accountResults[0]?.currency, 'EUR');
        const isHiddenValue = isVerified ? 0 : 1;

        // 1. Insertar en la tabla 'price'
        const priceQuery = 'INSERT INTO price (price, currency, price_type) VALUES (?, ?, ?)';
        connection.query(priceQuery, [price, priceCurrency, price_type], (err, result) => {
          if (err) {
            return connection.rollback(() => {
              console.error('Error al insertar en la tabla price:', err);
              connection.release(); // Liberar conexión en caso de error
              res.status(500).json({ error: 'Error al insertar en la tabla price.' });
            });
          }

          const price_id = result.insertId;

          // 2. Si user_can_consult es true, insertar en consult_via, de lo contrario, saltarlo.
          let consult_via_id = null;
          const insertService = () => {
            // 3. Insertar en la tabla 'service'
            const serviceQuery = `
              INSERT INTO service (
                service_title, user_id, description, service_category_id, price_id, latitude, longitude, action_rate, user_can_ask, user_can_consult, price_consult, consult_via_id, is_individual, minimum_notice_policy, allow_discounts, discount_rate, hobbies, experience_years, service_created_datetime, is_hidden, last_edit_datetime
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?, ?)
            `;
            const serviceValues = [
              service_title, user_id, description, service_category_id, price_id, latitude, longitude,
              action_rate, normalizedUserCanAsk, normalizedUserCanConsult, normalizedPriceConsult, consult_via_id, normalizedIsIndividual, normalizedMinimumNoticePolicy, normalizedAllowDiscounts, normalizedDiscountRate, normalizedHobbies, normalizedExperienceYears, isHiddenValue, null
            ];

            connection.query(serviceQuery, serviceValues, (err, result) => {
              if (err) {
                return connection.rollback(() => {
                  console.error('Error al insertar en la tabla service:', err);
                  connection.release(); // Liberar conexión en caso de error
                  res.status(500).json({ error: 'Error al insertar en la tabla service.' });
                });
              }

              const service_id = result.insertId;

              // 4. Insertar lenguajes en 'service_language'
              if (languages && languages.length > 0) {
                const languageQuery = 'INSERT INTO service_language (service_id, language) VALUES ?';
                const languageValues = languages.map(lang => [service_id, lang]);

                connection.query(languageQuery, [languageValues], err => {
                  if (err) {
                    return connection.rollback(() => {
                      console.error('Error al insertar lenguajes:', err);
                      connection.release(); // Liberar conexión en caso de error
                      res.status(500).json({ error: 'Error al insertar lenguajes.' });
                    });
                  }
                });
              }

              // 5. Insertar tags en 'service_tags'
              if (tags && tags.length > 0) {
                const tagsQuery = 'INSERT INTO service_tags (service_id, tag) VALUES ?';
                const tagsValues = tags.map(tag => [service_id, tag]);

                connection.query(tagsQuery, [tagsValues], err => {
                  if (err) {
                    return connection.rollback(() => {
                      console.error('Error al insertar tags:', err);
                      connection.release(); // Liberar conexión en caso de error
                      res.status(500).json({ error: 'Error al insertar tags.' });
                    });
                  }
                });
              }

              // 6. Insertar experiencias en 'experience_place'
              if (experiences && experiences.length > 0) {
                const experienceQuery = 'INSERT INTO experience_place (service_id, experience_title, place_name, experience_started_date, experience_end_date) VALUES ?';
                const experienceValues = experiences.map(exp => [
                  service_id,
                  exp.experience_title,
                  exp.place_name,
                  new Date(exp.experience_started_date).toISOString().slice(0, 19).replace('T', ' '),
                  exp.experience_end_date ? new Date(exp.experience_end_date).toISOString().slice(0, 19).replace('T', ' ') : null
                ]);

                connection.query(experienceQuery, [experienceValues], err => {
                  if (err) {
                    return connection.rollback(() => {
                      console.error('Error al insertar experiencias:', err);
                      console.error('Consulta:', experienceQuery);
                      console.error('Valores:', experienceValues);
                      connection.release(); // Liberar conexión en caso de error
                      res.status(500).json({ error: 'Error al insertar experiencias.' });
                    });
                  } else {
                    console.log('Experiencias insertadas correctamente');
                  }
                });
              }

              // 7. Insertar imágenes en 'service_image'
              if (images && images.length > 0) {
                const imageQuery = 'INSERT INTO service_image (service_id, image_url, object_name, `order`) VALUES ?';
                const imageValues = images.map((img, index) => {
                  const imageUrl = resolveImageUrl(img);
                  const objectName = resolveImageObjectName(
                    img?.object_name ?? img?.objectName,
                    imageUrl,
                    bucketName
                  );

                  return [
                    service_id,
                    imageUrl,
                    objectName,
                    img?.order ?? index + 1
                  ];
                });

                connection.query(imageQuery, [imageValues], err => {
                  if (err) {
                    return connection.rollback(() => {
                      console.error('Error al insertar imágenes:', err);
                      connection.release(); // Liberar conexión en caso de error
                      res.status(500).json({ error: 'Error al insertar imágenes.' });
                    });
                  }
                });
              }
              // 8. Marcar al usuario como profesional si aún no lo es
              const professionalQuery = 'UPDATE user_account SET is_professional = 1, professional_started_datetime = NOW() WHERE id = ? AND is_professional = 0';
              connection.query(professionalQuery, [user_id], err => {
                if (err) {
                  return connection.rollback(() => {
                    console.error('Error al actualizar el usuario como profesional:', err);
                    connection.release();
                    res.status(500).json({ error: 'Error al actualizar el usuario.' });
                  });
                }

                // Commit final
                connection.commit(err => {
                  if (err) {
                    return connection.rollback(() => {
                      console.error('Error al hacer commit de la transacción:', err);
                      connection.release(); // Liberar conexión en caso de error
                      res.status(500).json({ error: 'Error al hacer commit de la transacción.' });
                    });
                  }

                  connection.release(); // Liberar conexión después del commit exitoso
                  res.status(201).json({
                    message: 'Servicio creado con éxito.',
                    service_id,
                    is_hidden: isHiddenValue,
                    is_verified: isVerified,
                    requires_email_verification: !isVerified,
                    user_is_professional: true,
                  });
                });
              });
            });
          };

          // 2.1. Insertar consult_via y continuar con insertService
          if (normalizedUserCanConsult) {
            const consultViaQuery = 'INSERT INTO consult_via (provider, username, url) VALUES (?, ?, ?)';
            connection.query(consultViaQuery, [normalizedConsultViaProvide, normalizedConsultViaUsername, normalizedConsultViaUrl], (err, result) => {
              if (err) {
                return connection.rollback(() => {
                  console.error('Error al insertar en la tabla consult_via:', err);
                  connection.release(); // Liberar conexión en caso de error
                  res.status(500).json({ error: 'Error al insertar en la tabla consult_via.' });
                });
              }

              consult_via_id = result.insertId;
              insertService(); // Llama a insertService después de haber obtenido el consult_via_id
            });
          } else {
            insertService(); // Llama a insertService directamente si user_can_consult es false
          }
        });
      });
    });
  });
});

app.delete('/api/services/:id', async (req, res) => {
  const serviceId = Number(req.params.id);
  if (!Number.isInteger(serviceId) || serviceId <= 0) {
    return res.status(400).json({ error: 'invalid_service_id' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();
    const ownership = await fetchOwnedService(connection, serviceId, req.user.id, { lock: true });
    if (ownership.error) {
      await connection.rollback();
      const mapped = mapOwnershipError(ownership.error);
      return res.status(mapped.status).json(mapped.body);
    }

    const { service } = ownership;

    const [[activeBookings]] = await connection.query(
      `SELECT COUNT(*) AS count
         FROM booking
        WHERE service_id = ?
          AND (
            service_status NOT IN ('canceled', 'expired')
            AND NOT (
              service_status = 'finished'
              AND settlement_status IN ('paid', 'refunded', 'partially_refunded')
            )
          )`,
      [serviceId]
    );

    if (activeBookings.count > 0) {
      await connection.rollback();
      return res.status(409).json({ error: 'service_has_active_bookings' });
    }

    await connection.query(
      'DELETE sra FROM service_report_attachment sra JOIN service_report sr ON sra.report_id = sr.id WHERE sr.service_id = ?',
      [serviceId]
    );
    await connection.query('DELETE FROM service_report WHERE service_id = ?', [serviceId]);
    await connection.query('DELETE FROM review WHERE service_id = ?', [serviceId]);
    await connection.query('DELETE FROM item_list WHERE service_id = ?', [serviceId]);
    await connection.query('DELETE FROM experience_place WHERE service_id = ?', [serviceId]);
    await connection.query('DELETE FROM service_language WHERE service_id = ?', [serviceId]);
    await connection.query('DELETE FROM service_tags WHERE service_id = ?', [serviceId]);
    await connection.query('DELETE FROM service_image WHERE service_id = ?', [serviceId]);

    await connection.query('DELETE FROM service WHERE id = ?', [serviceId]);
    await connection.query('DELETE FROM price WHERE id = ?', [service.price_id]);
    if (service.consult_via_id) {
      await connection.query('DELETE FROM consult_via WHERE id = ?', [service.consult_via_id]);
    }

    await connection.commit();
    return res.status(200).json({ message: 'Servicio eliminado correctamente.' });
  } catch (error) {
    await connection.rollback();
    if (error && error.code === 'ER_ROW_IS_REFERENCED_2') {
      return res.status(409).json({ error: 'service_has_related_records' });
    }
    console.error('Error al eliminar el servicio:', error);
    return res.status(500).json({ error: 'Error al eliminar el servicio.' });
  } finally {
    connection.release();
  }
});

app.patch('/api/services/:id/visibility', authenticateToken, async (req, res) => {
  const serviceId = Number(req.params.id);
  if (!Number.isInteger(serviceId) || serviceId <= 0) {
    return res.status(400).json({ error: 'invalid_service_id' });
  }

  if (!Object.prototype.hasOwnProperty.call(req.body || {}, 'is_hidden')) {
    return res.status(400).json({ error: 'is_hidden_required' });
  }

  let isHidden;
  try {
    isHidden = parseBooleanInput(req.body.is_hidden, null, 'is_hidden');
    if (isHidden === null) {
      throw invalidInputError('invalid_boolean_is_hidden');
    }
  } catch (error) {
    return res.status(error.status || 400).json({ error: error.message || 'invalid_boolean_is_hidden' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();
    const ownership = await fetchOwnedService(connection, serviceId, req.user.id, { lock: true });
    if (ownership.error) {
      await connection.rollback();
      const mapped = mapOwnershipError(ownership.error);
      return res.status(mapped.status).json(mapped.body);
    }

    if (!isHidden && !ownership.service.is_verified) {
      await connection.rollback();
      return res.status(409).json({ error: 'email_not_verified_for_visibility' });
    }

    await connection.query(
      'UPDATE service SET is_hidden = ?, last_edit_datetime = NOW() WHERE id = ?',
      [isHidden ? 1 : 0, serviceId]
    );

    await connection.commit();
    return res.status(200).json({
      message: isHidden ? 'Servicio ocultado correctamente.' : 'Servicio visible nuevamente.',
      is_hidden: isHidden ? 1 : 0
    });
  } catch (error) {
    await connection.rollback();
    console.error('Error al actualizar la visibilidad del servicio:', error);
    return res.status(500).json({ error: 'Error al actualizar la visibilidad del servicio.' });
  } finally {
    connection.release();
  }
});

//Ruta mira si profesional tiene vacation mode activo o no
app.get('/api/professionals/:id/vacation-mode', authenticateToken, async (req, res) => {
  const professionalId = Number(req.params.id);
  if (!Number.isInteger(professionalId) || professionalId <= 0) {
    return res.status(400).json({ error: 'invalid_professional_id' });
  }
  // Solo el dueño, admin o soporte
  const isOwner = req.user && Number(req.user.id) === professionalId;
  const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
  if (!isOwner && !isStaff) return res.status(403).json({ error: 'not_authorized' });

  try {
    const [[row]] = await promisePool.query(
      'SELECT id, is_professional, vacation_mode FROM user_account WHERE id = ?',
      [professionalId]
    );
    if (!row) return res.status(404).json({ error: 'professional_not_found' });
    if (!row.is_professional) return res.status(400).json({ error: 'user_not_professional' });

    return res.status(200).json({ vacation_mode: !!row.vacation_mode });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: 'read_vacation_mode_failed' });
  }
});

//Ruta cambia vacation mode a true o false i oculta/muestra todos sus servicios
app.patch('/api/professionals/:id/vacation-mode', async (req, res) => {
  const professionalId = Number(req.params.id);
  if (!Number.isInteger(professionalId) || professionalId <= 0) {
    return res.status(400).json({ error: 'invalid_professional_id' });
  }

  if (!Object.prototype.hasOwnProperty.call(req.body || {}, 'vacation_mode')) {
    return res.status(400).json({ error: 'vacation_mode_required' });
  }

  let vacationMode;
  try {
    vacationMode = parseBooleanInput(req.body.vacation_mode, null, 'vacation_mode');
    if (vacationMode === null) {
      throw invalidInputError('invalid_boolean_vacation_mode');
    }
  } catch (error) {
    return res.status(error.status || 400).json({ error: error.message || 'invalid_boolean_vacation_mode' });
  }

  const isOwner = req.user && Number(req.user.id) === professionalId;
  const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
  if (!isOwner && !isStaff) {
    return res.status(403).json({ error: 'not_authorized' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();

    const [[professional]] = await connection.query(
      'SELECT id, is_professional FROM user_account WHERE id = ? FOR UPDATE',
      [professionalId]
    );

    if (!professional) {
      await connection.rollback();
      return res.status(404).json({ error: 'professional_not_found' });
    }

    if (!professional.is_professional) {
      await connection.rollback();
      return res.status(400).json({ error: 'user_not_professional' });
    }

    await connection.query(
      'UPDATE user_account SET vacation_mode = ? WHERE id = ?',
      [vacationMode ? 1 : 0, professionalId]
    );

    const [result] = await connection.query(
      'UPDATE service SET is_hidden = ?, last_edit_datetime = NOW() WHERE user_id = ?',
      [vacationMode ? 1 : 0, professionalId]
    );

    await connection.commit();

    return res.status(200).json({
      message: vacationMode
        ? 'Servicios del profesional ocultados (modo vacaciones activado).'
        : 'Servicios del profesional visibles (modo vacaciones desactivado).',
      affected_services: result.affectedRows,
      vacation_mode: vacationMode
    });
  } catch (error) {
    try {
      await connection.rollback();
    } catch (rollbackError) {
      console.error('Error al revertir la transacción de modo vacaciones:', rollbackError);
    }
    console.error('Error al actualizar el modo vacaciones del profesional:', error);
    return res.status(500).json({ error: 'error_updating_vacation_mode' });
  } finally {
    connection.release();
  }
});

//Ruta para actualizar un servicio
app.put('/api/services/:id', async (req, res) => {
  const serviceId = Number(req.params.id);
  console.log('[TEMP-DEBUG] Actualizar servicio - parámetros recibidos:', {
    serviceId,
    body: req.body,
    userId: req.user && req.user.id
  });
  if (!Number.isInteger(serviceId) || serviceId <= 0) {
    return res.status(400).json({ error: 'invalid_service_id' });
  }

  const body = req.body || {};
  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();
    const ownership = await fetchOwnedService(connection, serviceId, req.user.id, {
      lock: true,
      includePrice: true,
      includeConsult: true
    });

    if (ownership.error) {
      await connection.rollback();
      const mapped = mapOwnershipError(ownership.error);
      return res.status(mapped.status).json(mapped.body);
    }

    const service = ownership.service;

    let serviceTitle = body.service_title !== undefined ? String(body.service_title).trim() : service.service_title;
    if (!serviceTitle) {
      await connection.rollback();
      return res.status(400).json({ error: 'invalid_service_title' });
    }

    let description = body.description !== undefined ? body.description : service.description;
    if (description === undefined) {
      description = null;
    } else if (description !== null) {
      description = String(description);
    }

    let serviceCategoryId;
    let priceValue;
    let priceType;
    let latitude;
    let longitude;
    let actionRate;
    let userCanAsk;
    let userCanConsult;
    let priceConsult;
    let isIndividual;
    let allowDiscounts;
    let discountRate;
    let experienceYears;
    let minimumNoticePolicy;
    let hobbies;
    let isHidden;
    let consultProvider;
    let consultUsername;
    let consultUrl;

    try {
      serviceCategoryId = parseIntegerInput(
        body.service_category_id,
        service.service_category_id,
        'service_category_id'
      );

      // === NUEVO ORDEN Y REGLAS ===
      // 1) price_type primero
      priceType = (body.price_type !== undefined && body.price_type !== null)
        ? String(body.price_type).trim().toLowerCase()
        : (service.current_price_type || 'hour');

      if (!['hour', 'fix', 'budget'].includes(priceType)) {
        throw invalidInputError('invalid_price_type');
      }

      // 2) price según el tipo
      const allowNullPrice = (priceType === 'budget');
      const defaultWhenRequired = allowNullPrice ? null : service.current_price;

      // Si no es budget y no viene ni en body ni en la BD, error explícito
      if (!allowNullPrice && (body.price === undefined) && (defaultWhenRequired === null)) {
        throw invalidInputError('invalid_number_price');
      }

      priceValue = parseNumberInput(
        body.price,
        defaultWhenRequired,
        'price',
        { allowNull: allowNullPrice }
      );

      // Invariantes
      if (priceType !== 'budget' && priceValue === null) {
        throw invalidInputError('invalid_number_price');
      }
      if (priceValue !== null && Number(priceValue) < 0) {
        throw invalidInputError('invalid_number_price');
      }

      // resto de parseos como ya tenías
      latitude = parseNumberInput(body.latitude, service.latitude, 'latitude');
      longitude = parseNumberInput(body.longitude, service.longitude, 'longitude');
      actionRate = parseNumberInput(body.action_rate, service.action_rate, 'action_rate', { allowNull: false });
      userCanAsk = parseBooleanInput(body.user_can_ask, Boolean(service.user_can_ask), 'user_can_ask');
      userCanConsult = parseBooleanInput(body.user_can_consult, Boolean(service.user_can_consult), 'user_can_consult');
      priceConsult = parseNumberInput(body.price_consult, service.price_consult, 'price_consult');
      isIndividual = parseBooleanInput(body.is_individual, Boolean(service.is_individual), 'is_individual');
      allowDiscounts = parseBooleanInput(body.allow_discounts, Boolean(service.allow_discounts), 'allow_discounts');
      discountRate = parseNumberInput(body.discount_rate, service.discount_rate, 'discount_rate');
      experienceYears = parseExperienceYearsInput(body.experience_years, service.experience_years ?? 1);
      minimumNoticePolicy = parseMinimumNoticePolicyInput(body.minimum_notice_policy, service.minimum_notice_policy ?? 1440);
      hobbies = body.hobbies !== undefined ? body.hobbies : service.hobbies;
      if (hobbies !== undefined && hobbies !== null) {
        hobbies = String(hobbies);
      }
      isHidden = parseBooleanInput(body.is_hidden, Boolean(service.is_hidden), 'is_hidden');
      consultProvider = body.consult_via_provide !== undefined ? body.consult_via_provide : service.consult_provider;
      consultUsername = body.consult_via_username !== undefined ? body.consult_via_username : service.consult_username;
      consultUrl = body.consult_via_url !== undefined ? body.consult_via_url : service.consult_url;
    } catch (parseError) {
      await connection.rollback();
      return res.status(parseError.status || 400).json({ error: parseError.message || 'invalid_service_payload' });
    }

    await connection.query(
      'UPDATE price SET price = ?, price_type = ? WHERE id = ?',
      [priceType === 'budget' ? null : priceValue, priceType, service.price_id]
    );

    let consultViaId = service.consult_via_id;
    if (userCanConsult) {
      if (consultViaId) {
        await connection.query('UPDATE consult_via SET provider = ?, username = ?, url = ? WHERE id = ?', [consultProvider || null, consultUsername || null, consultUrl || null, consultViaId]);
      } else {
        const [insertConsult] = await connection.query('INSERT INTO consult_via (provider, username, url) VALUES (?, ?, ?)', [consultProvider || null, consultUsername || null, consultUrl || null]);
        consultViaId = insertConsult.insertId;
      }
    } else if (consultViaId) {
      await connection.query('DELETE FROM consult_via WHERE id = ?', [consultViaId]);
      consultViaId = null;
    }

    await connection.query(
      `UPDATE service
          SET service_title = ?,
              description = ?,
              service_category_id = ?,
              latitude = ?,
              longitude = ?,
              action_rate = ?,
              user_can_ask = ?,
              user_can_consult = ?,
              price_consult = ?,
              consult_via_id = ?,
              is_individual = ?,
              minimum_notice_policy = ?,
              allow_discounts = ?,
              discount_rate = ?,
              experience_years = ?,
              hobbies = ?,
              is_hidden = ?,
              last_edit_datetime = NOW()
        WHERE id = ?`,
      [
        serviceTitle,
        description,
        serviceCategoryId,
        latitude,
        longitude,
        actionRate,
        userCanAsk ? 1 : 0,
        userCanConsult ? 1 : 0,
        priceConsult,
        consultViaId,
        isIndividual ? 1 : 0,
        minimumNoticePolicy,
        allowDiscounts ? 1 : 0,
        discountRate,
        experienceYears,
        hobbies,
        isHidden ? 1 : 0,
        serviceId
      ]
    );

    if (body.languages !== undefined) {
      if (!Array.isArray(body.languages)) {
        await connection.rollback();
        return res.status(400).json({ error: 'invalid_languages' });
      }
      await connection.query('DELETE FROM service_language WHERE service_id = ?', [serviceId]);
      if (body.languages.length > 0) {
        const languageValues = body.languages.map(lang => {
          if (lang === null || lang === undefined) {
            throw invalidInputError('invalid_language_value');
          }
          return [serviceId, String(lang)];
        });
        await connection.query('INSERT INTO service_language (service_id, language) VALUES ?', [languageValues]);
      }
    }

    if (body.tags !== undefined) {
      if (!Array.isArray(body.tags)) {
        await connection.rollback();
        return res.status(400).json({ error: 'invalid_tags' });
      }
      await connection.query('DELETE FROM service_tags WHERE service_id = ?', [serviceId]);
      if (body.tags.length > 0) {
        const tagsValues = body.tags.map(tag => {
          if (tag === null || tag === undefined) {
            throw invalidInputError('invalid_tag_value');
          }
          return [serviceId, String(tag)];
        });
        await connection.query('INSERT INTO service_tags (service_id, tag) VALUES ?', [tagsValues]);
      }
    }

    if (body.experiences !== undefined) {
      if (!Array.isArray(body.experiences)) {
        await connection.rollback();
        return res.status(400).json({ error: 'invalid_experiences' });
      }
      await connection.query('DELETE FROM experience_place WHERE service_id = ?', [serviceId]);
      if (body.experiences.length > 0) {
        const experienceValues = body.experiences.map(exp => {
          if (!exp || typeof exp !== 'object') {
            throw invalidInputError('invalid_experience_value');
          }
          return [
            serviceId,
            exp.experience_title || null,
            exp.place_name || null,
            toMySQLDatetime(exp.experience_started_date),
            toMySQLDatetime(exp.experience_end_date)
          ];
        });
        await connection.query(
          'INSERT INTO experience_place (service_id, experience_title, place_name, experience_started_date, experience_end_date) VALUES ?',
          [experienceValues]
        );
      }
    }

    let imagesToDeleteFromBucket = [];

    if (body.images !== undefined) {
      if (!Array.isArray(body.images)) {
        await connection.rollback();
        return res.status(400).json({ error: 'invalid_images' });
      }
      const [existingImages] = await connection.query('SELECT image_url, object_name FROM service_image WHERE service_id = ?', [serviceId]);
      const existingObjectNames = existingImages
        .map(row => resolveImageObjectName(row?.object_name, row?.image_url, bucketName))
        .filter(name => typeof name === 'string' && name.length > 0);
      const newObjectNames = new Set(
        body.images
          .map(img => {
            if (!img || typeof img !== 'object') {
              return null;
            }
            const imageUrl = resolveImageUrl(img);
            return resolveImageObjectName(
              img.object_name ?? img.objectName,
              imageUrl,
              bucketName
            );
          })
          .filter(Boolean)
      );
      imagesToDeleteFromBucket = existingObjectNames.filter(objectName => !newObjectNames.has(objectName));
      await connection.query('DELETE FROM service_image WHERE service_id = ?', [serviceId]);
      if (body.images.length > 0) {
        const imageValues = body.images.map(img => {
          if (!img || typeof img !== 'object') {
            throw invalidInputError('invalid_image_value');
          }
          const imageUrl = resolveImageUrl(img);
          if (!imageUrl) {
            throw invalidInputError('invalid_image_value');
          }
          const orderValue = img.order === undefined ? null : parseNumberInput(img.order, null, 'image_order', { allowNull: true, integer: true });
          return [
            serviceId,
            imageUrl,
            resolveImageObjectName(img.object_name ?? img.objectName, imageUrl, bucketName),
            orderValue
          ];
        });
        await connection.query('INSERT INTO service_image (service_id, image_url, object_name, `order`) VALUES ?', [imageValues]);
      }
    }

    await connection.commit();

    if (imagesToDeleteFromBucket.length > 0) {
      await Promise.all(
        imagesToDeleteFromBucket.map(async objectName => {
          try {
            await bucket.file(objectName).delete({ ignoreNotFound: true });
          } catch (deleteError) {
            if (deleteError.code !== 404) {
              console.error(`Error al eliminar la imagen ${objectName} del bucket:`, deleteError);
            }
          }
        })
      );
    }

    return res.status(200).json({ message: 'Servicio actualizado correctamente.' });
  } catch (error) {
    await connection.rollback();
    if (error.status === 400) {
      return res.status(400).json({ error: error.message });
    }
    console.error('Error al actualizar el servicio:', error);
    return res.status(500).json({ error: 'Error al actualizar el servicio.' });
  } finally {
    connection.release();
  }
});

//Ruta para crear lista
app.post('/api/lists', (req, res) => {
  const { user_id, list_name } = req.body;

  if (!user_id || !list_name) {
    return res.status(400).json({ error: 'user_id y list_name son requeridos.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'INSERT INTO service_list (user_id, list_name) VALUES (?, ?)';
    const values = [user_id, list_name];

    connection.query(query, values, (err, result) => {
      connection.release(); // Libera la conexión después de usarla

      if (err) {
        console.error('Error al crear la lista:', err);
        return res.status(500).json({ error: 'Error al crear la lista.' });
      }

      res.status(201).json({ message: 'Lista creada con éxito', listId: result.insertId });
    });
  });
});

//Ruta para añadir un item a una lista
app.post('/api/lists/:list_id/items', (req, res) => {
  const { list_id } = req.params;
  const { service_id } = req.body;

  if (!service_id) {
    return res.status(400).json({ error: 'service_id es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Comprobar si ya existe un item con el mismo service_id en la lista
    const checkIfExistsQuery = 'SELECT id FROM item_list WHERE list_id = ? AND service_id = ?';
    connection.query(checkIfExistsQuery, [list_id, service_id], (err, results) => {
      if (err) {
        connection.release();
        console.error('Error al comprobar si el item ya existe:', err);
        return res.status(500).json({ error: 'Error al comprobar si el item ya existe.' });
      }

      // Si ya existe, no añadir el nuevo item
      if (results.length > 0) {
        connection.release();
        return res.status(201).json({ message: 'El item ya existe en la lista.', alreadyExists: true });
      }

      // Si no existe, proceder con la inserción
      const getLastOrderQuery = 'SELECT MAX(`order`) AS lastOrder FROM item_list WHERE list_id = ?';
      connection.query(getLastOrderQuery, [list_id], (err, result) => {
        if (err) {
          connection.release();
          console.error('Error al obtener el último orden:', err);
          return res.status(500).json({ error: 'Error al obtener el último orden.' });
        }

        const lastOrder = result[0].lastOrder || 0;
        const newOrder = lastOrder + 1;

        const insertItemQuery = `
          INSERT INTO item_list (list_id, service_id, \`order\`, added_datetime) 
          VALUES (?, ?, ?, NOW())
        `;
        const values = [list_id, service_id, newOrder];

        connection.query(insertItemQuery, values, (err, result) => {
          connection.release();

          if (err) {
            console.error('Error al añadir el item a la lista:', err);
            return res.status(500).json({ error: 'Error al añadir el item a la lista.' });
          }

          res.status(201).json({ message: 'Item añadido con éxito', itemId: result.insertId });
        });
      });
    });
  });
});

// Ruta para eliminar un item de una lista por su ID
app.delete('/api/lists/:list_id/items/:item_id', (req, res) => {
  const { list_id, item_id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const deleteQuery = 'DELETE FROM item_list WHERE id = ? AND list_id = ?';

    connection.query(deleteQuery, [item_id, list_id], (err, result) => {
      connection.release();

      if (err) {
        console.error('Error al eliminar el item de la lista:', err);
        return res.status(500).json({ error: 'Error al eliminar el item de la lista.' });
      }

      if (result.affectedRows === 0) {
        return res.status(404).json({ error: 'Item no encontrado en la lista.' });
      }

      return res.status(200).json({ message: 'Item eliminado con éxito.' });
    });
  });
});

//Ruta para obtener toda la info de un servicio y mostrar su profile
app.get('/api/service/:id', (req, res) => {
  const { id } = req.params; // ID del servicio

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información del servicio y sus relaciones
    const query = `
      SELECT 
        s.id AS service_id,
        s.service_title,
        s.description,
        s.service_category_id,
        s.price_id,
        s.latitude,
        s.longitude,
        s.action_rate,
        s.user_can_ask,
        s.user_can_consult,
        s.price_consult,
        s.consult_via_id,
        s.is_individual,
        s.minimum_notice_policy,
        s.is_hidden,
        s.service_created_datetime,
        s.last_edit_datetime,
        s.allow_discounts,
        s.discount_rate,
        s.hobbies,
        s.experience_years,
        p.price,
        p.currency AS currency,
        p.price_type,
        ua.id AS user_id,
        ua.email,
        ua.phone,
        ua.username,
        ua.first_name,
        ua.surname,
        ua.profile_picture,
        ua.is_professional,
        ua.is_verified,
        CASE
          WHEN ua.stripe_account_id IS NOT NULL AND ua.stripe_account_id LIKE 'acct_%' THEN 1
          ELSE 0
        END AS has_payout_method,
        ua.language,
        -- Subconsulta para obtener los tags del servicio
        (SELECT JSON_ARRAYAGG(tag) 
         FROM service_tags 
         WHERE service_id = s.id) AS tags,
        -- Subconsulta para obtener los idiomas del servicio
        (SELECT JSON_ARRAYAGG(language) 
         FROM service_language 
         WHERE service_id = s.id) AS languages,
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'object_name', si.object_name, 'order', si.order))
         FROM service_image si 
         WHERE si.service_id = s.id) AS images,
        -- Subconsulta para obtener las reseñas del servicio con información del usuario
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', r.id, 
            'user_id', r.user_id, 
            'rating', r.rating, 
            'comment', r.comment, 
            'review_datetime', r.review_datetime,
            'user', JSON_OBJECT('id', ua_r.id,
                                'email', ua_r.email,
                                'phone', ua_r.phone,
                                'username', ua_r.username,
                                'first_name', ua_r.first_name,
                                'surname', ua_r.surname,
                                'profile_picture', ua_r.profile_picture))
         )
         FROM review r 
         JOIN user_account ua_r ON r.user_id = ua_r.id
         WHERE r.service_id = s.id) AS reviews,
        -- Calcular la media de valoraciones
        (SELECT AVG(r.rating) 
         FROM review r 
         WHERE r.service_id = s.id) AS average_rating,
        -- Contar el número total de reseñas
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id) AS review_count,
        -- Contar el número de reseñas por rating
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 5) AS rating_5_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 4) AS rating_4_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 3) AS rating_3_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 2) AS rating_2_count,
        (SELECT COUNT(*) 
         FROM review r 
         WHERE r.service_id = s.id AND r.rating = 1) AS rating_1_count,
        -- Subconsulta para obtener las experiencias del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', ep.id, 
            'experience_title', ep.experience_title, 
            'place_name', ep.place_name, 
            'experience_started_date', ep.experience_started_date, 
            'experience_end_date', ep.experience_end_date))
         FROM experience_place ep
         WHERE ep.service_id = s.id) AS experiences,
        -- Información de consult_via
        CASE
          WHEN cv.id IS NOT NULL THEN JSON_OBJECT(
            'id', cv.id,
            'provider', cv.provider,
            'username', cv.username,
            'url', cv.url
          )
          ELSE NULL
        END AS consult_via,
        -- Información de la categoría del servicio
        (SELECT JSON_OBJECT('id', sc.id,
          'service_category_id', sc.id,
          'category_key', sct.category_key,
          'name', sct.category_key,
          'family', JSON_OBJECT('id', sf.id, 'family_key', sf.family_key, 'name', sf.family_key))
         FROM service_category sc
         JOIN service_family sf ON sc.service_family_id = sf.id
         JOIN service_category_type sct ON sc.service_category_type_id = sct.id
         WHERE sc.id = s.service_category_id) AS category,
        -- Métricas solicitadas
        (SELECT COUNT(*)
         FROM booking b
         WHERE b.service_id = s.id
           AND b.service_status IN ('accepted', 'in_progress', 'finished')) AS confirmed_booking_count,
        (SELECT COALESCE(SUM(completed_count) - COUNT(*), 0)
         FROM (
           SELECT COUNT(*) AS completed_count
           FROM booking b
           WHERE b.service_id = s.id
             AND b.service_status = 'finished'
           GROUP BY b.client_user_id
         ) AS completed_by_user) AS repeated_bookings_count,
        (SELECT COALESCE(SUM(
            COALESCE(cp.proposed_total_amount_cents, b.estimated_total_amount_cents, 0)
            - COALESCE(cp.proposed_commission_amount_cents, b.estimated_commission_amount_cents, 0)
          ), 0) / 100
         FROM booking b
         LEFT JOIN booking_closure_proposal cp ON cp.id = (
           SELECT cp2.id
           FROM booking_closure_proposal cp2
           WHERE cp2.booking_id = b.id
           ORDER BY cp2.id DESC
           LIMIT 1
         )
         WHERE b.service_id = s.id
           AND b.service_status = 'finished') AS total_earned_amount,
        (SELECT COUNT(DISTINCT sl.user_id)
         FROM item_list il
         JOIN service_list sl ON il.list_id = sl.id
         WHERE il.service_id = s.id) AS likes_count,
        (SELECT ROUND(COALESCE(SUM(
             CASE
               WHEN b.requested_duration_minutes IS NOT NULL THEN b.requested_duration_minutes
               WHEN b.requested_start_datetime IS NOT NULL AND b.requested_end_datetime IS NOT NULL
                 THEN GREATEST(TIMESTAMPDIFF(MINUTE, b.requested_start_datetime, b.requested_end_datetime), 0)
               ELSE 0
             END
           ), 0) / 60, 2)
         FROM booking b
         WHERE b.service_id = s.id
           AND b.service_status = 'finished') AS total_hours_completed
      FROM service s
      JOIN price p ON s.price_id = p.id
      JOIN user_account ua ON s.user_id = ua.id
      LEFT JOIN consult_via cv ON cv.id = s.consult_via_id
      WHERE s.id = ?;
    `;

    connection.query(query, [id], async (err, serviceData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información del servicio:', err);
        res.status(500).json({ error: 'Error al obtener la información del servicio.' });
        return;
      }

      if (serviceData.length > 0) {
        const service = serviceData[0];

        if (typeof service.consult_via === 'string') {
          try {
            service.consult_via = JSON.parse(service.consult_via);
          } catch (parseError) {
            console.error('Error parsing consult_via JSON:', parseError);
            service.consult_via = null;
          }
        }

        const viewerIdParam = req.query.viewerId ?? req.query.viewer_id ?? req.query.userId ?? req.query.user_id;
        const viewerId = viewerIdParam !== undefined
          ? Number(viewerIdParam)
          : req.user?.id !== undefined
            ? Number(req.user.id)
            : undefined;
        const isOwner = viewerId !== undefined && !Number.isNaN(viewerId) && viewerId === service.user_id;

        if (service.is_hidden && !isOwner) {
          return res.status(404).json({ notFound: true, message: 'Servicio no disponible.' });
        }

        try {
          const responseTimeResult = await computeServiceResponseTime({
            serviceId: service.service_id,
            professionalId: service.user_id,
            pool,
          });
          service.response_time_minutes = responseTimeResult?.value ?? null;
        } catch (metricError) {
          console.error('Error calculating service response time metric:', metricError);
          service.response_time_minutes = null;
        }

        try {
          const successResult = await computeServiceSuccessRate({
            serviceId: service.service_id,
            categoryId: service.service_category_id,
            responseTimeMinutes: service.response_time_minutes,
            pool,
          });
          service.success_rate = successResult?.value ?? null;
        } catch (successError) {
          console.error('Error calculating service success rate metric:', successError);
          service.success_rate = null;
        }

        let isLiked = false;
        if (Number.isFinite(viewerId)) {
          try {
            const likedQuery = `
              SELECT 1
              FROM item_list il
              JOIN service_list sl ON il.list_id = sl.id
              LEFT JOIN shared_list sh ON sh.list_id = il.list_id
              WHERE il.service_id = ? AND (sl.user_id = ? OR sh.user_id = ?)
              LIMIT 1
            `;
            const [likedRows] = await promisePool.query(
              likedQuery,
              [service.service_id, viewerId, viewerId]
            );
            isLiked = likedRows.length > 0;
          } catch (likedError) {
            console.error('Error checking liked status:', likedError);
          }
        }

        service.is_liked = isLiked;

        res.status(200).json(service); // Devolver la información del servicio
      } else {
        res.status(404).json({ notFound: true, message: 'Servicio no encontrado.' });
      }
    });
  });
});

app.get('/api/services/:id/pre-booking-chat-status', authenticateToken, async (req, res) => {
  const serviceId = normalizeNullableInteger(req.params.id);
  const requesterUserId = normalizeNullableInteger(req.user?.id);

  if (!serviceId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  if (!requesterUserId) {
    return res.status(401).json({ error: 'No autenticado.' });
  }

  const connection = await promisePool.getConnection();
  try {
    const [[serviceRow]] = await connection.query(
      `
      SELECT id, user_id
      FROM service
      WHERE id = ?
      LIMIT 1
      `,
      [serviceId]
    );

    if (!serviceRow) {
      return res.status(404).json({ error: 'Servicio no encontrado.' });
    }

    if (Number(serviceRow.user_id) === requesterUserId) {
      return res.status(200).json({ unlocked: true, booking_id: null });
    }

    const [[bookingRow]] = await connection.query(
      `
      SELECT id
      FROM booking
      WHERE service_id = ?
        AND client_user_id = ?
        AND provider_user_id_snapshot = ?
        AND service_status IN ('accepted', 'in_progress', 'finished')
      ORDER BY id DESC
      LIMIT 1
      `,
      [serviceId, requesterUserId, serviceRow.user_id]
    );

    return res.status(200).json({
      unlocked: Boolean(bookingRow),
      booking_id: bookingRow?.id ?? null,
    });
  } catch (error) {
    console.error('Error resolving pre-booking chat status:', error);
    return res.status(500).json({ error: 'Error al comprobar el estado del chat previo.' });
  } finally {
    connection.release();
  }
});

//Ruta para obtener los 10 profesionales de la tabla
app.get('/api/suggested_professional', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      SELECT
        ua.id AS user_id,
        ua.email, ua.phone, ua.username, ua.first_name, ua.surname,
        ua.profile_picture, ua.language,
        COALESCE(rs.average_rating, 0) AS average_rating,
        COALESCE(bu.booking_count, 0)  AS booking_count,
        bs.best_service_id,
        bs.best_service_title
      FROM user_account ua
      LEFT JOIN (
        SELECT s.user_id, AVG(r.rating) AS average_rating
        FROM review r
        JOIN service s ON r.service_id = s.id
        WHERE s.is_hidden = 0
        GROUP BY s.user_id
      ) rs ON ua.id = rs.user_id
      LEFT JOIN (
        SELECT s.user_id, COUNT(b.id) AS booking_count
        FROM booking b
        JOIN service s ON b.service_id = s.id
        WHERE s.is_hidden = 0
        /* opcional: WHERE b.status IN ('confirmed','completed') */
        GROUP BY s.user_id
      ) bu ON ua.id = bu.user_id
      LEFT JOIN (
        /* mejor servicio por usuario según (avg_rating_serv+1)*(bookings_serv+1) */
        SELECT user_id, best_service_id, best_service_title
        FROM (
          SELECT
            s.user_id,
            s.id AS best_service_id,
            s.service_title AS best_service_title,
            ( (COALESCE(r_by_s.avg_rating_service,0) + 1)
              * (COALESCE(b_by_s.booking_count_service,0) + 1) ) AS service_weight,
            ROW_NUMBER() OVER (
              PARTITION BY s.user_id
              ORDER BY
                ( (COALESCE(r_by_s.avg_rating_service,0) + 1)
                  * (COALESCE(b_by_s.booking_count_service,0) + 1) ) DESC,
                COALESCE(r_by_s.avg_rating_service,0) DESC,
                COALESCE(b_by_s.booking_count_service,0) DESC,
                s.id DESC
            ) AS rn
          FROM service s
          LEFT JOIN (
            SELECT service_id, AVG(rating) AS avg_rating_service
            FROM review
            GROUP BY service_id
          ) r_by_s ON r_by_s.service_id = s.id
          LEFT JOIN (
            SELECT service_id, COUNT(*) AS booking_count_service
            FROM booking
            /* opcional: WHERE status IN ('confirmed','completed') */
            GROUP BY service_id
          ) b_by_s ON b_by_s.service_id = s.id
          WHERE s.is_hidden = 0
          /* opcional: WHERE s.status='published' AND s.is_active=1 */
        ) ranked
        WHERE rn = 1
      ) bs ON ua.id = bs.user_id
      WHERE ua.is_professional = 1
        AND EXISTS (
          SELECT 1
          FROM service s_exist
          WHERE s_exist.user_id = ua.id
            AND s_exist.is_hidden = 0
        );
    `;

    connection.query(query, (err, pros) => {
      connection.release();
      if (err) {
        console.error('Error al obtener profesionales:', err);
        return res.status(500).json({ error: 'Error al obtener los profesionales.' });
      }

      if (!pros || pros.length === 0) return res.status(200).json([]);

      // Ponderación: multiplica la influencia de rating y de reservas.
      // Puedes ajustar a y ß según lo que quieras priorizar.
      const alpha = 2; // peso del rating
      const beta = 1; // peso de reservas

      const items = pros.map(p => {
        const rating = Math.max(0, Math.min(5, Number(p.average_rating) || 0));
        const bookings = Math.max(0, Number(p.booking_count) || 0);
        // Suavizado para no dejar a cero a quien no tiene datos:
        const weight = (0.5 + rating / 5) ** alpha * (1 + bookings) ** beta;
        return { ...p, weight };
      });

      // Si todos tienen peso 0 (muy raro), usa peso uniforme
      const allZero = items.every(i => i.weight === 0);
      if (allZero) items.forEach(i => (i.weight = 1));

      const k = Math.min(20, items.length);
      const selected = weightedSampleWithoutReplacement(items, k).map(({ weight, ...rest }) => rest);

      return res.status(200).json(selected);
    });
  });
});

//Ruta para obtener todas las reservas de un user
app.get('/api/user/:userId/bookings', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res, req.params.userId);
  if (!requestedUserId) return;

  try {
    const { status } = req.query;
    const statusFilter = buildBookingStatusFilter(status, 'b');
    const bookingsQuery = `
      SELECT
        b.id AS booking_id,
        b.id,
        b.client_user_id,
        b.provider_user_id_snapshot,
        b.service_id,
        b.address_id,
        b.description,
        b.service_status,
        b.settlement_status,
        b.order_datetime AS created_at,
        b.order_datetime,
        b.updated_at,
        b.requested_start_datetime,
        b.requested_duration_minutes,
        b.requested_end_datetime,
        b.deposit_confirmed_at,
        b.accepted_at,
        b.started_at,
        b.finished_at,
        b.canceled_at,
        b.expired_at,
        b.accept_deadline_at,
        b.expires_at,
        b.client_approval_deadline_at,
        b.last_minute_window_starts_at,
        b.canceled_by_user_id,
        b.cancellation_reason_code,
        b.cancellation_note,
        b.service_title_snapshot,
        b.price_type_snapshot,
        b.service_currency_snapshot,
        b.unit_price_amount_cents_snapshot,
        b.minimum_notice_policy_snapshot,
        b.estimated_base_amount_cents,
        b.estimated_commission_amount_cents,
        b.estimated_total_amount_cents,
        b.deposit_amount_cents_snapshot,
        b.deposit_currency_snapshot,
        cp.id AS closure_proposal_id,
        cp.proposed_base_amount_cents,
        cp.proposed_commission_amount_cents,
        cp.proposed_total_amount_cents,
        cp.proposed_final_duration_minutes,
        cp.status AS closure_status,
        cp.deposit_already_paid_amount_cents,
        cp.amount_due_from_client_cents,
        cp.amount_to_refund_cents,
        cp.provider_payout_amount_cents,
        cp.platform_amount_cents,
        cp.zero_charge_mode,
        cp.auto_charge_eligible,
        cp.auto_charge_scheduled_at,
        cp.sent_at AS closure_sent_at,
        cp.accepted_at AS closure_accepted_at,
        cp.rejected_at AS closure_rejected_at,
        cp.revoked_at AS closure_revoked_at,
        s.id AS service_id,
        COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
        s.description,
        s.service_category_id,
        s.price_id,
        s.latitude,
        s.longitude,
        s.action_rate,
        s.user_can_ask,
        s.user_can_consult,
        s.price_consult,
        s.consult_via_id,
        s.is_individual,
        s.is_hidden,
        s.service_created_datetime,
        s.last_edit_datetime,
        p.price,
        COALESCE(p.currency, b.service_currency_snapshot) AS currency,
        COALESCE(p.price_type, b.price_type_snapshot) AS price_type,
        su.id AS service_user_id,
        su.email,
        su.phone,
        su.username,
        su.first_name,
        su.surname,
        su.profile_picture,
        su.is_professional,
        su.language,
        (
          SELECT JSON_ARRAYAGG(
            JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'object_name', si.object_name, 'order', si.order)
          )
          FROM service_image si
          WHERE si.service_id = s.id
        ) AS images
      FROM booking b
      LEFT JOIN service s ON b.service_id = s.id
      LEFT JOIN price p ON s.price_id = p.id
      LEFT JOIN user_account su ON COALESCE(s.user_id, b.provider_user_id_snapshot) = su.id
      LEFT JOIN booking_closure_proposal cp ON cp.id = (
        SELECT cp2.id
        FROM booking_closure_proposal cp2
        WHERE cp2.booking_id = b.id
        ORDER BY cp2.id DESC
        LIMIT 1
      )
      WHERE b.client_user_id = ?${statusFilter.clause}
      ORDER BY b.order_datetime DESC
      `;
    let [bookingRows] = await promisePool.query(
      bookingsQuery,
      [requestedUserId, ...statusFilter.params]
    );

    const expiredBookingIds = bookingRows
      .filter((booking) => canAutoExpireRequestedBooking(booking))
      .map((booking) => booking.id);
    if (expiredBookingIds.length > 0) {
      for (const expiredBookingId of expiredBookingIds) {
        await expireRequestedBookingByIdIfNeeded(expiredBookingId);
      }
      [bookingRows] = await promisePool.query(
        bookingsQuery,
        [requestedUserId, ...statusFilter.params]
      );
    }

    return res.status(200).json(bookingRows.map(mapBookingRecordForApi));
  } catch (error) {
    console.error('Error al obtener la información de las reservas:', error);
    return res.status(500).json({ error: 'Error al obtener la información de las reservas.' });
  }
});

//Ruta para obtener todas las reservas de un profesional
app.get('/api/service-user/:userId/bookings', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res, req.params.userId);
  if (!requestedUserId) return;

  try {
    const { status } = req.query;
    const statusFilter = buildBookingStatusFilter(status, 'b');
    const bookingsQuery = `
      SELECT
        b.id AS booking_id,
        b.id,
        b.client_user_id,
        b.provider_user_id_snapshot,
        b.service_id,
        b.address_id,
        b.description,
        b.service_status,
        b.settlement_status,
        b.order_datetime AS created_at,
        b.order_datetime,
        b.updated_at,
        b.requested_start_datetime,
        b.requested_duration_minutes,
        b.requested_end_datetime,
        b.deposit_confirmed_at,
        b.accepted_at,
        b.started_at,
        b.finished_at,
        b.canceled_at,
        b.expired_at,
        b.accept_deadline_at,
        b.expires_at,
        b.client_approval_deadline_at,
        b.last_minute_window_starts_at,
        b.canceled_by_user_id,
        b.cancellation_reason_code,
        b.cancellation_note,
        b.service_title_snapshot,
        b.price_type_snapshot,
        b.service_currency_snapshot,
        b.unit_price_amount_cents_snapshot,
        b.minimum_notice_policy_snapshot,
        b.estimated_base_amount_cents,
        b.estimated_commission_amount_cents,
        b.estimated_total_amount_cents,
        b.deposit_amount_cents_snapshot,
        b.deposit_currency_snapshot,
        cp.id AS closure_proposal_id,
        cp.proposed_base_amount_cents,
        cp.proposed_commission_amount_cents,
        cp.proposed_total_amount_cents,
        cp.proposed_final_duration_minutes,
        cp.status AS closure_status,
        cp.deposit_already_paid_amount_cents,
        cp.amount_due_from_client_cents,
        cp.amount_to_refund_cents,
        cp.provider_payout_amount_cents,
        cp.platform_amount_cents,
        cp.zero_charge_mode,
        cp.sent_at AS closure_sent_at,
        cp.accepted_at AS closure_accepted_at,
        cp.rejected_at AS closure_rejected_at,
        cp.revoked_at AS closure_revoked_at,
        s.id AS service_id,
        COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
        s.description,
        s.service_category_id,
        s.price_id,
        s.latitude,
        s.longitude,
        s.action_rate,
        s.user_can_ask,
        s.user_can_consult,
        s.price_consult,
        s.consult_via_id,
        s.is_individual,
        s.experience_years,
        s.is_hidden,
        s.service_created_datetime,
        s.last_edit_datetime,
        p.price,
        COALESCE(p.currency, b.service_currency_snapshot) AS currency,
        COALESCE(p.price_type, b.price_type_snapshot) AS price_type,
        su.id AS service_user_id,
        su.email AS service_user_email,
        su.phone AS service_user_phone,
        su.username AS service_user_username,
        su.first_name AS service_user_first_name,
        su.surname AS service_user_surname,
        su.profile_picture AS service_user_profile_picture,
        su.is_professional AS service_user_is_professional,
        su.language AS service_user_language,
        bu.id AS booking_user_id,
        bu.email AS booking_user_email,
        bu.phone AS booking_user_phone,
        bu.username AS booking_user_username,
        bu.first_name AS booking_user_first_name,
        bu.surname AS booking_user_surname,
        bu.profile_picture AS booking_user_profile_picture,
        bu.is_professional AS booking_user_is_professional,
        bu.language AS booking_user_language,
        (
          SELECT JSON_ARRAYAGG(
            JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'object_name', si.object_name, 'order', si.order)
          )
          FROM service_image si
          WHERE si.service_id = s.id
        ) AS images
      FROM booking b
      LEFT JOIN service s ON b.service_id = s.id
      LEFT JOIN price p ON s.price_id = p.id
      LEFT JOIN user_account su ON COALESCE(s.user_id, b.provider_user_id_snapshot) = su.id
      LEFT JOIN user_account bu ON b.client_user_id = bu.id
      LEFT JOIN booking_closure_proposal cp ON cp.id = (
        SELECT cp2.id
        FROM booking_closure_proposal cp2
        WHERE cp2.booking_id = b.id
        ORDER BY cp2.id DESC
        LIMIT 1
      )
      WHERE COALESCE(s.user_id, b.provider_user_id_snapshot) = ?${statusFilter.clause}
      ORDER BY b.order_datetime DESC
      `;
    let [bookingRows] = await promisePool.query(
      bookingsQuery,
      [requestedUserId, ...statusFilter.params]
    );

    const expiredBookingIds = bookingRows
      .filter((booking) => canAutoExpireRequestedBooking(booking))
      .map((booking) => booking.id);
    if (expiredBookingIds.length > 0) {
      for (const expiredBookingId of expiredBookingIds) {
        await expireRequestedBookingByIdIfNeeded(expiredBookingId);
      }
      [bookingRows] = await promisePool.query(
        bookingsQuery,
        [requestedUserId, ...statusFilter.params]
      );
    }

    return res.status(200).json(bookingRows.map(mapBookingRecordForApi));
  } catch (error) {
    console.error('Error al obtener la información de las reservas:', error);
    return res.status(500).json({ error: 'Error al obtener la información de las reservas.' });
  }
});

//Ruta para mostrar todos los servicios de un profesional
app.get('/api/user/:id/services', authenticateToken, (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todos los servicios, sus tags y las imágenes
    const query = `
      SELECT 
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.minimum_notice_policy,
        service.is_hidden,
        service.service_created_datetime,
        service.last_edit_datetime,
        price.price,
        price.currency AS currency,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        -- Subconsulta para obtener los tags del servicio
        (SELECT JSON_ARRAYAGG(tag) 
         FROM service_tags 
         WHERE service_tags.service_id = service.id) AS tags,
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'object_name', si.object_name, 'order', si.order))
         FROM service_image si 
         WHERE si.service_id = service.id) AS images
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      LEFT JOIN (
        SELECT 
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.user_id = ?;
    `;

    connection.query(query, [requestedUserId], (err, servicesData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de los servicios:', err);
        res.status(500).json({ error: 'Error al obtener la información de los servicios.' });
        return;
      }

      if (servicesData.length > 0) {
        res.status(200).json(servicesData); // Devolver la lista de servicios con tags e imágenes
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron servicios para este usuario.' });
      }
    });
  });
});

//Ruta para obtener el dinero en wallet
app.get('/api/user/:id/wallet', authenticateToken, async (req, res) => {
  await ensureExchangeRatesFresh();
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener el valor de money_in_wallet
    const query = `
      SELECT money_in_wallet, currency
      FROM user_account
      WHERE id = ?;
    `;

    connection.query(query, [requestedUserId], (err, walletData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener el dinero en la cartera:', err);
        res.status(500).json({ error: 'Error al obtener el dinero en la cartera.' });
        return;
      }

      if (walletData.length > 0) {
        const userCurrency = normalizeCurrencyCode(walletData[0]?.currency, 'EUR');
        const rawWalletAmount = Number(walletData[0]?.money_in_wallet || 0);
        const convertedWalletAmount = convertAmount(rawWalletAmount, 'EUR', userCurrency);
        res.status(200).json({
          money_in_wallet: convertedWalletAmount,
          currency: userCurrency,
          base_currency: 'EUR',
          original_money_in_wallet: round2(rawWalletAmount),
        });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

//Ruta para obtener la información de un usuario
app.get('/api/user/:id', (req, res) => {
  const requestedUserId = parseRequestedUserId(req.params.id);
  if (!requestedUserId) {
    return res.status(400).json({ error: 'invalid_user_id' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    const query = `
      SELECT id, email, username, first_name, surname, phone, profile_picture,
             is_professional, language, joined_datetime
      FROM user_account
      WHERE id = ?;
    `;

    connection.query(query, [requestedUserId], (err, userData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información del usuario:', err);
        res.status(500).json({ error: 'Error al obtener la información del usuario.' });
        return;
      }

      if (userData.length > 0) {
        const userRecord = userData[0];
        const isOwner = req.user && Number(req.user.id) === requestedUserId;
        res.status(200).json(isOwner ? userRecord : buildPublicUserRecord(userRecord));
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

app.get('/api/user/:id/private', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  try {
    const user = await fetchSanitizedUserById(requestedUserId);

    if (!user) {
      return res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
    }

    return res.status(200).json(user);
  } catch (err) {
    console.error('Error al obtener la información privada del usuario:', err);
    return res.status(500).json({ error: 'Error al obtener la información privada del usuario.' });
  }
});

app.post('/api/user/:id/resend-verification-email', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  try {
    const [users] = await promisePool.query(
      'SELECT email, auth_provider, is_verified FROM user_account WHERE id = ? LIMIT 1',
      [requestedUserId]
    );

    if (users.length === 0) {
      return res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
    }

    const user = users[0];

    if (user.auth_provider !== 'email') {
      return res.status(409).json({
        error: 'VERIFICATION_NOT_AVAILABLE_FOR_PROVIDER',
        auth_provider: user.auth_provider,
      });
    }

    if (user.is_verified) {
      return res.status(200).json({ message: 'User already verified.' });
    }

    const verifyToken = jwt.sign({ id: requestedUserId }, process.env.JWT_SECRET, { expiresIn: '1d' });
    const url = process.env.BASE_URL + '/api/verify-email?token=' + verifyToken;

    await sendEmail({
      from: '"Wisdom" <wisdom.helpcontact@gmail.com>',
      to: user.email,
      subject: 'Confirm your Wisdom',
      html: `
            <!doctype html>
            <html lang="en" style="background:#ffffff;">
              <head>
                <meta charset="utf-8">
                <meta name="color-scheme" content="light only">
                <meta name="viewport" content="width=device-width,initial-scale=1">
                <title>Confirm your Wisdom</title>
              </head>
              <body style="margin:0;background:#ffffff;">
                <div style="max-width:640px;margin:0 auto;padding:48px 24px;font-family:Inter,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827;">
          
                  <div style="font-size:24px;font-weight:600;letter-spacing:.6px;margin-bottom:32px;text-align:center;">
                    WISDOM<sup style="font-size:12px;vertical-align:top;">®</sup>
                  </div>
          
                  <h1 style="font-size:30px;font-weight:500;margin:0 0 16px;text-align:center;">Welcome to Wisdom</h1>
          
                  <p style="font-size:16px;line-height:1.55;max-width:420px;margin:0 auto 32px;text-align:center;">
                    You've successfully sign up on Wisdom. Please confirm your email.
                  </p>
          
                  <div style="text-align:center;margin-bottom:50px;">
                    <a href="${url}"
                       style="display:inline-block;padding:22px 100px;background:#111827;border-radius:14px;text-decoration:none;font-size:14px;font-weight:600;color:#ffffff;">
                      Verify email
                    </a>
                  </div>
          
                  <hr style="border:none;height:1px;background-color:#f3f4f6;margin:70px 0;width:100%;" />
          
                  <table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
                    <tr>
                      <td style="padding:0 10px;">
                        <a href="https://wisdom-web.vercel.app/" aria-label="Wisdom web">
                          <img src="${IMG_WISDOM}" width="37" height="37" alt="Wisdom"
                               style="display:block;border:0;outline:none;text-decoration:none;" />
                        </a>
                      </td>
                      <td style="padding:0 10px;">
                        <a href="https://www.instagram.com/wisdom__app/" aria-label="Instagram">
                          <img src="${IMG_INSTA}" width="37" height="37" alt="Instagram"
                               style="display:block;border:0;outline:none;text-decoration:none;" />
                        </a>
                      </td>
                      <td style="padding:0 10px;">
                        <a href="https://x.com/wisdom_entity" aria-label="X">
                          <img src="${IMG_X}" width="37" height="37" alt="X"
                               style="display:block;border:0;outline:none;text-decoration:none;" />
                        </a>
                      </td>
                    </tr>
                  </table>
          
                  <div style="font-size:12px;color:#6b7280;line-height:1.4;text-align:center;">
                    <a href="#" style="color:#6b7280;text-decoration:none;">Privacy Policy</a>
                    &nbsp;·&nbsp;
                    <a href="#" style="color:#6b7280;text-decoration:none;">Terms of Service</a>
                    <br /><br />
                    Mataró, BCN, 08304
                    <br /><br />
                    This email was sent to ${user.email}
                  </div>
                </div>
              </body>
            </html>`
    });

    return res.status(200).json({ message: 'Verification email resent successfully.' });
  } catch (err) {
    console.error('Error al reenviar el correo de verificación:', err);
    return res.status(503).json({ error: 'VERIFICATION_EMAIL_DELIVERY_FAILED' });
  }
});
//Ruta para actualizar el profile
app.put('/api/user/:id/profile', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  const body = req.body || {};
  const hasField = (field) => Object.prototype.hasOwnProperty.call(body, field);

  try {
    const [users] = await promisePool.query(
      'SELECT username, first_name, surname, phone, profile_picture FROM user_account WHERE id = ? LIMIT 1',
      [requestedUserId]
    );

    if (users.length === 0) {
      return res.status(404).json({ notFound: true, message: 'No se encontr\u00f3 el usuario.' });
    }

    const currentUser = users[0];

    const username = hasField('username')
      ? (typeof body.username === 'string' ? body.username.trim() : '')
      : currentUser.username;
    const first_name = hasField('first_name')
      ? (typeof body.first_name === 'string' ? body.first_name.trim() : '')
      : currentUser.first_name;
    const surname = hasField('surname')
      ? (typeof body.surname === 'string' ? body.surname.trim() : '')
      : currentUser.surname;

    let phone = currentUser.phone;
    if (hasField('phone')) {
      if (body.phone === null) {
        phone = null;
      } else if (typeof body.phone === 'string') {
        const trimmedPhone = body.phone.trim();
        phone = trimmedPhone || null;
      } else {
        return res.status(400).json({ error: 'invalid_profile_fields' });
      }
    }

    let profile_picture = currentUser.profile_picture;
    if (hasField('profile_picture')) {
      if (body.profile_picture === null) {
        profile_picture = null;
      } else if (typeof body.profile_picture === 'string') {
        const trimmedProfilePicture = body.profile_picture.trim();
        profile_picture = trimmedProfilePicture || null;
      } else {
        return res.status(400).json({ error: 'invalid_profile_fields' });
      }
    }

    if (!username || !first_name || !surname) {
      return res.status(400).json({ error: 'invalid_profile_fields' });
    }

    const [result] = await promisePool.query(
      `UPDATE user_account
       SET profile_picture = ?, username = ?, first_name = ?, surname = ?, phone = ?
       WHERE id = ?`,
      [profile_picture, username, first_name, surname, phone, requestedUserId]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ notFound: true, message: 'No se encontr\u00f3 el usuario.' });
    }

    const user = await fetchSanitizedUserById(requestedUserId);
    return res.status(200).json({ message: 'Perfil actualizado exitosamente.', user });
  } catch (err) {
    if (err?.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'USERNAME_EXISTS' });
    }

    console.error('Error al actualizar el perfil del usuario:', err);
    return res.status(500).json({ error: 'Error al actualizar el perfil del usuario.' });
  }
});
//Ruta para actualizar account (ahora mismo solo actualiza email)
app.put('/api/user/:id/email', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;
  const email = normalizeEmail(req.body?.email);

  if (!email || !isValidEmail(email)) {
    return res.status(400).json({ error: 'invalid_email' });
  }

  try {
    const [users] = await promisePool.query(
      'SELECT auth_provider FROM user_account WHERE id = ? LIMIT 1',
      [requestedUserId]
    );

    if (users.length === 0) {
      return res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
    }

    if (users[0].auth_provider && users[0].auth_provider !== 'email') {
      return res.status(409).json({
        error: 'EMAIL_CHANGE_NOT_AVAILABLE_FOR_PROVIDER',
        auth_provider: users[0].auth_provider,
      });
    }

    const [result] = await promisePool.query(
      'UPDATE user_account SET email = ? WHERE id = ?',
      [email, requestedUserId]
    );

    if (result.affectedRows > 0) {
      return res.status(200).json({ message: 'Email actualizado exitosamente.' });
    }

    return res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
  } catch (err) {
    if (err?.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'EMAIL_EXISTS' });
    }

    console.error('Error al actualizar el email del usuario:', err);
    return res.status(500).json({ error: 'Error al actualizar el email del usuario.' });
  }
});
// Ruta para actualizar el idioma preferido del usuario
app.put('/api/user/:id/language', authenticateToken, (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;
  const { language } = req.body;

  if (typeof language !== 'string' || language.trim() === '') {
    return res.status(400).json({ error: 'invalid_language' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      UPDATE user_account
      SET language = ?
      WHERE id = ?;
    `;

    connection.query(query, [language.trim(), requestedUserId], (queryErr, result) => {
      connection.release();

      if (queryErr) {
        console.error('Error al actualizar el idioma del usuario:', queryErr);
        return res.status(500).json({ error: 'Error al actualizar el idioma del usuario.' });
      }

      if (result.affectedRows > 0) {
        return res.status(200).json({ message: 'Idioma actualizado exitosamente.' });
      }

      return res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
    });
  });
});

app.put('/api/user/:id/currency', authenticateToken, (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  const currency = normalizeCurrencyCode(req.body?.currency, null);
  if (!currency) {
    return res.status(400).json({ error: 'invalid_currency' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      UPDATE user_account
      SET currency = ?
      WHERE id = ?;
    `;

    connection.query(query, [currency, requestedUserId], (queryErr, result) => {
      connection.release();

      if (queryErr) {
        console.error('Error al actualizar la moneda del usuario:', queryErr);
        return res.status(500).json({ error: 'Error al actualizar la moneda del usuario.' });
      }

      if (result.affectedRows > 0) {
        return res.status(200).json({ message: 'Moneda actualizada exitosamente.' });
      }

      return res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
    });
  });
});

// Cambiar contraseña
app.put('/api/user/:id/password', authenticateToken, async (req, res) => {
  const currentPassword = typeof req.body?.currentPassword === 'string' ? req.body.currentPassword : '';
  const newPassword = typeof req.body?.newPassword === 'string' ? req.body.newPassword : '';
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  if (!currentPassword) {
    return res.status(400).json({ error: 'invalid_password' });
  }

  if (isPasswordTooShort(newPassword)) {
    return res.status(400).json({ error: 'PASSWORD_TOO_SHORT' });
  }

  try {
    const [results] = await promisePool.query(
      'SELECT password, auth_provider FROM user_account WHERE id = ? LIMIT 1',
      [requestedUserId]
    );

    if (results.length === 0) {
      return res.status(404).json({ error: 'Usuario no encontrado.' });
    }

    if (results[0].auth_provider && results[0].auth_provider !== 'email') {
      return res.status(409).json({
        error: 'PASSWORD_CHANGE_NOT_AVAILABLE_FOR_PROVIDER',
        auth_provider: results[0].auth_provider,
      });
    }

    if (typeof results[0].password !== 'string' || !results[0].password) {
      return res.status(400).json({ error: 'Contrase\u00f1a actual incorrecta.' });
    }

    const match = await bcrypt.compare(currentPassword, results[0].password);
    if (!match) {
      return res.status(400).json({ error: 'Contrase\u00f1a actual incorrecta.' });
    }

    const hashed = await bcrypt.hash(newPassword, 10);
    await promisePool.query('UPDATE user_account SET password = ? WHERE id = ?', [hashed, requestedUserId]);
    await revokeAllUserSessions(requestedUserId);

    const user = await fetchSanitizedUserById(requestedUserId);
    const tokens = await issueAuthTokens(requestedUserId, req);

    return res.json({
      message: 'Contrase\u00f1a actualizada con \u00e9xito.',
      user,
      ...tokens,
    });
  } catch (err) {
    console.error('Error al actualizar la contrase\u00f1a:', err);
    return res.status(500).json({ error: 'Error al actualizar la contrase\u00f1a.' });
  }
});
//Ruta para borrar una cuenta
app.delete('/api/user/:id', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  const authorizationCode = typeof req.body?.authorizationCode === 'string' ? req.body.authorizationCode.trim() : '';
  const identityToken = typeof req.body?.identityToken === 'string' ? req.body.identityToken.trim() : '';
  let connection;

  if (!requestedUserId) return;

  try {
    const [users] = await promisePool.query(
      'SELECT id, auth_provider, provider_id FROM user_account WHERE id = ? LIMIT 1',
      [requestedUserId]
    );

    if (users.length === 0) {
      return res.status(404).json({ notFound: true, message: 'No se encontr\u00f3 el usuario.' });
    }

    const account = users[0];

    if (account.auth_provider === 'apple') {
      if (!authorizationCode || !identityToken) {
        return res.status(400).json({ error: 'APPLE_REAUTH_REQUIRED' });
      }

      const payload = await appleSigninAuth.verifyIdToken(identityToken, {
        audience: APPLE_CLIENT_ID,
      });
      const providerIdFromToken = typeof payload?.sub === 'string' ? payload.sub.trim() : '';

      if (!providerIdFromToken || providerIdFromToken !== String(account.provider_id || '').trim()) {
        return res.status(403).json({ error: 'APPLE_REAUTH_PROVIDER_MISMATCH' });
      }

      await revokeAppleSessionWithAuthorizationCode(authorizationCode);
    }

    connection = await promisePool.getConnection();
    await connection.beginTransaction();

    const [ownedAddressRows] = await connection.query(
      `SELECT address_id
         FROM directions
        WHERE user_id = ?
       UNION
       SELECT address_id
         FROM user_address
        WHERE user_id = ?
       UNION
       SELECT address_id
         FROM collection_method
        WHERE user_id = ? AND address_id IS NOT NULL`,
      [requestedUserId, requestedUserId, requestedUserId]
    );

    const ownedAddressIds = ownedAddressRows
      .map((row) => Number(row.address_id))
      .filter((addressId) => Number.isInteger(addressId));

    await connection.query('DELETE FROM password_reset_codes WHERE user_id = ?', [requestedUserId]);
    await connection.query('DELETE FROM auth_session WHERE user_id = ?', [requestedUserId]);
    await connection.query('DELETE FROM service_report WHERE reporter_user_id = ?', [requestedUserId]);
    await connection.query('DELETE FROM payment_method WHERE user_id = ?', [requestedUserId]);
    await connection.query('DELETE FROM collection_method WHERE user_id = ?', [requestedUserId]);

    const [result] = await connection.query('DELETE FROM user_account WHERE id = ?', [requestedUserId]);

    if (result.affectedRows === 0) {
      await connection.rollback();
      return res.status(404).json({ notFound: true, message: 'No se encontr\u00f3 el usuario.' });
    }

    if (ownedAddressIds.length > 0) {
      const placeholders = ownedAddressIds.map(() => '?').join(', ');
      await connection.query(
        `DELETE a FROM address a
          WHERE a.id IN (${placeholders})
            AND NOT EXISTS (SELECT 1 FROM directions d WHERE d.address_id = a.id)
            AND NOT EXISTS (SELECT 1 FROM user_address ua WHERE ua.address_id = a.id)
            AND NOT EXISTS (SELECT 1 FROM collection_method cm WHERE cm.address_id = a.id)`,
        ownedAddressIds
      );
    }

    await connection.commit();
    return res.status(200).json({ message: 'Cuenta eliminada exitosamente.' });
  } catch (err) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error al hacer rollback al eliminar la cuenta del usuario:', rollbackError);
      }
    }

    if (err?.message === 'APPLE_REVOKE_CONFIG_MISSING') {
      return res.status(500).json({ error: 'APPLE_REVOKE_CONFIG_MISSING' });
    }

    if (
      err?.message === 'APPLE_REVOKE_TOKEN_MISSING' ||
      err?.message === 'invalid_grant' ||
      err?.code === 'APPLE_TOKEN_EXCHANGE_FAILED' ||
      err?.code === 'APPLE_TOKEN_REVOKE_FAILED' ||
      err?.name === 'JsonWebTokenError' ||
      err?.name === 'TokenExpiredError'
    ) {
      return res.status(400).json({ error: 'APPLE_REAUTH_REQUIRED' });
    }

    if (err?.code === 'ER_ROW_IS_REFERENCED_2') {
      return res.status(409).json({ error: 'user_has_related_records' });
    }

    console.error('Error al eliminar la cuenta del usuario:', err);
    return res.status(500).json({ error: 'Error al eliminar la cuenta del usuario.' });
  } finally {
    if (connection) {
      connection.release();
    }
  }
});
//Ruta para actualizar allow_notis de un usuario
app.put('/api/user/:id/allow_notis', authenticateToken, (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;
  const { allow_notis } = req.body;  // Nuevo valor de `allow_notis`

  // Verificar que el valor de `allow_notis` sea válido (booleano)
  if (typeof allow_notis !== 'boolean') {
    res.status(400).json({ error: 'El valor de allow_notis debe ser un booleano.' });
    return;
  }

  // Obtener una conexión del pool de MySQL
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta SQL para actualizar el campo `allow_notis`
    const query = `
          UPDATE user_account
          SET allow_notis = ?
          WHERE id = ?;
      `;

    // Ejecutar la consulta
    connection.query(query, [allow_notis, requestedUserId], (err, result) => {
      connection.release();  // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al actualizar allow_notis:', err);
        res.status(500).json({ error: 'Error al actualizar allow_notis.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'allow_notis actualizado exitosamente.' });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

// Ruta para añadir un strike a un usuario
app.post('/api/user/:id/strike', authenticateToken, (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    const query = `
      UPDATE user_account
      SET strikes_num = COALESCE(strikes_num, 0) + 1
      WHERE id = ?;
    `;

    connection.query(query, [requestedUserId], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al añadir el strike:', err);
        res.status(500).json({ error: 'Error al añadir el strike.' });
        return;
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Strike añadido exitosamente.' });
      } else {
        res.status(404).json({ notFound: true, message: 'No se encontró el usuario.' });
      }
    });
  });
});

//Ruta para guardar address + direction
app.post('/api/directions', (req, res) => {
  const {
    user_id,
    address_type,
    street_number,
    address_1,
    address_2,
    postal_code,
    city,
    state,
    country,
  } = req.body;
  const latitude = normalizeCoordinateValue(req.body?.latitude ?? req.body?.address_latitude, {
    min: -90,
    max: 90,
  });
  const longitude = normalizeCoordinateValue(req.body?.longitude ?? req.body?.address_longitude, {
    min: -180,
    max: 180,
  });

  // Verificar que los campos requeridos estén presentes, excepto address_2 y street_number que pueden ser nulos
  if (!user_id || !address_type || !address_1 || !postal_code || !city || !state || !country) {
    return res.status(400).json({ error: 'Algunos campos requeridos faltan.' });
  }

  // Si street_number o address_2 son undefined o vacíos, se establecen como NULL
  const streetNumberValue = street_number || null;
  const address2Value = address_2 || null;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Primero insertar la dirección en la tabla address
    const addressQuery = 'INSERT INTO address (address_type, street_number, address_1, address_2, postal_code, city, state, country, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
    const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country, latitude, longitude];

    connection.query(addressQuery, addressValues, (err, result) => {
      if (err) {
        connection.release();
        console.error('Error al insertar la dirección:', err);
        return res.status(500).json({ error: 'Error al insertar la dirección.' });
      }

      const addressId = result.insertId; // Obtenemos el ID de la dirección recién insertada

      // Ahora insertar en la tabla directions utilizando el user_id y el address_id
      const directionsQuery = 'INSERT INTO directions (user_id, address_id) VALUES (?, ?)';
      const directionsValues = [user_id, addressId];

      connection.query(directionsQuery, directionsValues, (err, result) => {
        connection.release(); // Liberar la conexión después de usarla

        if (err) {
          console.error('Error al insertar la dirección en directions:', err);
          return res.status(500).json({ error: 'Error al insertar en directions.' });
        }

        res.status(201).json({ message: 'Dirección añadida con éxito', address_id: addressId });
      });
    });
  });
});

//Ruta para obtener todas las direcciones de un user
app.get('/api/directions/:user_id', (req, res) => {
  const { user_id } = req.params;

  if (!user_id) {
    return res.status(400).json({ error: 'El user_id es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Consulta para obtener todas las direcciones del usuario junto con los detalles de address
    const query = `
      SELECT d.id AS direction_id, a.id AS address_id, a.address_type, a.street_number, a.address_1, a.address_2, a.postal_code, a.city, a.state, a.country, a.latitude, a.longitude
      FROM directions d
      JOIN address a ON d.address_id = a.id
      WHERE d.user_id = ?
    `;

    connection.query(query, [user_id], (err, results) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener las direcciones:', err);
        return res.status(500).json({ error: 'Error al obtener las direcciones.' });
      }

      if (results.length === 0) {
        return res.status(200).json({ message: 'No se encontraron direcciones para este usuario.', notFound: true });
      }

      res.status(200).json({ directions: results });
    });
  });
});

//Actualziar address 
app.put('/api/address/:id', (req, res) => {
  const { id } = req.params; // ID de la address a actualizar
  const {
    address_type,
    street_number,
    address_1,
    address_2,
    postal_code,
    city,
    state,
    country,
  } = req.body;
  const latitude = normalizeCoordinateValue(req.body?.latitude ?? req.body?.address_latitude, {
    min: -90,
    max: 90,
  });
  const longitude = normalizeCoordinateValue(req.body?.longitude ?? req.body?.address_longitude, {
    min: -180,
    max: 180,
  });

  // Verificar que los campos requeridos estén presentes
  if (!address_type || !address_1 || !postal_code || !city || !state || !country) {
    return res.status(400).json({ error: 'Algunos campos requeridos faltan.' });
  }

  // Si street_number o address_2 son undefined o vacíos, se establecen como NULL
  const streetNumberValue = street_number || null;
  const address2Value = address_2 || null;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Actualizar la dirección en la tabla address
      const addressQuery = `
        UPDATE address 
        SET address_type = ?, street_number = ?, address_1 = ?, address_2 = ?, postal_code = ?, city = ?, state = ?, country = ?, latitude = ?, longitude = ?
        WHERE id = ?`;
    const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country, latitude, longitude, id];

    connection.query(addressQuery, addressValues, (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al actualizar la dirección:', err);
        return res.status(500).json({ error: 'Error al actualizar la dirección.' });
      }

      if (result.affectedRows === 0) {
        return res.status(404).json({ error: 'Dirección no encontrada.' });
      }

      res.status(200).json({ message: 'Dirección actualizada con éxito', address_id: Number(id) });
    });
  });
});

//Borrar address por su id
app.delete('/api/address/:id', (req, res) => {
  const { id } = req.params; // ID de la dirección a eliminar

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Eliminar la dirección en la tabla address
    const deleteQuery = 'DELETE FROM address WHERE id = ?';
    const deleteValues = [id];

    connection.query(deleteQuery, deleteValues, (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al eliminar la dirección:', err);
        return res.status(500).json({ error: 'Error al eliminar la dirección.' });
      }

      if (result.affectedRows === 0) {
        return res.status(404).json({ error: 'Dirección no encontrada.' });
      }

      res.status(200).json({ message: 'Dirección eliminada con éxito' });
    });
  });
});

//Crear reserva
app.post('/api/bookings', authenticateToken, async (req, res) => {
  const authenticatedUserId = parseRequestedUserId(req.user?.id);
  const requestedUserId = normalizeNullableInteger(req.body.client_user_id ?? req.body.user_id);
  const serviceId = normalizeNullableInteger(req.body.service_id);

  if (!authenticatedUserId || !requestedUserId || requestedUserId !== authenticatedUserId) {
    return res.status(403).json({ error: 'No autorizado para crear la reserva.' });
  }

  if (!serviceId) {
    return res.status(400).json({ error: 'service_id es requerido.' });
  }

  const requestedStartInput = req.body.requested_start_datetime ?? req.body.booking_start_datetime ?? null;
  const requestedDurationMinutes = normalizeDurationMinutes(
    req.body.requested_duration_minutes ?? req.body.service_duration ?? null
  );

  if (!isDurationMinutesInRange(requestedDurationMinutes)) {
    return res.status(400).json({
      error: `La duración debe estar entre ${MIN_BOOKING_DURATION_MINUTES} y ${MAX_BOOKING_DURATION_MINUTES} minutos.`,
    });
  }

  const parsedRequestedStartDateTime = parseDateTimeInput(requestedStartInput);
  if (parsedRequestedStartDateTime && parsedRequestedStartDateTime.getTime() < Date.now()) {
    return res.status(400).json({ error: 'No se puede reservar en el pasado.' });
  }

  const description = typeof req.body.description === 'string' && req.body.description.trim()
    ? req.body.description.trim()
    : null;

  let addressId = normalizeNullableInteger(req.body.address_id);
  const streetNumberValue = req.body.street_number || null;
  const address2Value = req.body.address_2 || null;
  const connection = await promisePool.getConnection();

  try {
    await connection.beginTransaction();

    const [[serviceSnapshot]] = await connection.query(
      `
      SELECT
        s.id,
        s.user_id AS provider_user_id_snapshot,
        s.service_title,
        s.minimum_notice_policy,
        s.latitude,
        s.longitude,
        s.action_rate,
        p.price,
        p.currency AS currency,
        p.price_type
      FROM service s
      LEFT JOIN price p ON s.price_id = p.id
      WHERE s.id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [serviceId]
    );

    if (!serviceSnapshot) {
      await connection.rollback();
      return res.status(404).json({ error: 'Servicio no encontrado.' });
    }

    const now = new Date();
    const minimumNoticePolicyMinutes = normalizeMinimumNoticeMinutes(serviceSnapshot.minimum_notice_policy);
    if (!meetsMinimumNotice({
      requestedStartDateTime: parsedRequestedStartDateTime,
      minimumNoticeMinutes: minimumNoticePolicyMinutes,
      now,
    })) {
      await connection.rollback();
      return res.status(400).json({
        error_code: 'minimum_notice_not_met',
        minimum_notice_policy: minimumNoticePolicyMinutes,
        error: `Este servicio exige una antelación mínima de ${formatLeadTimeLabel(minimumNoticePolicyMinutes)}.`,
      });
    }

    const bookingAddressRule = getBookingAddressRuleForService(serviceSnapshot);
    if (bookingAddressRule.mode === 'hidden') {
      addressId = null;
    } else if (addressId !== null) {
      const [[existingAddress]] = await connection.query(
        `
        SELECT
          id,
          address_type,
          street_number,
          address_1,
          address_2,
          postal_code,
          city,
          state,
          country,
          latitude,
          longitude
        FROM address
        WHERE id = ?
        LIMIT 1
        FOR UPDATE
        `,
        [addressId]
      );
      if (!existingAddress) {
        await connection.rollback();
        return res.status(400).json({ error: 'address_id no existe.' });
      }

      assertBookingAddressRulesForService({
        serviceRow: serviceSnapshot,
        addressRow: existingAddress,
        requestBody: req.body || {},
      });
    } else if (hasInlineAddressFields(req.body || {})) {
      assertBookingAddressRulesForService({
        serviceRow: serviceSnapshot,
        requestBody: req.body || {},
      });

      const inlineAddressLocation = extractAddressLocationFromSource(req.body || {});
      const [addressInsertResult] = await connection.query(
        `INSERT INTO address
          (address_type, street_number, address_1, address_2, postal_code, city, state, country, latitude, longitude)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          req.body.address_type,
          streetNumberValue,
          req.body.address_1,
          address2Value,
          req.body.postal_code,
          req.body.city,
          req.body.state,
          req.body.country,
          inlineAddressLocation?.lat ?? null,
          inlineAddressLocation?.lng ?? null,
        ]
      );
      addressId = addressInsertResult.insertId;
    } else {
      try {
        assertBookingAddressRulesForService({
          serviceRow: serviceSnapshot,
          requestBody: req.body || {},
        });
      } catch (addressError) {
        await connection.rollback();
        return res.status(addressError.statusCode || 400).json({ error: addressError.message });
      }
    }

    const schedule = buildBookingSchedule({
      createdAt: now,
      requestedStartDateTime: parsedRequestedStartDateTime,
      requestedDurationMinutes,
    });
    const serviceCurrency = normalizeCurrencyCode(serviceSnapshot.currency, 'EUR');
    const unitPriceAmount = Number.parseFloat(serviceSnapshot.price) || 0;
    const unitPriceAmountCentsSnapshot = serviceSnapshot.price === null || serviceSnapshot.price === undefined
      ? null
      : toMinorUnits(unitPriceAmount, serviceCurrency);
    const pricingSnapshot = computeBookingPricingSnapshot({
      priceType: serviceSnapshot.price_type,
      unitPrice: unitPriceAmount,
      durationMinutes: requestedDurationMinutes,
      currency: serviceCurrency,
    });
    const estimatedBaseAmountCents = pricingSnapshot.final === null && pricingSnapshot.type !== 'fix'
      ? null
      : toMinorUnits(pricingSnapshot.base, serviceCurrency);
    const estimatedCommissionAmountCents = toMinorUnits(pricingSnapshot.commission || 0, serviceCurrency);
    const estimatedTotalAmountCents = pricingSnapshot.final === null
      ? null
      : toMinorUnits(pricingSnapshot.final, serviceCurrency);

    const [bookingInsertResult] = await connection.query(
      `
      INSERT INTO booking
        (
          client_user_id,
          service_id,
          provider_user_id_snapshot,
          address_id,
          description,
          service_status,
          settlement_status,
          requested_start_datetime,
          requested_duration_minutes,
          requested_end_datetime,
          accept_deadline_at,
          expires_at,
          last_minute_window_starts_at,
          service_title_snapshot,
          price_type_snapshot,
          service_currency_snapshot,
          unit_price_amount_cents_snapshot,
          minimum_notice_policy_snapshot,
          estimated_base_amount_cents,
          estimated_commission_amount_cents,
          estimated_total_amount_cents,
          deposit_amount_cents_snapshot,
          deposit_currency_snapshot
        )
      VALUES (?, ?, ?, ?, ?, 'pending_deposit', 'none', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        requestedUserId,
        serviceId,
        serviceSnapshot.provider_user_id_snapshot,
        addressId,
        description,
        toDbDateTime(schedule.requestedStartDateTime),
        schedule.requestedDurationMinutes,
        toDbDateTime(schedule.requestedEndDateTime),
        toDbDateTime(schedule.acceptDeadlineAt),
        toDbDateTime(schedule.expiresAt),
        toDbDateTime(schedule.lastMinuteWindowStartsAt),
        serviceSnapshot.service_title,
        String(serviceSnapshot.price_type || '').trim().toLowerCase() || null,
        serviceCurrency,
        unitPriceAmountCentsSnapshot,
        minimumNoticePolicyMinutes,
        estimatedBaseAmountCents,
        estimatedCommissionAmountCents,
        estimatedTotalAmountCents,
        estimatedCommissionAmountCents,
        serviceCurrency,
      ]
    );

    await insertBookingStatusHistory(connection, {
      bookingId: bookingInsertResult.insertId,
      fromServiceStatus: null,
      toServiceStatus: 'pending_deposit',
      fromSettlementStatus: null,
      toSettlementStatus: 'none',
      changedByUserId: authenticatedUserId,
      reasonCode: 'created',
    });

    const [[createdBookingRow]] = await connection.query(
      `
      SELECT
        b.id AS booking_id,
        b.*,
        p.price,
        COALESCE(p.currency, b.service_currency_snapshot) AS currency,
        COALESCE(p.price_type, b.price_type_snapshot) AS price_type
      FROM booking b
      LEFT JOIN service s ON b.service_id = s.id
      LEFT JOIN price p ON s.price_id = p.id
      WHERE b.id = ?
      LIMIT 1
      `,
      [bookingInsertResult.insertId]
    );

    await connection.commit();
    return res.status(201).json({
      message: 'Reserva creada con éxito',
      booking: mapBookingRecordForApi(createdBookingRow),
    });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    if (error?.statusCode) {
      return res.status(error.statusCode).json({ error: error.message });
    }
    console.error('Error al insertar la reserva:', error);
    return res.status(500).json({ error: 'Error al insertar la reserva.' });
  } finally {
    connection.release();
  }
});

// Obtener detalles de una reserva
app.get('/api/bookings/:id', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  try {
    const bookingQuery = `
      SELECT
        b.id AS booking_id,
        b.id,
        b.client_user_id,
        b.provider_user_id_snapshot,
        b.service_id,
        b.address_id,
        b.description,
        b.service_status,
        b.settlement_status,
        b.order_datetime AS created_at,
        b.order_datetime,
        b.updated_at,
        b.requested_start_datetime,
        b.requested_duration_minutes,
        b.requested_end_datetime,
        b.deposit_confirmed_at,
        b.accepted_at,
        b.started_at,
        b.finished_at,
        b.canceled_at,
        b.expired_at,
        b.accept_deadline_at,
        b.expires_at,
        b.client_approval_deadline_at,
        b.last_minute_window_starts_at,
        b.canceled_by_user_id,
        b.cancellation_reason_code,
        b.cancellation_note,
        b.service_title_snapshot,
        b.price_type_snapshot,
        b.service_currency_snapshot,
        b.unit_price_amount_cents_snapshot,
        b.minimum_notice_policy_snapshot,
        b.estimated_base_amount_cents,
        b.estimated_commission_amount_cents,
        b.estimated_total_amount_cents,
        b.deposit_amount_cents_snapshot,
        b.deposit_currency_snapshot,
        b.selected_customer_payment_method_id,
        cp.id AS closure_proposal_id,
        cp.status AS closure_status,
        cp.proposed_base_amount_cents,
        cp.proposed_commission_amount_cents,
        cp.proposed_total_amount_cents,
        cp.proposed_final_duration_minutes,
        cp.deposit_already_paid_amount_cents,
        cp.amount_due_from_client_cents,
        cp.amount_to_refund_cents,
        cp.provider_payout_amount_cents,
        cp.platform_amount_cents,
        cp.zero_charge_mode,
        cp.sent_at AS closure_sent_at,
        cp.accepted_at AS closure_accepted_at,
        cp.rejected_at AS closure_rejected_at,
        cp.revoked_at AS closure_revoked_at,
        bir.id AS open_issue_report_id,
        bir.issue_type AS open_issue_type,
        bir.status AS open_issue_status,
        bir.reported_by_user_id AS open_issue_reported_by_user_id,
        bir.created_at AS open_issue_created_at,
        bcr.id AS change_request_id,
        bcr.requested_by_user_id AS change_request_requested_by_user_id,
        bcr.target_user_id AS change_request_target_user_id,
        bcr.status AS change_request_status,
        bcr.changes_json AS change_request_changes_json,
        bcr.message AS change_request_message,
        bcr.created_at AS change_request_created_at,
        bcr.resolved_at AS change_request_resolved_at,
        p.price,
        COALESCE(p.currency, b.service_currency_snapshot) AS currency,
        COALESCE(p.price_type, b.price_type_snapshot) AS price_type,
        pm.payment_type AS customer_payment_method_stripe_id,
        pm.provider,
        pm.brand,
        pm.card_number,
        pm.expiry_date,
        pm.is_safed,
        pm.is_default,
        a.address_type,
        a.street_number,
        a.address_1,
        a.address_2,
        a.postal_code,
        a.city,
        a.state,
        a.country,
        a.latitude AS address_latitude,
        a.longitude AS address_longitude
      FROM booking b
      LEFT JOIN service s ON b.service_id = s.id
      LEFT JOIN price p ON s.price_id = p.id
      LEFT JOIN payment_method pm ON b.selected_customer_payment_method_id = pm.id
      LEFT JOIN address a ON b.address_id = a.id
      LEFT JOIN booking_closure_proposal cp ON cp.id = (
        SELECT cp2.id
        FROM booking_closure_proposal cp2
        WHERE cp2.booking_id = b.id
        ORDER BY cp2.id DESC
        LIMIT 1
      )
      LEFT JOIN booking_issue_report bir ON bir.id = (
        SELECT bir2.id
        FROM booking_issue_report bir2
        WHERE bir2.booking_id = b.id AND bir2.status = 'open'
        ORDER BY bir2.created_at DESC, bir2.id DESC
        LIMIT 1
      )
      LEFT JOIN booking_change_request bcr ON bcr.id = (
        SELECT bcr2.id
        FROM booking_change_request bcr2
        WHERE bcr2.booking_id = b.id
        ORDER BY
          CASE WHEN bcr2.status = 'pending' THEN 0 ELSE 1 END,
          bcr2.created_at DESC,
          bcr2.id DESC
        LIMIT 1
      )
      WHERE b.id = ?
      LIMIT 1
      `;

    let [bookingRows] = await promisePool.query(bookingQuery, [bookingId]);

    if (bookingRows.length === 0) {
      return res.status(404).json({ message: 'Reserva no encontrada.' });
    }

    const booking = bookingRows[0];
    const isClientOwner = req.user && Number(req.user.id) === Number(booking.client_user_id);
    const isProviderOwner = req.user && Number(req.user.id) === Number(booking.provider_user_id_snapshot);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isClientOwner && !isProviderOwner && !isStaff) {
      return res.status(403).json({ error: 'No autorizado.' });
    }

    if (canAutoExpireRequestedBooking(booking)) {
      await expireRequestedBookingByIdIfNeeded(bookingId);
      [bookingRows] = await promisePool.query(bookingQuery, [bookingId]);
      if (bookingRows.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }
    }

    let depositPaymentSummary = null;
    let finalPaymentSummary = null;

    try {
      const [paymentRows] = await promisePool.query(
        `
        SELECT
          p.type,
          p.amount_cents,
          p.currency,
          p.status,
          p.payment_method_id,
          p.payment_method_last4,
          pm.brand AS payment_method_brand
        FROM payments p
        LEFT JOIN payment_method pm ON pm.payment_type = p.payment_method_id
        WHERE p.booking_id = ?
        ORDER BY p.id DESC
        `,
        [bookingId]
      );

      const pickPayment = (type) => (
        paymentRows.find((row) => row.type === type && row.status === 'succeeded')
        || paymentRows.find((row) => row.type === type)
        || null
      );

      const fallbackCurrency = normalizeCurrencyCode(
        bookingRows[0]?.service_currency_snapshot,
        normalizeCurrencyCode(bookingRows[0]?.currency, 'EUR')
      );

      depositPaymentSummary = mapBookingPaymentSummaryForApi(
        pickPayment('deposit'),
        fallbackCurrency
      );
      finalPaymentSummary = mapBookingPaymentSummaryForApi(
        pickPayment('final'),
        fallbackCurrency
      );
    } catch (paymentLookupError) {
      console.error('Error al obtener el resumen de pagos de la reserva:', paymentLookupError);
    }

    const bookingResponse = mapBookingRecordForApi(bookingRows[0]);
    bookingResponse.deposit_payment_summary = depositPaymentSummary;
    bookingResponse.final_payment_summary = finalPaymentSummary;

    return res.status(200).json(bookingResponse);
  } catch (error) {
    console.error('Error al obtener la reserva:', error);
    return res.status(500).json({ error: 'Error al obtener la reserva.' });
  }
});

// Actualizar una reserva
app.put('/api/bookings/:id', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  let createdChangeRequestId = null;
  try {
    await connection.beginTransaction();

    const [[currentBooking]] = await connection.query(
      `
      SELECT
        b.id,
        b.service_id,
        b.client_user_id,
        b.provider_user_id_snapshot,
        b.service_status,
        b.settlement_status,
        b.order_datetime AS created_at,
        b.order_datetime,
        b.requested_start_datetime,
        b.requested_duration_minutes,
        b.requested_end_datetime,
        b.description,
        b.address_id,
        b.price_type_snapshot,
        b.service_currency_snapshot,
        b.unit_price_amount_cents_snapshot,
        b.minimum_notice_policy_snapshot,
        b.estimated_base_amount_cents,
        b.estimated_commission_amount_cents,
        b.estimated_total_amount_cents
      FROM booking b
      WHERE b.id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!currentBooking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const isClientOwner = req.user && Number(req.user.id) === Number(currentBooking.client_user_id);
    const isProviderOwner = req.user && Number(req.user.id) === Number(currentBooking.provider_user_id_snapshot);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isClientOwner && !isProviderOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'No autorizado.' });
    }

    const normalizedCurrentServiceStatus = normalizeServiceStatus(
      currentBooking.service_status,
      'pending_deposit'
    );
    const requestedUpdateKeys = Object.keys(req.body || {});
    const isRequestedDescriptionOnlyEdit = normalizedCurrentServiceStatus === 'requested'
      && isClientOwner
      && !isStaff;

    if (isRequestedDescriptionOnlyEdit) {
      const unsupportedRequestedKeys = requestedUpdateKeys.filter((key) => key !== 'description');
      if (unsupportedRequestedKeys.length > 0) {
        await connection.rollback();
        return res.status(400).json({
          error: 'Mientras la reserva está en solicitud solo se puede editar la descripción.',
        });
      }

      if (!requestedUpdateKeys.includes('description')) {
        await connection.rollback();
        return res.status(400).json({ error: 'No hay cambios nuevos para enviar.' });
      }

      const nextDescription = normalizeNullableText(req.body?.description);
      if (normalizeNullableText(currentBooking.description) === nextDescription) {
        await connection.rollback();
        return res.status(400).json({ error: 'No hay cambios nuevos para enviar.' });
      }

      await connection.query(
        `
        UPDATE booking
        SET description = ?
        WHERE id = ?
        `,
        [nextDescription, bookingId]
      );
      await connection.commit();
      return res.status(200).json({ message: 'Descripción actualizada con éxito.' });
    }

    if (!canEditBooking(currentBooking)) {
      await connection.rollback();
      return res.status(409).json({ error: 'La reserva ya no se puede editar en este estado.' });
    }

    await expirePendingBookingChangeRequestsForBooking(connection, currentBooking, {
      now: new Date(),
    });

    const preparedUpdate = await prepareBookingEditableUpdate(connection, currentBooking, req.body || {});
    const currentComparableFields = buildBookingEditableComparableFields(currentBooking);
    if (bookingEditableFieldsAreEqual(currentComparableFields, preparedUpdate.comparableFields)) {
      await connection.rollback();
      return res.status(400).json({ error: 'No hay cambios nuevos para enviar.' });
    }

    if (isStaff) {
      await applyPreparedBookingEditableUpdate(connection, bookingId, preparedUpdate);
      await expirePendingBookingChangeRequestsForBooking(connection, currentBooking, {
        force: true,
        now: new Date(),
      });
      await connection.commit();
      return res.status(200).json({ message: 'Reserva actualizada con éxito' });
    }

    const requesterUserId = isClientOwner
      ? normalizeNullableInteger(currentBooking.client_user_id)
      : normalizeNullableInteger(currentBooking.provider_user_id_snapshot);
    const targetUserId = isClientOwner
      ? normalizeNullableInteger(currentBooking.provider_user_id_snapshot)
      : normalizeNullableInteger(currentBooking.client_user_id);
    if (!requesterUserId || !targetUserId) {
      await connection.rollback();
      return res.status(409).json({ error: 'La reserva no tiene ambas partes asociadas para crear la solicitud.' });
    }

    const existingPendingChangeRequest = await getLatestBookingChangeRequest(connection, bookingId, {
      forUpdate: true,
      pendingOnly: true,
    });
    const changeRequestMessage = normalizeNullableText(req.body?.message);
    if (existingPendingChangeRequest) {
      if (Number(existingPendingChangeRequest.requested_by_user_id) !== Number(requesterUserId)) {
        await connection.rollback();
        return res.status(409).json({ error: 'Ya existe una solicitud de modificación pendiente para esta reserva.' });
      }

      await connection.query(
        `
        UPDATE booking_change_request
        SET changes_json = ?,
            message = ?,
            created_at = CURRENT_TIMESTAMP,
            resolved_at = NULL
        WHERE id = ?
        `,
        [
          JSON.stringify(preparedUpdate.changeRequestPayload),
          changeRequestMessage,
          existingPendingChangeRequest.id,
        ]
      );
      createdChangeRequestId = existingPendingChangeRequest.id;
    } else {
      const [insertResult] = await connection.query(
        `
        INSERT INTO booking_change_request
          (booking_id, requested_by_user_id, target_user_id, status, changes_json, message)
        VALUES (?, ?, ?, 'pending', ?, ?)
        `,
        [
          bookingId,
          requesterUserId,
          targetUserId,
          JSON.stringify(preparedUpdate.changeRequestPayload),
          changeRequestMessage,
        ]
      );
      createdChangeRequestId = insertResult.insertId;
    }

    await connection.commit();

    try {
      await sendBookingChangeRequestNotificationEmail({
        changeRequestId: createdChangeRequestId,
        mode: 'created',
      });
    } catch (emailError) {
      console.error('Error sending booking change request email:', {
        bookingId,
        changeRequestId: createdChangeRequestId,
        error: emailError.message,
      });
    }

    return res.status(202).json({
      message: 'Solicitud de modificación enviada.',
      requires_approval: true,
      change_request: {
        id: createdChangeRequestId,
        booking_id: bookingId,
        requested_by_user_id: requesterUserId,
        target_user_id: targetUserId,
        status: 'pending',
        changes: preparedUpdate.changeRequestPayload,
        message: changeRequestMessage,
      },
    });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    if (error?.statusCode) {
      return res.status(error.statusCode).json({ error: error.message });
    }
    console.error('Error al actualizar la reserva:', error);
    return res.status(500).json({ error: 'Error al actualizar la reserva.' });
  } finally {
    connection.release();
  }
});

app.patch('/api/bookings/:id/change-requests/:changeRequestId', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  const changeRequestId = normalizeNullableInteger(req.params.changeRequestId);
  if (!bookingId || !changeRequestId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const action = normalizeBookingChangeRequestStatus(req.body?.action, null);
  if (!['accepted', 'rejected', 'canceled'].includes(action)) {
    return res.status(400).json({ error: 'action inválida.' });
  }

  const connection = await promisePool.getConnection();
  let shouldNotifyRequester = false;
  try {
    await connection.beginTransaction();

    const [[currentBooking]] = await connection.query(
      `
      SELECT
        b.id,
        b.client_user_id,
        b.provider_user_id_snapshot,
        b.service_status,
        b.settlement_status,
        b.order_datetime AS created_at,
        b.order_datetime,
        b.requested_start_datetime,
        b.requested_duration_minutes,
        b.requested_end_datetime,
        b.description,
        b.address_id,
        b.price_type_snapshot,
        b.service_currency_snapshot,
        b.unit_price_amount_cents_snapshot,
        b.minimum_notice_policy_snapshot,
        b.estimated_base_amount_cents,
        b.estimated_commission_amount_cents,
        b.estimated_total_amount_cents
      FROM booking b
      WHERE b.id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!currentBooking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const requesterUserId = normalizeNullableInteger(req.user?.id);
    const isClientOwner = requesterUserId !== null && requesterUserId === Number(currentBooking.client_user_id);
    const isProviderOwner = requesterUserId !== null && requesterUserId === Number(currentBooking.provider_user_id_snapshot);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isClientOwner && !isProviderOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'No autorizado.' });
    }

    await expirePendingBookingChangeRequestsForBooking(connection, currentBooking, {
      now: new Date(),
    });

    const [[changeRequest]] = await connection.query(
      `
      SELECT
        id,
        booking_id,
        requested_by_user_id,
        target_user_id,
        status,
        changes_json,
        message,
        created_at,
        resolved_at
      FROM booking_change_request
      WHERE id = ?
        AND booking_id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [changeRequestId, bookingId]
    );

    if (!changeRequest) {
      await connection.rollback();
      return res.status(404).json({ error: 'Solicitud de modificación no encontrada.' });
    }

    if (normalizeBookingChangeRequestStatus(changeRequest.status, 'pending') !== 'pending') {
      await connection.rollback();
      return res.status(409).json({ error: 'La solicitud de modificación ya no está pendiente.' });
    }

    const isRequestOwner = requesterUserId !== null && requesterUserId === Number(changeRequest.requested_by_user_id);
    const isRequestTarget = requesterUserId !== null && requesterUserId === Number(changeRequest.target_user_id);

    if (action === 'canceled' && !isRequestOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'Solo quien creó la solicitud puede cancelarla.' });
    }

    if ((action === 'accepted' || action === 'rejected') && !isRequestTarget && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'Solo la otra parte puede responder la solicitud.' });
    }

    if (action === 'accepted') {
      if (!canEditBooking(currentBooking)) {
        await connection.rollback();
        return res.status(409).json({ error: 'La reserva ya no se puede modificar en este estado.' });
      }

      const requestedChanges = parseJsonObject(changeRequest.changes_json) || {};
      const preparedUpdate = await prepareBookingEditableUpdate(connection, currentBooking, requestedChanges);
      const currentComparableFields = buildBookingEditableComparableFields(currentBooking);
      if (!bookingEditableFieldsAreEqual(currentComparableFields, preparedUpdate.comparableFields)) {
        await applyPreparedBookingEditableUpdate(connection, bookingId, preparedUpdate);
      }
    }

    await connection.query(
      `
      UPDATE booking_change_request
      SET status = ?,
          resolved_at = ?
      WHERE id = ?
      `,
      [action, toDbDateTime(new Date()), changeRequestId]
    );

    if (action === 'accepted') {
      await expirePendingBookingChangeRequestsForBooking(connection, currentBooking, {
        force: true,
        now: new Date(),
      });
    }

    await connection.commit();
    shouldNotifyRequester = action !== 'canceled' || isStaff;

    if (shouldNotifyRequester) {
      try {
        await sendBookingChangeRequestNotificationEmail({
          changeRequestId,
          mode: action,
        });
      } catch (emailError) {
        console.error('Error sending booking change request resolution email:', {
          bookingId,
          changeRequestId,
          error: emailError.message,
        });
      }
    }

    return res.status(200).json({
      message: action === 'accepted'
        ? 'Solicitud aceptada.'
        : action === 'rejected'
          ? 'Solicitud rechazada.'
          : 'Solicitud cancelada.',
    });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    if (error?.statusCode) {
      return res.status(error.statusCode).json({ error: error.message });
    }
    console.error('Error al responder la solicitud de modificación:', error);
    return res.status(500).json({ error: 'Error al responder la solicitud de modificación.' });
  } finally {
    connection.release();
  }
});

// Actualizar datos de una reserva
app.patch('/api/bookings/:id/update-data', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  let refundRequest = null;
  let lifecycleEmailKind = null;
  let lifecycleEmailBooking = null;
  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        b.id,
        b.client_user_id,
        b.provider_user_id_snapshot,
        b.service_status,
        b.settlement_status,
        b.order_datetime AS created_at,
        b.order_datetime,
        b.requested_start_datetime,
        b.requested_duration_minutes,
        b.last_minute_window_starts_at,
        b.expires_at,
        b.accepted_at,
        b.price_type_snapshot,
        b.service_currency_snapshot,
        b.unit_price_amount_cents_snapshot,
        b.estimated_total_amount_cents,
        b.deposit_amount_cents_snapshot,
        b.cancellation_reason_code
      FROM booking b
      WHERE b.id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!booking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const isClientOwner = req.user && Number(req.user.id) === Number(booking.client_user_id);
    const isProviderOwner = req.user && Number(req.user.id) === Number(booking.provider_user_id_snapshot);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isClientOwner && !isProviderOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'No autorizado.' });
    }

    if (canAutoExpireRequestedBooking(booking)) {
      const expiredTransition = await transitionBookingWithOptionalDepositRefund(connection, booking, {
        nextServiceStatus: 'expired',
        nextSettlementStatus: booking.settlement_status,
        reasonCode: 'request_expired',
        requestDepositRefund: true,
      });
      refundRequest = expiredTransition.refundRequest;
      await connection.commit();

      if (refundRequest?.paymentIntentId) {
        try {
          await triggerStripeRefundForPaymentIntent(refundRequest.paymentIntentId, {
            booking_id: String(refundRequest.bookingId),
            source: 'booking_expired',
          });
        } catch (refundError) {
          console.error('Error refunding expired booking deposit during status update:', {
            bookingId: refundRequest.bookingId,
            paymentIntentId: refundRequest.paymentIntentId,
            error: refundError.message,
          });
        }
      }

      return res.status(409).json({ error: 'La solicitud ha expirado.' });
    }

    const mappedLegacyStatus = typeof req.body.status !== 'undefined'
      ? normalizeLegacyStatusUpdate(req.body.status, booking)
      : {};

    let nextServiceStatus = typeof req.body.service_status !== 'undefined'
      ? normalizeServiceStatus(req.body.service_status, booking.service_status)
      : (mappedLegacyStatus.serviceStatus || booking.service_status);
    let nextSettlementStatus = typeof req.body.settlement_status !== 'undefined'
      ? normalizeSettlementStatus(req.body.settlement_status, booking.settlement_status)
      : (mappedLegacyStatus.settlementStatus || booking.settlement_status);

    if (typeof req.body.is_paid !== 'undefined') {
      nextSettlementStatus = req.body.is_paid ? 'paid' : (
        normalizeServiceStatus(nextServiceStatus, booking.service_status) === 'finished'
          ? 'awaiting_payment'
          : 'none'
      );
    }

    const extraPatch = {};
    if (typeof req.body.address_id !== 'undefined') {
      const nextAddressId = normalizeNullableInteger(req.body.address_id);
      if (nextAddressId !== null) {
        const [[existingAddress]] = await connection.query(
          'SELECT id FROM address WHERE id = ? LIMIT 1 FOR UPDATE',
          [nextAddressId]
        );
        if (!existingAddress) {
          await connection.rollback();
          return res.status(400).json({ error: 'address_id no existe.' });
        }
      }
      extraPatch.address_id = nextAddressId;
    }

    const explicitCancellationReason = typeof req.body.cancellation_reason_code === 'string'
      ? req.body.cancellation_reason_code.trim()
      : null;
    const explicitCancellationNote = typeof req.body.cancellation_note === 'string'
      ? req.body.cancellation_note.trim()
      : null;
    const mappedCancellationReason = mappedLegacyStatus.cancellationReasonCode || null;
    if (explicitCancellationReason || mappedCancellationReason) {
      extraPatch.cancellation_reason_code = explicitCancellationReason || mappedCancellationReason;
    }
    if (explicitCancellationNote !== null) {
      extraPatch.cancellation_note = explicitCancellationNote || null;
    }
    if (
      normalizeServiceStatus(nextServiceStatus, booking.service_status) !== 'canceled'
      && normalizeServiceStatus(nextServiceStatus, booking.service_status) !== 'expired'
      && Object.prototype.hasOwnProperty.call(extraPatch, 'cancellation_reason_code')
    ) {
      extraPatch.cancellation_reason_code = null;
    }

    const {
      normalizedCurrentServiceStatus,
      normalizedNextServiceStatus,
    } = assertBookingStatusUpdateAllowed({
      booking,
      nextServiceStatus,
      isClientOwner,
      isProviderOwner,
      isStaff,
      cancellationReasonCode: extraPatch.cancellation_reason_code || mappedCancellationReason,
    });

    if (
      normalizedNextServiceStatus === 'canceled'
      && normalizedCurrentServiceStatus === 'accepted'
    ) {
      const isLastMinuteCancellation = isWithinLastMinuteWindow({
        createdAt: booking.order_datetime ?? booking.created_at,
        requestedStartDateTime: booking.requested_start_datetime,
        lastMinuteWindowStartsAt: booking.last_minute_window_starts_at,
        now: new Date(),
      });

      if (isClientOwner && isLastMinuteCancellation) {
        await connection.rollback();
        const outcome = await processCanceledBookingIssueOutcome(bookingId, {
          issueType: 'last_minute_client',
          changedByUserId: req.user?.id || null,
          details: 'Cancelación del cliente dentro de la ventana last minute.',
        });
        if (!outcome.handled) {
          return res.status(500).json({ error: 'No se pudo procesar la cancelación last minute del cliente.' });
        }
        return res.status(200).json({ message: 'Estado actualizado' });
      }

      if (isProviderOwner && isLastMinuteCancellation) {
        await connection.rollback();
        const outcome = await processCanceledBookingIssueOutcome(bookingId, {
          issueType: 'last_minute_provider',
          changedByUserId: req.user?.id || null,
          details: 'Cancelación del profesional dentro de la ventana last minute.',
        });
        if (!outcome.handled) {
          return res.status(500).json({ error: 'No se pudo procesar la cancelación last minute del profesional.' });
        }
        return res.status(200).json({ message: 'Estado actualizado' });
      }
    }

    if (
      normalizedNextServiceStatus === 'canceled'
      && normalizedCurrentServiceStatus === 'in_progress'
    ) {
      await connection.rollback();
      const outcome = await openBookingIssueDispute(bookingId, {
        issueType: 'general_problem',
        reportedByUserId: req.user?.id || null,
        reportedAgainstUserId: isClientOwner
          ? booking.provider_user_id_snapshot
          : booking.client_user_id,
        details: isClientOwner
          ? 'El cliente ha solicitado cancelar la reserva durante el servicio.'
          : 'El profesional ha solicitado cancelar la reserva durante el servicio.',
        reasonCode: 'in_progress_cancellation_requested',
      });
      if (!outcome.opened) {
        return res.status(500).json({ error: 'No se pudo enviar la reserva a disputa.' });
      }
      return res.status(200).json({ message: 'Reserva enviada a disputa.' });
    }

    if (normalizedNextServiceStatus === 'canceled') {
      extraPatch.canceled_by_user_id = req.user?.id || null;
      if (!extraPatch.cancellation_reason_code && normalizedCurrentServiceStatus === 'requested') {
        extraPatch.cancellation_reason_code = isClientOwner
          ? 'client_canceled_request'
          : 'provider_canceled_request';
      }
    }

    const bookingCurrency = normalizeCurrencyCode(booking.service_currency_snapshot, 'EUR');
    const unitPriceAmount = booking.unit_price_amount_cents_snapshot === null || booking.unit_price_amount_cents_snapshot === undefined
      ? 0
      : fromMinorUnits(booking.unit_price_amount_cents_snapshot, bookingCurrency);

    if (
      Object.prototype.hasOwnProperty.call(req.body, 'service_duration')
      || Object.prototype.hasOwnProperty.call(req.body, 'requested_duration_minutes')
      || Object.prototype.hasOwnProperty.call(req.body, 'final_price')
    ) {
      const priceTypeSnapshot = String(booking.price_type_snapshot || '').trim().toLowerCase();
      if (priceTypeSnapshot === 'hour' || priceTypeSnapshot === 'budget') {
        const proposedDurationMinutes = normalizeDurationMinutes(
          req.body.service_duration ?? req.body.requested_duration_minutes ?? booking.requested_duration_minutes
        );
        if (priceTypeSnapshot === 'hour' && !isDurationMinutesInRange(proposedDurationMinutes)) {
          await connection.rollback();
          return res.status(400).json({
            error: `La duración debe estar entre ${MIN_BOOKING_DURATION_MINUTES} y ${MAX_BOOKING_DURATION_MINUTES} minutos.`,
          });
        }

        let proposedBaseAmountCents = 0;
        if (priceTypeSnapshot === 'hour') {
          const hourlyPricing = computeBookingPricingSnapshot({
            priceType: 'hour',
            unitPrice: unitPriceAmount,
            durationMinutes: proposedDurationMinutes,
            currency: bookingCurrency,
          });
          proposedBaseAmountCents = toMinorUnits(hourlyPricing.base, bookingCurrency);
        } else {
          const proposedBudgetBase = Number(req.body.final_price ?? 0);
          if (!Number.isFinite(proposedBudgetBase) || proposedBudgetBase < 0) {
            await connection.rollback();
            return res.status(400).json({ error: 'final_price inválido.' });
          }
          proposedBaseAmountCents = toMinorUnits(proposedBudgetBase, bookingCurrency);
        }

        await upsertActiveClosureProposal(connection, {
          booking: {
            ...booking,
            id: bookingId,
          },
          createdByUserId: req.user?.id || null,
          proposedBaseAmountCents,
          proposedFinalDurationMinutes: priceTypeSnapshot === 'hour' ? proposedDurationMinutes : null,
        });

        if (
          normalizeServiceStatus(nextServiceStatus, booking.service_status) === 'finished'
          && normalizeSettlementStatus(nextSettlementStatus, booking.settlement_status) === 'none'
        ) {
          nextSettlementStatus = 'awaiting_payment';
        }
      }
    }

    const shouldRequestDepositRefund = (
      normalizedCurrentServiceStatus === 'requested'
      && (normalizedNextServiceStatus === 'canceled' || normalizedNextServiceStatus === 'expired')
    ) || (
      normalizedNextServiceStatus === 'canceled'
      && normalizedCurrentServiceStatus === 'accepted'
    );
    const transitionResult = await transitionBookingWithOptionalDepositRefund(connection, booking, {
      nextServiceStatus,
      nextSettlementStatus,
      changedByUserId: req.user?.id || null,
      reasonCode: typeof req.body.status === 'string' ? String(req.body.status).trim().toLowerCase() : null,
      extraPatch,
      requestDepositRefund: shouldRequestDepositRefund,
    });
    refundRequest = transitionResult.refundRequest;

    if (!transitionResult.changed && Object.keys(transitionResult.appliedPatch || {}).length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: 'No fields to update.' });
    }

    await connection.commit();

    if (normalizedCurrentServiceStatus !== normalizedNextServiceStatus) {
      if (normalizedCurrentServiceStatus === 'requested' && normalizedNextServiceStatus === 'accepted') {
        lifecycleEmailKind = 'accepted';
      } else if (normalizedCurrentServiceStatus === 'accepted' && normalizedNextServiceStatus === 'in_progress') {
        lifecycleEmailKind = 'started';
      }
    }

    if (lifecycleEmailKind) {
      try {
        lifecycleEmailBooking = await getBookingLifecycleNotificationContext(connection, bookingId);
      } catch (notificationContextError) {
        console.error('Error building booking lifecycle notification context:', {
          bookingId,
          error: notificationContextError.message,
        });
      }
    }

    if (normalizedCurrentServiceStatus === 'requested' && normalizedNextServiceStatus === 'accepted') {
      await syncPreBookingConversationUnlock({
        serviceId: booking.service_id,
        clientUserId: booking.client_user_id,
        providerUserId: booking.provider_user_id_snapshot,
        bookingId,
      });
    }

    if (refundRequest?.paymentIntentId) {
      try {
        await triggerStripeRefundForPaymentIntent(refundRequest.paymentIntentId, {
          booking_id: String(refundRequest.bookingId),
          source: normalizedNextServiceStatus === 'expired' ? 'booking_expired' : 'booking_canceled',
        });
      } catch (refundError) {
        console.error('Error refunding booking deposit after state update:', {
          bookingId: refundRequest.bookingId,
          paymentIntentId: refundRequest.paymentIntentId,
          error: refundError.message,
        });
      }
    }

    if (normalizedNextServiceStatus === 'canceled' && !refundRequest?.paymentIntentId) {
      try {
        await releaseEphemeralBookingPaymentMethodsIfClosed(bookingId);
      } catch (cleanupError) {
        console.error('Error releasing ephemeral payment methods after booking cancellation:', {
          bookingId,
          error: cleanupError.message,
        });
      }
    }

    if (lifecycleEmailKind && lifecycleEmailBooking) {
      try {
        await sendBookingLifecycleNotificationEmail({
          kind: lifecycleEmailKind,
          booking: lifecycleEmailBooking,
        });
      } catch (notificationError) {
        console.error('Error sending booking lifecycle notification email:', {
          bookingId,
          kind: lifecycleEmailKind,
          error: notificationError.message,
        });
      }
    }

    return res.status(200).json({ message: 'Estado actualizado' });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    if (error?.statusCode === 400 && error?.message) {
      return res.status(400).json({ error: error.message });
    }
    console.error('Error al actualizar el estado de la reserva:', error);
    return res.status(500).json({ error: 'Error al actualizar la reserva.' });
  } finally {
    connection.release();
  }
});

app.post('/api/bookings/:id/closure-proposal', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        id,
        client_user_id,
        provider_user_id_snapshot,
        service_status,
        settlement_status,
        requested_start_datetime,
        requested_end_datetime,
        requested_duration_minutes,
        price_type_snapshot,
        service_currency_snapshot,
        unit_price_amount_cents_snapshot,
        estimated_base_amount_cents,
        estimated_commission_amount_cents,
        estimated_total_amount_cents,
        deposit_amount_cents_snapshot
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!booking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const isProviderOwner = req.user && Number(req.user.id) === Number(booking.provider_user_id_snapshot);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isProviderOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'Solo el profesional puede enviar la propuesta de cierre.' });
    }

    const normalizedServiceStatus = normalizeServiceStatus(booking.service_status, 'pending_deposit');
    const normalizedSettlementStatus = normalizeSettlementStatus(booking.settlement_status, 'none');
    if (normalizedServiceStatus !== 'in_progress') {
      await connection.rollback();
      return res.status(400).json({ error: 'Solo se puede cerrar una reserva en progreso.' });
    }
    if (['pending_client_approval', 'awaiting_payment', 'paid', 'in_dispute', 'manual_review_required'].includes(normalizedSettlementStatus)) {
      await connection.rollback();
      return res.status(409).json({ error: 'La reserva ya tiene un cierre en curso.' });
    }

    const latestProposal = await getLatestClosureProposal(connection, bookingId, { forUpdate: true });
    if (latestProposal?.status === 'active') {
      await connection.rollback();
      return res.status(409).json({ error: 'Ya existe una propuesta de cierre activa.' });
    }

    const bookingCurrency = normalizeCurrencyCode(booking.service_currency_snapshot, 'EUR');
    const priceTypeSnapshot = String(booking.price_type_snapshot || '').trim().toLowerCase();
    const unitPriceAmount = booking.unit_price_amount_cents_snapshot == null
      ? 0
      : fromMinorUnits(booking.unit_price_amount_cents_snapshot, bookingCurrency);
    const zeroChargeMode = req.body?.zero_charge_mode === true || req.body?.zero_charge_mode === 1;

    let proposedBaseAmountCents = 0;
    let proposedFinalDurationMinutes = null;

    if (!zeroChargeMode) {
      if (priceTypeSnapshot === 'hour') {
        proposedFinalDurationMinutes = normalizeDurationMinutes(
          req.body?.proposed_final_duration_minutes
          ?? req.body?.service_duration
          ?? req.body?.requested_duration_minutes
          ?? booking.requested_duration_minutes
        );

        if (!isDurationMinutesInRange(proposedFinalDurationMinutes)) {
          await connection.rollback();
          return res.status(400).json({
            error: `La duración debe estar entre ${MIN_BOOKING_DURATION_MINUTES} y ${MAX_BOOKING_DURATION_MINUTES} minutos.`,
          });
        }

        const hourlyPricing = computeBookingPricingSnapshot({
          priceType: 'hour',
          unitPrice: unitPriceAmount,
          durationMinutes: proposedFinalDurationMinutes,
          currency: bookingCurrency,
        });
        proposedBaseAmountCents = toMinorUnits(hourlyPricing.base, bookingCurrency);
      } else if (priceTypeSnapshot === 'budget') {
        const proposedBudgetBase = Number(req.body?.proposed_final_price ?? req.body?.final_price);
        if (!Number.isFinite(proposedBudgetBase) || proposedBudgetBase < 0) {
          await connection.rollback();
          return res.status(400).json({ error: 'final_price inválido.' });
        }
        proposedBaseAmountCents = toMinorUnits(proposedBudgetBase, bookingCurrency);
      } else {
        const fallbackPricing = computeBookingPricingSnapshot({
          priceType: priceTypeSnapshot === 'budget' ? 'budget' : 'fix',
          unitPrice: unitPriceAmount,
          durationMinutes: booking.requested_duration_minutes || 0,
          currency: bookingCurrency,
        });
        proposedBaseAmountCents = Number(
          booking.estimated_base_amount_cents
          ?? toMinorUnits(fallbackPricing.base, bookingCurrency)
          ?? 0
        );
      }
    }

    const proposalId = await upsertActiveClosureProposal(connection, {
      booking: {
        ...booking,
        id: bookingId,
      },
      createdByUserId: req.user?.id || null,
      proposedBaseAmountCents,
      proposedFinalDurationMinutes,
      zeroChargeMode,
    });

    const shouldCompleteImmediately = zeroChargeMode || proposedBaseAmountCents <= 0;
    if (shouldCompleteImmediately) {
      await updateClosureProposalStatus(connection, proposalId, 'accepted', new Date());
      await connection.commit();

      const settlementResult = await finalizeBookingSettlementAfterSuccessfulPayment(bookingId, {
        changedByUserId: req.user?.id || null,
        reasonCode: zeroChargeMode ? 'closure_zero_charge_completed' : 'closure_zero_amount_completed',
      });
      if (!settlementResult.settled) {
        return res.status(500).json({ error: 'No se pudo completar automáticamente el cierre.' });
      }

      return res.status(200).json({
        message: 'Reserva completada automáticamente.',
        closure_proposal_id: proposalId,
        zero_charge_completed: true,
      });
    }

    await transitionBookingStateRecord(connection, booking, {
      nextSettlementStatus: 'pending_client_approval',
      changedByUserId: req.user?.id || null,
      reasonCode: 'closure_proposal_sent',
      extraPatch: {
        client_approval_deadline_at: buildClientApprovalDeadline(),
      },
    });

    await connection.commit();
    return res.status(200).json({
      message: 'Propuesta de cierre enviada.',
      closure_proposal_id: proposalId,
    });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error al crear la propuesta de cierre:', error);
    return res.status(500).json({ error: 'Error al crear la propuesta de cierre.' });
  } finally {
    connection.release();
  }
});

app.post('/api/bookings/:id/closure-proposal/revoke', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        id,
        client_user_id,
        provider_user_id_snapshot,
        service_status,
        settlement_status
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!booking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const isProviderOwner = req.user && Number(req.user.id) === Number(booking.provider_user_id_snapshot);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isProviderOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'Solo el profesional puede revocar esta propuesta.' });
    }

    const latestProposal = await getLatestClosureProposal(connection, bookingId, { forUpdate: true });
    if (!latestProposal || latestProposal.status !== 'active') {
      await connection.rollback();
      return res.status(409).json({ error: 'No hay ninguna propuesta activa para revocar.' });
    }

    if (normalizeSettlementStatus(booking.settlement_status, 'none') !== 'pending_client_approval') {
      await connection.rollback();
      return res.status(409).json({ error: 'La propuesta ya no puede revocarse.' });
    }

    await updateClosureProposalStatus(connection, latestProposal.id, 'revoked', new Date());
    await transitionBookingStateRecord(connection, booking, {
      nextServiceStatus: 'in_progress',
      nextSettlementStatus: 'none',
      changedByUserId: req.user?.id || null,
      reasonCode: 'closure_proposal_revoked',
      extraPatch: {
        client_approval_deadline_at: null,
      },
    });

    await connection.commit();
    return res.status(200).json({ message: 'Propuesta de cierre anulada.' });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error al revocar la propuesta de cierre:', error);
    return res.status(500).json({ error: 'Error al revocar la propuesta de cierre.' });
  } finally {
    connection.release();
  }
});

app.post('/api/bookings/:id/dispute', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        id,
        client_user_id,
        provider_user_id_snapshot,
        service_status,
        settlement_status
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!booking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const isClientOwner = req.user && Number(req.user.id) === Number(booking.client_user_id);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isClientOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'Solo el cliente puede abrir una disputa de cierre.' });
    }

    const normalizedSettlementStatus = normalizeSettlementStatus(booking.settlement_status, 'none');
    if (!['pending_client_approval', 'awaiting_payment'].includes(normalizedSettlementStatus)) {
      await connection.rollback();
      return res.status(409).json({ error: 'La reserva no está en un estado disputable.' });
    }

    const latestProposal = await getLatestClosureProposal(connection, bookingId, { forUpdate: true });
    if (normalizedSettlementStatus === 'pending_client_approval') {
      if (!latestProposal || latestProposal.status !== 'active') {
        await connection.rollback();
        return res.status(409).json({ error: 'La propuesta de cierre ya no está disponible.' });
      }
      await updateClosureProposalStatus(connection, latestProposal.id, 'rejected', new Date());
    }

    const disputeDetails = typeof req.body?.details === 'string' && req.body.details.trim()
      ? req.body.details.trim()
      : 'Client requested manual review for the closure proposal.';
    const reportedAgainstUserId = Number.isInteger(Number(booking.provider_user_id_snapshot))
      ? Number(booking.provider_user_id_snapshot)
      : null;

    const [insertResult] = await connection.query(
      `
      INSERT INTO booking_issue_report
        (booking_id, reported_by_user_id, reported_against_user_id, issue_type, status, details)
      VALUES (?, ?, ?, 'payment_dispute', 'open', ?)
      `,
      [bookingId, req.user?.id || null, reportedAgainstUserId, disputeDetails]
    );

    await transitionBookingStateRecord(connection, booking, {
      nextServiceStatus: 'finished',
      nextSettlementStatus: 'in_dispute',
      changedByUserId: req.user?.id || null,
      reasonCode: 'payment_dispute_opened',
      extraPatch: {
        client_approval_deadline_at: null,
      },
    });

    await connection.commit();

    try {
      await sendBookingSupportAlertEmail({
        bookingId,
        headline: 'Disputa abierta por el cliente',
        details: disputeDetails,
        category: 'payment_dispute',
      });
    } catch (emailError) {
      console.error('Error sending booking dispute support alert:', {
        bookingId,
        error: emailError.message,
      });
    }

    return res.status(201).json({
      message: 'Disputa abierta.',
      issue_report: {
        id: insertResult.insertId,
        booking_id: bookingId,
        issue_type: 'payment_dispute',
        status: 'open',
        details: disputeDetails,
      },
    });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error al abrir la disputa de la reserva:', error);
    return res.status(500).json({ error: 'Error al abrir la disputa.' });
  } finally {
    connection.release();
  }
});

app.post('/api/bookings/:id/dispute/cancel', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        id,
        client_user_id,
        provider_user_id_snapshot,
        service_status,
        settlement_status
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!booking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const [[openIssue]] = await connection.query(
      `
      SELECT
        id,
        issue_type,
        status,
        reported_by_user_id,
        details
      FROM booking_issue_report
      WHERE booking_id = ?
        AND status = 'open'
      ORDER BY created_at DESC, id DESC
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!openIssue) {
      await connection.rollback();
      return res.status(409).json({ error: 'No hay ninguna disputa abierta para esta reserva.' });
    }

    const isReporter = req.user && Number(req.user.id) === Number(openIssue.reported_by_user_id);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isReporter && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'Solo el creador de la disputa puede anularla.' });
    }

    await connection.query(
      `
      UPDATE booking_issue_report
      SET status = 'dismissed',
          resolved_at = ?,
          details = COALESCE(?, details)
      WHERE id = ?
      `,
      [
        toDbDateTime(new Date()),
        normalizeNullableText(req.body?.details) || null,
        openIssue.id,
      ]
    );

    const normalizedIssueType = String(openIssue.issue_type || '').trim().toLowerCase();
    if (normalizedIssueType === 'payment_dispute') {
      const latestProposal = await getLatestClosureProposal(connection, bookingId, { forUpdate: true });
      const shouldRestorePendingApproval = latestProposal && latestProposal.status === 'rejected';

      if (shouldRestorePendingApproval) {
        await updateClosureProposalStatus(connection, latestProposal.id, 'active', new Date());
      }

      await transitionBookingStateRecord(connection, booking, {
        nextServiceStatus: booking.service_status,
        nextSettlementStatus: shouldRestorePendingApproval ? 'pending_client_approval' : 'awaiting_payment',
        changedByUserId: req.user?.id || null,
        reasonCode: 'booking_dispute_canceled',
        extraPatch: {
          client_approval_deadline_at: shouldRestorePendingApproval
            ? buildClientApprovalDeadline()
            : null,
        },
      });
    } else if (
      ['in_dispute', 'manual_review_required'].includes(
        normalizeSettlementStatus(booking.settlement_status, 'none')
      )
    ) {
      await transitionBookingStateRecord(connection, booking, {
        nextServiceStatus: booking.service_status,
        nextSettlementStatus: 'none',
        changedByUserId: req.user?.id || null,
        reasonCode: 'booking_dispute_canceled',
        extraPatch: {
          client_approval_deadline_at: null,
        },
      });
    }

    await connection.commit();
    return res.status(200).json({
      message: 'Disputa anulada.',
      issue_report: {
        id: openIssue.id,
        booking_id: bookingId,
        issue_type: normalizedIssueType,
        status: 'dismissed',
      },
    });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error al anular la disputa de la reserva:', error);
    return res.status(500).json({ error: 'Error al anular la disputa.' });
  } finally {
    connection.release();
  }
});

app.post('/api/bookings/:id/issues', authenticateToken, async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();

    const [[booking]] = await connection.query(
      `
      SELECT
        id,
        client_user_id,
        provider_user_id_snapshot,
        service_status,
        settlement_status,
        requested_start_datetime
      FROM booking
      WHERE id = ?
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (!booking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const reporterUserId = normalizeNullableInteger(req.user?.id);
    const isClientOwner = reporterUserId !== null && reporterUserId === Number(booking.client_user_id);
    const isProviderOwner = reporterUserId !== null && reporterUserId === Number(booking.provider_user_id_snapshot);

    if (!isClientOwner && !isProviderOwner) {
      await connection.rollback();
      return res.status(403).json({ error: 'No autorizado.' });
    }

    if (!canReportBookingIssue(booking)) {
      await connection.rollback();
      return res.status(400).json({
        error: 'La incidencia solo se puede reportar cuando la reserva está en progreso, no tiene fecha de inicio o ya ha pasado la hora de inicio.',
      });
    }

    const [[existingOpenIssue]] = await connection.query(
      `
      SELECT id
      FROM booking_issue_report
      WHERE booking_id = ? AND status = 'open'
      ORDER BY created_at DESC, id DESC
      LIMIT 1
      FOR UPDATE
      `,
      [bookingId]
    );

    if (existingOpenIssue) {
      await connection.rollback();
      return res.status(409).json({ error: 'Ya existe una incidencia abierta para esta reserva.' });
    }

    const requestedIssueType = String(req.body?.issue_type || '').trim().toLowerCase();
    let issueType = requestedIssueType || 'general_problem';
    if (issueType === 'no_show') {
      issueType = isProviderOwner ? 'no_show_client' : 'no_show_provider';
    }

    const allowedIssueTypes = new Set(['general_problem']);
    if (isProviderOwner) {
      allowedIssueTypes.add('no_show_client');
    }
    if (isClientOwner) {
      allowedIssueTypes.add('no_show_provider');
    }

    if (!allowedIssueTypes.has(issueType)) {
      await connection.rollback();
      return res.status(400).json({ error: 'issue_type inválido para este usuario.' });
    }

    const defaultDetailsByType = {
      general_problem: 'Incidencia reportada desde la app.',
      no_show_client: 'El cliente no se ha presentado.',
      no_show_provider: 'El profesional no se ha presentado.',
    };
    const details = typeof req.body?.details === 'string' && req.body.details.trim()
      ? req.body.details.trim()
      : defaultDetailsByType[issueType];
    const reportedAgainstUserId = isClientOwner
      ? booking.provider_user_id_snapshot
      : booking.client_user_id;

    if (issueType === 'no_show_client' || issueType === 'no_show_provider') {
      await connection.rollback();
      const outcome = await openBookingIssueDispute(bookingId, {
        issueType,
        reportedByUserId: reporterUserId,
        reportedAgainstUserId: reportedAgainstUserId || null,
        details,
        reasonCode: 'no_show_dispute_opened',
      });
      if (!outcome.opened) {
        return res.status(500).json({ error: 'No se pudo abrir la disputa automáticamente.' });
      }
      return res.status(201).json({
        message: 'Incidencia registrada y enviada a disputa.',
        issue_report: {
          booking_id: bookingId,
          issue_type: issueType,
          status: 'open',
          details,
        },
      });
    }

    const [insertResult] = await connection.query(
      `
      INSERT INTO booking_issue_report
        (booking_id, reported_by_user_id, reported_against_user_id, issue_type, status, details)
      VALUES (?, ?, ?, ?, 'open', ?)
      `,
      [
        bookingId,
        reporterUserId,
        reportedAgainstUserId || null,
        issueType,
        details,
      ]
    );

    await connection.commit();

    try {
      await sendBookingSupportAlertEmail({
        bookingId,
        headline: 'Nueva incidencia abierta en una reserva',
        details,
        category: issueType,
      });
    } catch (emailError) {
      console.error('Error sending booking issue support alert:', {
        bookingId,
        error: emailError.message,
      });
    }

    return res.status(201).json({
      message: 'Incidencia registrada.',
      issue_report: {
        id: insertResult.insertId,
        booking_id: bookingId,
        issue_type: issueType,
        status: 'open',
        details,
      },
    });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error al registrar la incidencia de la reserva:', error);
    return res.status(500).json({ error: 'Error al registrar la incidencia.' });
  } finally {
    connection.release();
  }
});

app.get('/api/booking-support/cases', authenticateToken, async (req, res) => {
  const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
  if (!isStaff) {
    return res.status(403).json({ error: 'No autorizado.' });
  }

  const statusFilter = String(req.query?.status || 'open').trim().toLowerCase();
  if (!['open', 'resolved', 'dismissed', 'all'].includes(statusFilter)) {
    return res.status(400).json({ error: 'status inválido.' });
  }

  const limit = Math.min(100, Math.max(1, Number(req.query?.limit || 50)));
  const offset = Math.max(0, Number(req.query?.offset || 0));
  const issueJoinStatusClause = statusFilter === 'all' ? '' : 'AND bir2.status = ?';
  const issueExistsStatusClause = statusFilter === 'all' ? '' : 'AND bir3.status = ?';
  const params = [];
  if (statusFilter !== 'all') {
    params.push(statusFilter, statusFilter);
  }
  params.push(limit, offset);

  try {
    const [rows] = await pool.promise().query(
      `
      SELECT
        b.id AS booking_id,
        b.service_status,
        b.settlement_status,
        b.order_datetime,
        b.updated_at,
        COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
        client.id AS client_user_id,
        client.email AS client_email,
        client.first_name AS client_first_name,
        client.surname AS client_surname,
        provider.id AS provider_user_id,
        provider.email AS provider_email,
        provider.first_name AS provider_first_name,
        provider.surname AS provider_surname,
        bir.id AS issue_report_id,
        bir.issue_type,
        bir.status AS issue_status,
        bir.details AS issue_details,
        bir.created_at AS issue_created_at,
        bir.resolved_at AS issue_resolved_at
      FROM booking b
      LEFT JOIN service s ON s.id = b.service_id
      LEFT JOIN user_account client ON client.id = b.client_user_id
      LEFT JOIN user_account provider ON provider.id = b.provider_user_id_snapshot
      LEFT JOIN booking_issue_report bir ON bir.id = (
        SELECT bir2.id
        FROM booking_issue_report bir2
        WHERE bir2.booking_id = b.id
          ${issueJoinStatusClause}
        ORDER BY
          CASE WHEN bir2.status = 'open' THEN 0 ELSE 1 END,
          bir2.created_at DESC,
          bir2.id DESC
        LIMIT 1
      )
      WHERE (
        b.settlement_status IN ('manual_review_required', 'in_dispute')
        OR EXISTS (
          SELECT 1
          FROM booking_issue_report bir3
          WHERE bir3.booking_id = b.id
            ${issueExistsStatusClause}
        )
      )
      ORDER BY
        CASE WHEN b.settlement_status IN ('manual_review_required', 'in_dispute') THEN 0 ELSE 1 END,
        COALESCE(bir.created_at, b.updated_at, b.order_datetime) DESC
      LIMIT ?
      OFFSET ?
      `,
      params
    );

    return res.status(200).json((rows || []).map((row) => ({
      booking_id: row.booking_id,
      service_title: row.service_title,
      service_status: normalizeServiceStatus(row.service_status, 'pending_deposit'),
      settlement_status: normalizeSettlementStatus(row.settlement_status, 'none'),
      needs_manual_review: ['manual_review_required', 'in_dispute'].includes(
        normalizeSettlementStatus(row.settlement_status, 'none')
      ),
      client: {
        id: normalizeNullableInteger(row.client_user_id),
        email: row.client_email || null,
        name: composeDisplayName({
          firstName: row.client_first_name,
          surname: row.client_surname,
          email: row.client_email,
        }),
      },
      provider: {
        id: normalizeNullableInteger(row.provider_user_id),
        email: row.provider_email || null,
        name: composeDisplayName({
          firstName: row.provider_first_name,
          surname: row.provider_surname,
          email: row.provider_email,
        }),
      },
      issue_report: row.issue_report_id
        ? {
          id: row.issue_report_id,
          issue_type: row.issue_type,
          status: row.issue_status,
          details: row.issue_details,
          created_at: row.issue_created_at,
          resolved_at: row.issue_resolved_at,
        }
        : null,
    })));
  } catch (error) {
    console.error('Error al listar casos de soporte de reservas:', error);
    return res.status(500).json({ error: 'Error al listar los casos de soporte de reservas.' });
  }
});

app.patch('/api/booking-support/issues/:issueReportId', authenticateToken, async (req, res) => {
  const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
  if (!isStaff) {
    return res.status(403).json({ error: 'No autorizado.' });
  }

  const issueReportId = normalizeNullableInteger(req.params.issueReportId);
  if (!issueReportId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const nextStatus = String(req.body?.status || '').trim().toLowerCase();
  if (!['open', 'resolved', 'dismissed'].includes(nextStatus)) {
    return res.status(400).json({ error: 'status inválido.' });
  }

  const nextDetails = normalizeNullableText(req.body?.details);
  const resolution = String(req.body?.resolution || '').trim().toLowerCase();

  try {
    const [issueRows] = await pool.promise().query(
      `
      SELECT
        bir.id,
        bir.booking_id,
        bir.issue_type,
        bir.status,
        bir.details,
        b.service_status,
        b.settlement_status
      FROM booking_issue_report bir
      LEFT JOIN booking b ON b.id = bir.booking_id
      WHERE bir.id = ?
      LIMIT 1
      `,
      [issueReportId]
    );

    const issueRow = issueRows[0] || null;
    if (!issueRow) {
      return res.status(404).json({ error: 'Incidencia no encontrada.' });
    }

    if (
      nextStatus === 'resolved'
      && ['no_show_client', 'no_show_provider', 'general_problem'].includes(issueRow.issue_type)
    ) {
      if (
        issueRow.issue_type === 'general_problem'
        && !['refund_client_full', 'keep_deposit'].includes(resolution)
      ) {
        return res.status(400).json({
          error: 'resolution es requerida para incidencias general_problem. Usa refund_client_full o keep_deposit.',
        });
      }

      const outcome = await processCanceledBookingIssueOutcome(issueRow.booking_id, {
        issueType: issueRow.issue_type,
        changedByUserId: req.user?.id || null,
        details: nextDetails || issueRow.details,
        resolution: issueRow.issue_type === 'general_problem' ? resolution : null,
      });
      if (!outcome.handled) {
        return res.status(500).json({ error: 'No se pudo resolver la incidencia.' });
      }

      return res.status(200).json({ message: 'Incidencia resuelta.' });
    }

    if (
      nextStatus === 'dismissed'
      && ['no_show_client', 'no_show_provider', 'general_problem'].includes(issueRow.issue_type)
    ) {
      const connection = await pool.promise().getConnection();
      try {
        await connection.beginTransaction();

        const [[booking]] = await connection.query(
          'SELECT id, service_status, settlement_status FROM booking WHERE id = ? LIMIT 1 FOR UPDATE',
          [issueRow.booking_id]
        );

        await connection.query(
          `
          UPDATE booking_issue_report
          SET status = 'dismissed',
              details = COALESCE(?, details),
              resolved_at = ?
          WHERE id = ?
          `,
          [
            nextDetails,
            toDbDateTime(new Date()),
            issueReportId,
          ]
        );

        if (
          booking
          && ['in_dispute', 'manual_review_required'].includes(
            normalizeSettlementStatus(booking.settlement_status, 'none')
          )
        ) {
          await transitionBookingStateRecord(connection, booking, {
            nextServiceStatus: booking.service_status,
            nextSettlementStatus: 'none',
            changedByUserId: req.user?.id || null,
            reasonCode: 'booking_issue_dismissed',
            extraPatch: {
              client_approval_deadline_at: null,
            },
          });
        }

        await connection.commit();
        return res.status(200).json({ message: 'Incidencia desestimada.' });
      } catch (error) {
        try { await connection.rollback(); } catch {}
        console.error('Error dismissing booking support issue:', error);
        return res.status(500).json({ error: 'Error al desestimar la incidencia.' });
      } finally {
        connection.release();
      }
    }

    const [result] = await pool.promise().query(
      `
      UPDATE booking_issue_report
      SET status = ?,
          details = COALESCE(?, details),
          resolved_at = ?
      WHERE id = ?
      `,
      [
        nextStatus,
        nextDetails,
        nextStatus === 'resolved' || nextStatus === 'dismissed'
          ? toDbDateTime(new Date())
          : null,
        issueReportId,
      ]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Incidencia no encontrada.' });
    }

    return res.status(200).json({ message: 'Incidencia actualizada.' });
  } catch (error) {
    console.error('Error al actualizar la incidencia de soporte:', error);
    return res.status(500).json({ error: 'Error al actualizar la incidencia de soporte.' });
  }
});

// Actualizar el pago de una reserva
app.patch('/api/bookings/:id/is_paid', authenticateToken, async (req, res) => {
  if (typeof req.body.is_paid === 'undefined') {
    return res.status(400).json({ error: 'is_paid es requerido.' });
  }

  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  const connection = await promisePool.getConnection();
  try {
    await connection.beginTransaction();
    const [[booking]] = await connection.query(
      'SELECT id, service_status, settlement_status FROM booking WHERE id = ? LIMIT 1 FOR UPDATE',
      [bookingId]
    );

    if (!booking) {
      await connection.rollback();
      return res.status(404).json({ error: 'Reserva no encontrada.' });
    }

    const nextSettlementStatus = req.body.is_paid ? 'paid' : (
      normalizeServiceStatus(booking.service_status, 'pending_deposit') === 'finished'
        ? 'awaiting_payment'
        : 'none'
    );

    await transitionBookingStateRecord(connection, booking, {
      nextSettlementStatus,
      changedByUserId: req.user?.id || null,
      reasonCode: 'manual_payment_update',
    });

    await connection.commit();
    if (nextSettlementStatus === 'paid') {
      try {
        await releaseEphemeralBookingPaymentMethodsIfClosed(bookingId);
      } catch (cleanupError) {
        console.error('Error releasing ephemeral payment methods after manual payment update:', {
          bookingId,
          error: cleanupError.message,
        });
      }
    }
    return res.status(200).json({ message: 'Pago actualizado' });
  } catch (error) {
    try { await connection.rollback(); } catch {}
    console.error('Error al actualizar el pago de la reserva:', error);
    return res.status(500).json({ error: 'Error al actualizar la reserva.' });
  } finally {
    connection.release();
  }
});

const COLLECTION_METHOD_BANK_COUNTRY_TO_CURRENCY = Object.freeze({
  AR: 'ars',
  AT: 'eur',
  AU: 'aud',
  BE: 'eur',
  BG: 'bgn',
  BR: 'brl',
  CA: 'cad',
  CH: 'chf',
  CY: 'eur',
  CZ: 'czk',
  DE: 'eur',
  DK: 'dkk',
  EE: 'eur',
  ES: 'eur',
  FI: 'eur',
  FR: 'eur',
  GB: 'gbp',
  GR: 'eur',
  HK: 'hkd',
  HR: 'eur',
  HU: 'huf',
  ID: 'idr',
  IE: 'eur',
  IL: 'ils',
  IN: 'inr',
  IT: 'eur',
  JP: 'jpy',
  KR: 'krw',
  LT: 'eur',
  LU: 'eur',
  LV: 'eur',
  MT: 'eur',
  MX: 'mxn',
  MY: 'myr',
  NL: 'eur',
  NO: 'nok',
  NZ: 'nzd',
  PH: 'php',
  PL: 'pln',
  PT: 'eur',
  RO: 'ron',
  SA: 'sar',
  SE: 'sek',
  SG: 'sgd',
  SI: 'eur',
  SK: 'eur',
  TH: 'thb',
  US: 'usd',
  VN: 'vnd',
  AE: 'aed',
});

const COLLECTION_METHOD_SUPPORTED_COUNTRIES = new Set([
  'AE',
  'AT',
  'BE',
  'BG',
  'CH',
  'CY',
  'CZ',
  'DE',
  'DK',
  'EE',
  'ES',
  'FI',
  'FR',
  'GB',
  'GR',
  'HR',
  'HU',
  'IE',
  'IT',
  'LT',
  'LU',
  'LV',
  'MT',
  'NL',
  'NO',
  'PL',
  'PT',
  'RO',
  'SA',
  'SE',
  'SI',
  'SK',
  'US',
]);

const COLLECTION_METHOD_IBAN_COUNTRY_LENGTHS = Object.freeze({
  AE: 23,
  AT: 20,
  BE: 16,
  BG: 22,
  CH: 21,
  CY: 28,
  CZ: 24,
  DE: 22,
  DK: 18,
  EE: 20,
  ES: 24,
  FI: 18,
  FR: 27,
  GB: 22,
  GR: 27,
  HR: 21,
  HU: 28,
  IE: 22,
  IT: 27,
  LT: 20,
  LU: 20,
  LV: 21,
  MT: 31,
  NL: 18,
  NO: 15,
  PL: 28,
  PT: 25,
  RO: 24,
  SA: 24,
  SE: 24,
  SI: 19,
  SK: 24,
});

function normalizeCollectionMethodCountry(value = '') {
  return String(value || '').trim().toUpperCase();
}

function isCollectionMethodCountrySupported(value = '') {
  return COLLECTION_METHOD_SUPPORTED_COUNTRIES.has(normalizeCollectionMethodCountry(value));
}

function normalizeCollectionMethodIban(value = '') {
  return String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function collectionMethodIbanToNumericString(value = '') {
  return [...String(value || '')].map((char) => {
    const code = char.charCodeAt(0);
    if (code >= 65 && code <= 90) {
      return String(code - 55);
    }
    return char;
  }).join('');
}

function computeCollectionMethodModulo97(value = '') {
  let remainder = 0;

  for (const char of String(value || '')) {
    remainder = (remainder * 10 + Number(char)) % 97;
  }

  return remainder;
}

function isValidCollectionMethodIban(value = '') {
  const normalizedIban = normalizeCollectionMethodIban(value);

  if (!/^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$/.test(normalizedIban)) {
    return false;
  }

  const countryCode = normalizedIban.slice(0, 2);
  const expectedLength = COLLECTION_METHOD_IBAN_COUNTRY_LENGTHS[countryCode];
  if (expectedLength && normalizedIban.length !== expectedLength) {
    return false;
  }

  const rearrangedValue = `${normalizedIban.slice(4)}${normalizedIban.slice(0, 4)}`;
  return computeCollectionMethodModulo97(collectionMethodIbanToNumericString(rearrangedValue)) === 1;
}

function normalizeCollectionMethodRoutingNumber(value = '') {
  return String(value || '').replace(/[^\d]/g, '').slice(0, 9);
}

function normalizeCollectionMethodUsAccountNumber(value = '') {
  return String(value || '').replace(/[^\d]/g, '').slice(0, 17);
}

function isValidCollectionMethodRoutingNumber(value = '') {
  const digits = normalizeCollectionMethodRoutingNumber(value);
  if (!/^\d{9}$/.test(digits)) {
    return false;
  }

  const numbers = digits.split('').map(Number);
  const checksum = (
    3 * (numbers[0] + numbers[3] + numbers[6]) +
    7 * (numbers[1] + numbers[4] + numbers[7]) +
    (numbers[2] + numbers[5] + numbers[8])
  ) % 10;

  return checksum === 0;
}

function isValidCollectionMethodUsAccountNumber(value = '') {
  return /^\d{4,17}$/.test(normalizeCollectionMethodUsAccountNumber(value));
}

function getCollectionMethodBankCountry({ iban = '', fallbackCountry = '' } = {}) {
  const normalizedIban = normalizeCollectionMethodIban(iban);
  if (/^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$/.test(normalizedIban)) {
    return normalizedIban.slice(0, 2);
  }

  return normalizeCollectionMethodCountry(fallbackCountry);
}

function getCollectionMethodBankCurrency(countryCode = '') {
  return COLLECTION_METHOD_BANK_COUNTRY_TO_CURRENCY[normalizeCollectionMethodCountry(countryCode)] || null;
}

function splitCollectionMethodFullName(fullName = '') {
  const parts = String(fullName || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    return { firstName: '', lastName: '' };
  }

  if (parts.length === 1) {
    return { firstName: parts[0], lastName: parts[0] };
  }

  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(' '),
  };
}

function buildCollectionMethodStreetLine1(address1 = '', streetNumber = '') {
  return [String(address1 || '').trim(), String(streetNumber || '').trim()].filter(Boolean).join(' ').trim();
}

function parseCollectionMethodDateOfBirth(value = '') {
  const match = String(value || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return null;
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);

  const parsedDate = new Date(Date.UTC(year, month - 1, day));
  if (
    Number.isNaN(parsedDate.getTime()) ||
    parsedDate.getUTCFullYear() !== year ||
    parsedDate.getUTCMonth() !== month - 1 ||
    parsedDate.getUTCDate() !== day
  ) {
    return null;
  }

  return {
    year,
    month,
    day,
    canonical: `${match[1]}-${match[2]}-${match[3]}`,
  };
}

function getCollectionMethodRequestIp(req) {
  return (req.headers['x-forwarded-for'] || req.ip || '')
    .toString()
    .split(',')[0]
    .trim()
    .substring(0, 45) || undefined;
}

async function acquireCollectionMethodCreationLock(connection, userId) {
  const lockName = `collection-method:${userId}`;
  const [[row]] = await connection.query('SELECT GET_LOCK(?, 0) AS acquired', [lockName]);

  return {
    lockName,
    acquired: Number(row?.acquired) === 1,
  };
}

async function releaseCollectionMethodCreationLock(connection, lockName) {
  if (!lockName) {
    return;
  }

  try {
    await connection.query('SELECT RELEASE_LOCK(?) AS released', [lockName]);
  } catch (lockError) {
    console.error('Error al liberar el lock de collection method:', lockError);
  }
}

async function rollbackCollectionMethodStripeArtifacts({
  accountId = null,
  externalAccountId = null,
  createdAccount = false,
} = {}) {
  if (externalAccountId && accountId) {
    try {
      await stripe.accounts.removeExternalAccount(accountId, externalAccountId);
    } catch (removeExternalAccountError) {
      console.error('Error al revertir la cuenta bancaria externa de Stripe:', removeExternalAccountError);
    }
  }

  if (createdAccount && accountId) {
    try {
      await stripe.accounts.del(accountId);
    } catch (deleteAccountError) {
      console.error('Error al revertir la cuenta Connect de Stripe:', deleteAccountError);
    }
  }
}

app.get('/api/user/:id/collection-method', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  try {
    const [rows] = await promisePool.query(
      `SELECT cm.id,
              cm.type,
              cm.external_account_id,
              cm.last4,
              cm.brand,
              cm.currency,
              cm.created_at,
              cm.updated_at,
              cm.full_name,
              a.country,
              a.state,
              a.city,
              a.address_1,
              a.address_2,
              a.street_number,
              a.postal_code
         FROM collection_method cm
         LEFT JOIN address a ON a.id = cm.address_id
        WHERE cm.user_id = ?
        ORDER BY cm.id DESC
        LIMIT 1`,
      [requestedUserId]
    );

    const collectionMethod = rows[0];
    if (!collectionMethod) {
      return res.status(404).json({ error: 'collection_method_not_found' });
    }

    return res.status(200).json({
      ...collectionMethod,
      method_kind: collectionMethod.type === 'us_bank_account' || collectionMethod.brand === 'us_bank_account' || collectionMethod.country === 'US'
        ? 'us_bank_account'
        : 'iban',
    });
  } catch (error) {
    console.error('Error al obtener el método de cobro:', error);
    return res.status(500).json({ error: 'collection_method_fetch_failed' });
  }
});

// Crear método de cobro y cuenta Stripe Connect
app.post('/api/user/:id/collection-method', authenticateToken, async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  const fullName = typeof req.body?.full_name === 'string' ? req.body.full_name.trim() : '';
  const rawDateOfBirth = typeof req.body?.date_of_birth === 'string' ? req.body.date_of_birth.trim() : '';
  const nif = typeof req.body?.nif === 'string' ? req.body.nif.trim().toUpperCase() : '';
  const rawIban = typeof req.body?.iban === 'string' ? req.body.iban : '';
  const rawRoutingNumber = typeof req.body?.routing_number === 'string' ? req.body.routing_number : '';
  const rawAccountNumber = typeof req.body?.account_number === 'string' ? req.body.account_number : '';
  const addressType = typeof req.body?.address_type === 'string' ? req.body.address_type.trim() : '';
  const streetNumber = req.body?.street_number == null ? '' : String(req.body.street_number).trim();
  const address1 = typeof req.body?.address_1 === 'string' ? req.body.address_1.trim() : '';
  const address2 = typeof req.body?.address_2 === 'string' ? req.body.address_2.trim() : '';
  const postalCode = req.body?.postal_code == null ? '' : String(req.body.postal_code).trim();
  const city = typeof req.body?.city === 'string' ? req.body.city.trim() : '';
  const state = typeof req.body?.state === 'string' ? req.body.state.trim() : '';
  const country = normalizeCollectionMethodCountry(req.body?.country);
  const phone = typeof req.body?.phone === 'string' ? req.body.phone.trim() : '';
  const fileTokenAnverso = typeof req.body?.fileTokenAnverso === 'string' ? req.body.fileTokenAnverso.trim() : '';
  const fileTokenReverso = typeof req.body?.fileTokenReverso === 'string' ? req.body.fileTokenReverso.trim() : '';

  const normalizedIban = normalizeCollectionMethodIban(rawIban);
  const normalizedRoutingNumber = normalizeCollectionMethodRoutingNumber(rawRoutingNumber);
  const normalizedAccountNumber = normalizeCollectionMethodUsAccountNumber(rawAccountNumber);
  const usesUsBankAccount = country === 'US';
  const dateOfBirth = parseCollectionMethodDateOfBirth(rawDateOfBirth);
  const bankCountry = usesUsBankAccount
    ? 'US'
    : getCollectionMethodBankCountry({ iban: normalizedIban, fallbackCountry: country });
  const bankCurrency = getCollectionMethodBankCurrency(bankCountry);
  const { firstName, lastName } = splitCollectionMethodFullName(fullName);
  const stripeAddressLine1 = buildCollectionMethodStreetLine1(address1, streetNumber);
  const hasValidBankDetails = usesUsBankAccount
    ? isValidCollectionMethodRoutingNumber(normalizedRoutingNumber) && isValidCollectionMethodUsAccountNumber(normalizedAccountNumber)
    : isValidCollectionMethodIban(normalizedIban);
  const collectionMethodType = usesUsBankAccount ? 'us_bank_account' : 'iban';

  if (
    !fullName ||
    !dateOfBirth ||
    !nif ||
    !hasValidBankDetails ||
    !addressType ||
    !address1 ||
    !postalCode ||
    !city ||
    !state ||
    !country ||
    !isCollectionMethodCountrySupported(country) ||
    !phone ||
    !fileTokenAnverso ||
    !fileTokenReverso ||
    !firstName ||
    !lastName ||
    !stripeAddressLine1
  ) {
    return res.status(400).json({ error: 'Campos requeridos faltantes o inválidos' });
  }

  if (!bankCurrency) {
    return res.status(400).json({
      error: 'No se pudo determinar una moneda válida para la cuenta bancaria.',
      bankCountry,
    });
  }

  const connection = await pool.promise().getConnection();
  let transactionStarted = false;
  let lockName = null;
  let stripeAccountId = null;
  let stripeExternalAccountId = null;
  let createdStripeAccount = false;

  try {
    const lockState = await acquireCollectionMethodCreationLock(connection, requestedUserId);
    lockName = lockState.lockName;

    if (!lockState.acquired) {
      return res.status(409).json({
        error: 'La creación del método de cobro ya está en curso.',
        code: 'COLLECTION_METHOD_IN_PROGRESS',
      });
    }

    await connection.beginTransaction();
    transactionStarted = true;

    const [[user]] = await connection.query(
      'SELECT id, email, stripe_account_id FROM user_account WHERE id = ? FOR UPDATE',
      [requestedUserId]
    );

    if (!user) {
      await connection.rollback();
      transactionStarted = false;
      return res.status(404).json({ message: 'Usuario no encontrado' });
    }

    const [[existingCollectionMethod]] = await connection.query(
      'SELECT id, external_account_id, address_id FROM collection_method WHERE user_id = ? ORDER BY id DESC LIMIT 1 FOR UPDATE',
      [requestedUserId]
    );

    const hasStripeAccount = typeof user.stripe_account_id === 'string' && user.stripe_account_id.startsWith('acct_');

    if (hasStripeAccount && existingCollectionMethod) {
      await connection.commit();
      transactionStarted = false;
      return res.status(200).json({
        message: 'Método de cobro ya configurado',
        stripe_account_id: user.stripe_account_id,
        alreadyExists: true,
      });
    }

    const accountPayload = {
      email: user.email,
      business_type: 'individual',
      individual: {
        first_name: firstName,
        last_name: lastName,
        id_number: nif,
        dob: {
          day: dateOfBirth.day,
          month: dateOfBirth.month,
          year: dateOfBirth.year,
        },
        address: {
          line1: stripeAddressLine1,
          line2: address2 || undefined,
          postal_code: postalCode,
          city,
          state,
          country,
        },
        email: user.email,
        phone,
        verification: {
          document: {
            front: fileTokenAnverso,
            back: fileTokenReverso,
          },
        },
      },
      capabilities: {
        card_payments: { requested: true },
        transfers: { requested: true },
      },
      business_profile: {
        mcc: '7299',
        product_description: 'Servicios profesionales',
      },
      tos_acceptance: {
        date: Math.floor(Date.now() / 1000),
        ip: getCollectionMethodRequestIp(req),
      },
    };

    if (hasStripeAccount) {
      stripeAccountId = user.stripe_account_id;
      try {
        await stripe.accounts.update(stripeAccountId, accountPayload);
      } catch (updateStripeAccountError) {
        if (updateStripeAccountError?.code === 'resource_missing' || updateStripeAccountError?.statusCode === 404) {
          const createdAccount = await stripe.accounts.create({
            type: 'custom',
            country,
            ...accountPayload,
          });
          stripeAccountId = createdAccount.id;
          createdStripeAccount = true;
        } else {
          throw updateStripeAccountError;
        }
      }
    } else {
      const createdAccount = await stripe.accounts.create({
        type: 'custom',
        country,
        ...accountPayload,
      });
      stripeAccountId = createdAccount.id;
      createdStripeAccount = true;
    }

    const externalAccountPayload = {
      object: 'bank_account',
      country: bankCountry,
      currency: bankCurrency,
      account_holder_name: fullName,
      account_number: usesUsBankAccount ? normalizedAccountNumber : normalizedIban,
    };

    if (usesUsBankAccount) {
      externalAccountPayload.routing_number = normalizedRoutingNumber;
    }

    const bank = await stripe.accounts.createExternalAccount(stripeAccountId, {
      external_account: externalAccountPayload,
    });
    stripeExternalAccountId = bank.id;

    let addressId = existingCollectionMethod?.address_id || null;
    const streetNumberValue = streetNumber || null;
    const address2Value = address2 || null;

    if (addressId) {
      await connection.query(
        'UPDATE address SET address_type = ?, street_number = ?, address_1 = ?, address_2 = ?, postal_code = ?, city = ?, state = ?, country = ? WHERE id = ?',
        [addressType, streetNumberValue, address1, address2Value, postalCode, city, state, country, addressId]
      );
    } else {
      const [addressResult] = await connection.query(
        'INSERT INTO address (address_type, street_number, address_1, address_2, postal_code, city, state, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [addressType, streetNumberValue, address1, address2Value, postalCode, city, state, country]
      );
      addressId = addressResult.insertId;
    }

    const last4 = (usesUsBankAccount ? normalizedAccountNumber : normalizedIban).slice(-4);
    const collectionMethodBrand = usesUsBankAccount ? 'us_bank_account' : null;
    if (existingCollectionMethod) {
      await connection.query(
        'UPDATE collection_method SET type = ?, external_account_id = ?, last4 = ?, brand = ?, currency = ?, address_id = ?, full_name = ? WHERE id = ?',
        [collectionMethodType, stripeExternalAccountId, last4, collectionMethodBrand, bankCurrency, addressId, fullName, existingCollectionMethod.id]
      );
    } else {
      await connection.query(
        'INSERT INTO collection_method (user_id, type, external_account_id, last4, brand, currency, address_id, full_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [requestedUserId, collectionMethodType, stripeExternalAccountId, last4, collectionMethodBrand, bankCurrency, addressId, fullName]
      );
    }

    await connection.query(
      'UPDATE user_account SET date_of_birth = ?, nif = ?, phone = ?, stripe_account_id = ?, is_professional = 1, professional_started_datetime = IF(is_professional = 1, professional_started_datetime, NOW()) WHERE id = ?',
      [dateOfBirth.canonical, nif, phone, stripeAccountId, requestedUserId]
    );

    await connection.commit();
    transactionStarted = false;

    return res.status(201).json({
      message: 'Método de cobro creado',
      stripe_account_id: stripeAccountId,
      alreadyExists: false,
      bank_country: bankCountry,
      bank_currency: bankCurrency,
    });
  } catch (stripeErr) {
    if (transactionStarted) {
      try {
        await connection.rollback();
      } catch { }
    }

    await rollbackCollectionMethodStripeArtifacts({
      accountId: stripeAccountId,
      externalAccountId: stripeExternalAccountId,
      createdAccount: createdStripeAccount,
    });

    console.error('Error al crear la cuenta de Stripe:', stripeErr);
    const stripeMessage = typeof stripeErr?.message === 'string' ? stripeErr.message : null;
    const statusCode = stripeErr?.type === 'StripeInvalidRequestError' ? 400 : 500;

    return res.status(statusCode).json({
      error: 'Error al crear la cuenta de cobro.',
      stripeCode: stripeErr?.code || null,
      stripeMessage,
    });
  } finally {
    await releaseCollectionMethodCreationLock(connection, lockName);
    connection.release();
  }
});

app.post('/api/user/:id/id-document/detect-number', multerMid.single('file'), async (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res);
  if (!requestedUserId) return;

  if (!req.file) {
    return res.status(400).json({ error: 'file_required' });
  }

  try {
    const detectionResult = await visionIdDetector.detectIdNumberFromImageBuffer(req.file.buffer);
    return res.json({
      detectedIdNumber: detectionResult.detectedIdNumber || null,
    });
  } catch (error) {
    console.error('Cloud Vision ID detection error:', error?.response?.data || error);
    return res.status(500).json({ error: 'id_detection_failed' });
  }
});

// Marca booking como failed si no se ha pagado el deposit
app.post('/api/bookings/:id/cancel-if-unpaid', authenticateToken, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  const conn = await pool.promise().getConnection();
  try {
    await conn.beginTransaction();
    const [[b]] = await conn.query(
      'SELECT id, client_user_id, service_status, settlement_status FROM booking WHERE id = ? FOR UPDATE',
      [id]
    );
    if (!b) { await conn.rollback(); return res.status(404).json({ error: 'Reserva no encontrada' }); }
    const isOwner = req.user && Number(req.user.id) === Number(b.client_user_id);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isOwner && !isStaff) { await conn.rollback(); return res.status(403).json({ error: 'No autorizado' }); }
    const [[pdep]] = await conn.query(
      "SELECT status FROM payments WHERE booking_id = ? AND type='deposit' LIMIT 1", [id]
    );
    const succeeded = pdep && pdep.status === 'succeeded';
    if (!succeeded && normalizeServiceStatus(b.service_status, 'pending_deposit') === 'pending_deposit') {
      await transitionBookingStateRecord(conn, b, {
        nextSettlementStatus: 'payment_failed',
        changedByUserId: req.user?.id || null,
        reasonCode: 'deposit_payment_failed',
      });
    }
    await conn.commit();
    res.json({ ok: true });
  } catch (e) {
    try { await conn.rollback(); } catch { }
    res.status(500).json({ error: 'No se pudo cancelar la reserva impagada' });
  } finally { conn.release(); }
});

app.get('/api/payment-methods/default', authenticateToken, async (req, res) => {
  const requestedUserId = normalizeNullableInteger(req.user?.id);
  if (!requestedUserId) {
    return res.status(401).json({ error: 'No autorizado.' });
  }

  const connection = await pool.promise().getConnection();
  try {
    const defaultPaymentMethod = await getDefaultSavedCustomerPaymentMethod(connection, requestedUserId);
    return res.status(200).json({
      payment_method: defaultPaymentMethod ? mapStoredPaymentMethodForApi(defaultPaymentMethod) : null,
    });
  } catch (error) {
    console.error('Error fetching default payment method:', error);
    return res.status(500).json({ error: 'No se pudo obtener el método de pago por defecto.' });
  } finally {
    connection.release();
  }
});

// Cobra la comisión 10% (mín 1€)
app.post('/api/bookings/:id/deposit', authenticateToken, async (req, res) => {
  await ensureExchangeRatesFresh();
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Id inválido' });
  const { payment_method_id, save_payment_method } = req.body;
  const requestedSavePaymentMethod = normalizeBooleanInput(save_payment_method, false);

  console.log('Iniciando proceso de depósito:', {
    bookingId: id,
    userId: req.user?.id,
    userRole: req.user?.role,
    timestamp: new Date().toISOString(),
    paymentMethodId: payment_method_id || null,
    savePaymentMethod: requestedSavePaymentMethod,
  });

  let booking;
  let payment;
  let commissionCents;
  let chargeCurrency = 'EUR';

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();

    const [rows] = await connection.query(
      `
      SELECT
             b.id,
             b.client_user_id AS user_id,
             b.service_status,
             b.settlement_status,
             b.requested_duration_minutes AS service_duration,
             b.requested_start_datetime AS booking_start_datetime,
             b.requested_end_datetime AS booking_end_datetime,
             b.deposit_amount_cents_snapshot,
             b.deposit_currency_snapshot,
             b.price_type_snapshot,
             b.service_currency_snapshot,
             b.unit_price_amount_cents_snapshot,
             b.estimated_commission_amount_cents,
             b.estimated_total_amount_cents,
             cp.proposed_commission_amount_cents,
             cp.proposed_total_amount_cents,
             s.id AS service_id,
             p.price AS unit_price,
             COALESCE(p.price_type, b.price_type_snapshot) AS price_type,
             COALESCE(p.currency, b.service_currency_snapshot) AS service_currency,
             u.email AS customer_email, u.stripe_customer_id, u.currency AS customer_currency
      FROM booking b
      JOIN service s ON b.service_id = s.id
      JOIN price   p ON s.price_id = p.id
      JOIN user_account u ON b.client_user_id = u.id
      LEFT JOIN booking_closure_proposal cp ON cp.id = (
        SELECT cp2.id
        FROM booking_closure_proposal cp2
        WHERE cp2.booking_id = b.id
        ORDER BY cp2.id DESC
        LIMIT 1
      )
      WHERE b.id = ? FOR UPDATE
      `,
      [id]
    );
    booking = rows[0];
    if (!booking) throw new Error('Reserva no encontrada');
    if (normalizeServiceStatus(booking.service_status, 'pending_deposit') === 'canceled') {
      throw new Error('Reserva cancelada');
    }

    const bookingCurrency = normalizeCurrencyCode(
      booking.service_currency_snapshot,
      normalizeCurrencyCode(booking.service_currency, 'EUR')
    );
    booking.final_price = booking.proposed_total_amount_cents != null
      ? fromMinorUnits(booking.proposed_total_amount_cents, bookingCurrency)
      : (
        booking.estimated_total_amount_cents != null
          ? fromMinorUnits(booking.estimated_total_amount_cents, bookingCurrency)
          : null
      );
    booking.commission = booking.proposed_commission_amount_cents != null
      ? fromMinorUnits(booking.proposed_commission_amount_cents, bookingCurrency)
      : (
        booking.estimated_commission_amount_cents != null
          ? fromMinorUnits(booking.estimated_commission_amount_cents, bookingCurrency)
          : null
      );
    booking.booking_status = deriveLegacyBookingStatus({
      serviceStatus: booking.service_status,
      settlementStatus: booking.settlement_status,
    });

    console.log('Información de reserva obtenida:', {
      bookingId: id,
      userId: booking.user_id,
      finalPrice: booking.final_price,
      commission: booking.commission,
      status: booking.booking_status,
      customerEmail: booking.customer_email,
      hasStripeCustomer: !!booking.stripe_customer_id
    });

    const isOwner = req.user && Number(req.user.id) === Number(booking.user_id);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'No autorizado' });
    }

    // Helpers de redondeo como en el front
    const round1 = (x) => Number((Math.round(Number(x) * 10) / 10).toFixed(1));
const round2 = (n) => {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.round((x + Number.EPSILON) * 100) / 100;
};

    // Duración efectiva
    const storedDurationMin = Number.isFinite(Number(booking.service_duration)) ? Number(booking.service_duration) : null;
    let derivedMinutes = null;
    try {
      if (!storedDurationMin && booking.booking_start_datetime && booking.booking_end_datetime) {
        const t0 = new Date(booking.booking_start_datetime);
        const t1 = new Date(booking.booking_end_datetime);
        if (!Number.isNaN(t0.getTime()) && !Number.isNaN(t1.getTime())) {
          derivedMinutes = Math.round(Math.max(0, t1.getTime() - t0.getTime()) / 60000);
        }
      }
    } catch { }
    const effectiveMinutes = storedDurationMin ?? derivedMinutes ?? 0;

    // Garantiza Customer y persiste si no existe
    const customerId = await ensureStripeCustomerId(connection, {
      userId: booking.user_id,
      email: booking.customer_email,
    });

    const existingDepositPayment = await getPaymentRow(connection, id, 'deposit');
    chargeCurrency = normalizeCurrencyCode(
      existingDepositPayment?.currency,
      normalizeCurrencyCode(
        booking.deposit_currency_snapshot,
        normalizeCurrencyCode(booking.customer_currency, normalizeCurrencyCode(booking.service_currency, 'EUR'))
      )
    );
    const fallbackConvertedUnitPrice = booking.unit_price_amount_cents_snapshot == null
      ? convertAmount(booking.unit_price || 0, booking.service_currency, chargeCurrency)
      : convertAmount(
        fromMinorUnits(booking.unit_price_amount_cents_snapshot, bookingCurrency),
        bookingCurrency,
        chargeCurrency
      );
    const fallbackPricing = computeBookingPricingSnapshot({
      priceType: booking.price_type,
      unitPrice: fallbackConvertedUnitPrice,
      durationMinutes: effectiveMinutes,
      currency: chargeCurrency,
    });

    commissionCents = Number(
      existingDepositPayment?.amount_cents
      ?? booking.deposit_amount_cents_snapshot
      ?? booking.estimated_commission_amount_cents
      ?? toMinorUnits(fallbackPricing.commission || 0, chargeCurrency)
    );
    const finalCentsSnapshot = Number(
      booking.proposed_total_amount_cents
      ?? booking.estimated_total_amount_cents
      ?? (fallbackPricing.final == null ? 0 : toMinorUnits(fallbackPricing.final, chargeCurrency))
    );

    // Congelar importes creando fila de pago (para poder atar idempotencia a payment.id)
    await upsertPaymentRow(connection, {
      bookingId: id,
      type: 'deposit',
      amountCents: commissionCents,
      commissionSnapshotCents: commissionCents,
      finalPriceSnapshotCents: finalCentsSnapshot,
      status: 'creating',
      currency: chargeCurrency,
    });

    payment = await getPaymentRow(connection, id, 'deposit');

    await connection.commit();

    // A partir de aquí, fuera de transacción
    const transferGroup = `booking-${id}`;
    // Clave de idempotencia sensible al importe para evitar conflictos si varía la comisión/importe entre intentos
    const idemParts = ['payment', String(payment.id), 'amt', String(commissionCents)];
    if (payment_method_id) idemParts.push('pm', String(payment_method_id));
    const idemKey = stableKey(idemParts);

    let intent;
    let pm = null;

    try {
      if (payment.payment_intent_id) {
        // Recuperar intent existente
        intent = await stripe.paymentIntents.retrieve(payment.payment_intent_id, {
          expand: ['payment_method', 'latest_charge.payment_method_details'],
        });

        // Si falta método de pago y el cliente nos envía uno ahora, adjuntarlo y confirmar on-session
        if (intent.status === 'requires_payment_method' && payment_method_id) {
          pm = await stripe.paymentMethods.retrieve(payment_method_id);
          if (pm.customer && pm.customer !== customerId) {
            return res.status(409).json({ error: 'payment_method_id pertenece a otro customer.' });
          }
          if (!pm.customer) {
            try {
              pm = await stripe.paymentMethods.attach(payment_method_id, { customer: customerId });
            } catch (eAttach) {
              console.error('No se pudo adjuntar el PM al customer (depósito):', eAttach);
              return res.status(400).json({ error: 'No se pudo adjuntar el método de pago al cliente.' });
            }
          }
          await stripe.paymentIntents.update(intent.id, {
            payment_method: payment_method_id,
            customer: customerId,
            setup_future_usage: 'off_session',
            metadata: {
              booking_id: String(id),
              type: 'deposit',
              save_payment_method: requestedSavePaymentMethod ? '1' : '0',
            },
          });
          intent = await stripe.paymentIntents.confirm(intent.id);
        }
      } else {
        // Crear intent nuevo: si traen PM, confirmar on-session; si no, devolver client_secret para confirmar en el cliente
        if (payment_method_id) {
          intent = await stripe.paymentIntents.create(
            {
              amount: commissionCents,
              currency: toStripeCurrencyCode(chargeCurrency),
              customer: customerId,
              payment_method: payment_method_id,
              confirm: true,
              receipt_email: booking.customer_email || undefined,
              automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
              transfer_group: transferGroup,
              setup_future_usage: 'off_session',
              metadata: {
                booking_id: String(id),
                type: 'deposit',
                save_payment_method: requestedSavePaymentMethod ? '1' : '0',
              },
            },
            { idempotencyKey: idemKey }
          );
        } else {
          intent = await stripe.paymentIntents.create(
            {
              amount: commissionCents,
              currency: toStripeCurrencyCode(chargeCurrency),
              customer: customerId,
              setup_future_usage: 'off_session',
              receipt_email: booking.customer_email || undefined,
              automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
              transfer_group: transferGroup,
              metadata: {
                booking_id: String(id),
                type: 'deposit',
                save_payment_method: requestedSavePaymentMethod ? '1' : '0',
              },
            },
            { idempotencyKey: idemKey }
          );
        }
      }
      // Asegura expansión para capturar last4 y errores
      if (!intent.payment_method || !intent.latest_charge) {
        intent = await stripe.paymentIntents.retrieve(intent.id, {
          expand: ['payment_method', 'latest_charge.payment_method_details'],
        });
      }

    } catch (stripeError) {
      console.error('Error de Stripe al crear/recuperar PaymentIntent:', {
        bookingId: id,
        userId: req.user?.id,
        stripeError: {
          type: stripeError.type,
          code: stripeError.code,
          message: stripeError.message,
          declineCode: stripeError.decline_code,
          param: stripeError.param,
          requestId: stripeError.requestId
        },
        paymentData: {
          amountCents: commissionCents,
          customerId,
          transferGroup,
          idemKey
        }
      });

      // Si Stripe devolvió un PaymentIntent parcial, persistirlo y guiar al frontend
      const pi = stripeError?.payment_intent || stripeError?.raw?.payment_intent || null;
      if (pi && pi.id) {
        const paymentPersistence = await resolvePaymentIntentPersistence(pi, {
          paymentMethodFallback: pm || null,
        });
        const persistedPi = paymentPersistence.intent || pi;

        const connErr = await pool.promise().getConnection();
        try {
          await connErr.beginTransaction();
          await upsertPayment(connErr, {
            bookingId: id,
            type: 'deposit',
            paymentIntentId: persistedPi.id,
            amountCents: commissionCents,
            commissionSnapshotCents: commissionCents,
            finalPriceSnapshotCents: finalCentsSnapshot,
            status: mapStatus(persistedPi.status),
            currency: chargeCurrency,
            transferGroup,
            paymentMethodId: paymentPersistence.paymentMethodId || payment_method_id || null,
            paymentMethodLast4: paymentPersistence.paymentMethodLast4 || null,
            lastErrorCode: paymentPersistence.lastErrorCode,
            lastErrorMessage: paymentPersistence.lastErrorMessage,
          });
          await connErr.commit();
        } catch (e2) {
          try { await connErr.rollback(); } catch { }
          console.error('No se pudo persistir el PaymentIntent (depósito) tras error:', e2);
        } finally {
          connErr.release();
        }

        if (pi.status === 'requires_action') {
          return res.status(202).json({ requiresAction: true, clientSecret: pi.client_secret, paymentIntentId: pi.id });
        }
        if (pi.status === 'requires_payment_method') {
          return res.status(202).json({ requiresPaymentMethod: true, clientSecret: pi.client_secret, paymentIntentId: pi.id });
        }
      }

      // Si el conflicto es de idempotencia, devolver 409 específico
      if (stripeError?.type === 'idempotency_error') {
        return res.status(409).json({ error: 'Conflicto de idempotencia en depósito.' });
      }

      return res.status(400).json({ error: 'No se pudo crear el depósito.' });
    }

    const paymentPersistence = await resolvePaymentIntentPersistence(intent, {
      paymentMethodFallback: pm || null,
    });
    intent = paymentPersistence.intent || intent;

    const conn2 = await pool.promise().getConnection();
    try {
      await conn2.beginTransaction();
      await upsertPayment(conn2, {
        bookingId: id,
        type: 'deposit',
        paymentIntentId: intent.id,
        amountCents: commissionCents,
        commissionSnapshotCents: commissionCents,
        finalPriceSnapshotCents: finalCentsSnapshot,
        status: mapStatus(intent.status),
        currency: chargeCurrency,
        transferGroup,
        paymentMethodId: paymentPersistence.paymentMethodId || payment_method_id || null,
        paymentMethodLast4: paymentPersistence.paymentMethodLast4,
        lastErrorCode: paymentPersistence.lastErrorCode,
        lastErrorMessage: paymentPersistence.lastErrorMessage,
      });
      if (intent.status === 'succeeded') {
        await syncBookingSelectedPaymentMethodFromIntent(conn2, {
          bookingId: id,
          userId: booking.user_id,
          intent,
          paymentMethodFallback: pm || null,
          saveForFuture: requestedSavePaymentMethod,
        });
        await transitionBookingStateRecord(conn2, booking, {
          nextServiceStatus: 'requested',
          nextSettlementStatus: booking.settlement_status === 'payment_failed' ? 'none' : booking.settlement_status,
          changedByUserId: req.user?.id || null,
          reasonCode: 'deposit_succeeded',
          extraPatch: {
            deposit_confirmed_at: new Date(),
            deposit_amount_cents_snapshot: commissionCents,
            deposit_currency_snapshot: chargeCurrency,
          },
        });
      } else if (intent.status === 'canceled') {
        await transitionBookingStateRecord(conn2, booking, {
          nextSettlementStatus: 'payment_failed',
          changedByUserId: req.user?.id || null,
          reasonCode: 'deposit_canceled',
        });
      }
      await conn2.commit();
    } catch (e) {
      try { await conn2.rollback(); } catch { }
      console.error('Error al actualizar el estado del pago en BD:', {
        bookingId: id,
        userId: req.user?.id,
        error: e.message,
        stack: e.stack
      });
      return res.status(400).json({
        error: 'No se pudo actualizar el estado del pago.',
        details: e.message
      });
    } finally {
      conn2.release();
    }

    console.log('PaymentIntent creado exitosamente:', {
      bookingId: id,
      paymentIntentId: intent.id,
      status: intent.status,
      amountCents: commissionCents,
      transferGroup
    });

    if (intent.status === 'requires_payment_method') {
      return res.status(202).json({
        requiresPaymentMethod: true,
        clientSecret: intent.client_secret,
        paymentIntentId: intent.id
      });
    }
    if (intent.status === 'requires_action') {
      return res.status(202).json({ requiresAction: true, clientSecret: intent.client_secret, paymentIntentId: intent.id });
    }
    if (intent.status === 'processing') {
      return res.status(202).json({ processing: true, paymentIntentId: intent.id });
    }
    if (intent.status === 'succeeded') {
      return res.status(200).json({ message: 'Depósito pagado', paymentIntentId: intent.id });
    }
    return res.status(200).json({ paymentIntentId: intent.id, status: intent.status, clientSecret: intent.client_secret });
  } catch (err) {
    try { await connection.rollback(); } catch { }

    // Log detallado del error para debugging
    console.error('Error en depósito de reserva:', {
      bookingId: id,
      userId: req.user?.id,
      error: err.message,
      stack: err.stack,
      stripeError: err.type || err.code || null,
      stripeMessage: err.decline_code || err.param || null
    });

    // Si es un error de Stripe, devolver más detalles
    if (err.type && err.type.startsWith('Stripe')) {
      return res.status(400).json({
        error: 'Error de Stripe en el depósito',
        details: {
          type: err.type,
          code: err.code,
          message: err.message,
          declineCode: err.decline_code,
          param: err.param
        }
      });
    }

    return res.status(400).json({
      error: 'No se pudo crear el depósito.',
      details: err.message
    });
  } finally {
    connection.release(); // release solo en finally (evita doble release)
  }
});

// Cobro final en plataforma + liquidación posterior al profesional
app.post('/api/bookings/:id/final-payment-transfer', authenticateToken, async (req, res) => {
  await ensureExchangeRatesFresh();
  const id = parseInt(req.params.id, 10);
  const { payment_method_id, save_payment_method } = req.body;
  const requestedSavePaymentMethod = normalizeBooleanInput(save_payment_method, false);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Id inválido' });

  console.log('Iniciando proceso de pago final:', {
    bookingId: id,
    userId: req.user?.id,
    userRole: req.user?.role,
    timestamp: new Date().toISOString(),
    paymentMethodId: payment_method_id || null,
    savePaymentMethod: requestedSavePaymentMethod,
  });

  let booking;
  let payment;
  let amountToCharge;
  let commissionCents; // almacenada
  let finalCents;      // almacenado
  let commissionCalcCents; // recalculada
  let finalCalcCents;      // recalculado
  let commissionChosenCents; // usada para cobrar
  let finalChosenCents;      // usado para cobrar
  let chargeCurrency = 'EUR';
  let depositPayment = null;

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();

    // Bloquear la reserva y obtener datos necesarios
    const [rows] = await connection.query(
      `
      SELECT
             b.id,
             b.client_user_id AS user_id,
             b.service_status,
             b.settlement_status,
             b.requested_duration_minutes AS service_duration,
             b.requested_start_datetime AS booking_start_datetime,
             b.requested_end_datetime AS booking_end_datetime,
             b.price_type_snapshot,
             b.service_currency_snapshot,
             b.unit_price_amount_cents_snapshot,
             b.estimated_base_amount_cents,
             b.estimated_commission_amount_cents,
             b.estimated_total_amount_cents,
             b.deposit_amount_cents_snapshot,
             b.client_approval_deadline_at,
             cp.id AS closure_proposal_id,
             cp.status AS closure_status,
             cp.proposed_base_amount_cents,
             cp.proposed_commission_amount_cents,
             cp.proposed_total_amount_cents,
             cp.proposed_final_duration_minutes,
             cp.amount_due_from_client_cents,
             cp.amount_to_refund_cents,
             cp.zero_charge_mode,
             cust.email AS customer_email, cust.stripe_customer_id AS customer_id, cust.currency AS customer_currency,
             provider.stripe_account_id,
             p.price AS unit_price,
             COALESCE(p.price_type, b.price_type_snapshot) AS price_type,
             COALESCE(p.currency, b.service_currency_snapshot) AS service_currency
      FROM booking b
      JOIN service s ON b.service_id = s.id
      JOIN price p ON s.price_id = p.id
      JOIN user_account provider ON s.user_id = provider.id
      JOIN user_account cust ON b.client_user_id = cust.id
      LEFT JOIN booking_closure_proposal cp ON cp.id = (
        SELECT cp2.id
        FROM booking_closure_proposal cp2
        WHERE cp2.booking_id = b.id
        ORDER BY cp2.id DESC
        LIMIT 1
      )
      WHERE b.id = ? FOR UPDATE
      `,
      [id]
    );
    booking = rows[0];
    if (!booking) throw new Error('Reserva no encontrada');
    if (normalizeSettlementStatus(booking.settlement_status, 'none') === 'paid') {
      throw new Error('Reserva ya pagada');
    }

    const bookingCurrency = normalizeCurrencyCode(
      booking.service_currency_snapshot,
      normalizeCurrencyCode(booking.service_currency, 'EUR')
    );
    booking.final_price = booking.proposed_total_amount_cents != null
      ? fromMinorUnits(booking.proposed_total_amount_cents, bookingCurrency)
      : (
        booking.estimated_total_amount_cents != null
          ? fromMinorUnits(booking.estimated_total_amount_cents, bookingCurrency)
          : null
      );
    booking.commission = booking.proposed_commission_amount_cents != null
      ? fromMinorUnits(booking.proposed_commission_amount_cents, bookingCurrency)
      : (
        booking.estimated_commission_amount_cents != null
          ? fromMinorUnits(booking.estimated_commission_amount_cents, bookingCurrency)
          : null
      );

    const isOwner = req.user && Number(req.user.id) === Number(booking.user_id);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'No autorizado' });
    }
    console.log('Información de reserva obtenida (pago final):', {
      bookingId: id,
      userId: booking.user_id,
      finalPrice: booking.final_price,
      commission: booking.commission,
      isPaid: !!booking.is_paid,
      customerEmail: booking.customer_email,
      hasStripeCustomer: !!booking.customer_id,
      providerAccountId: booking.stripe_account_id,
    });

    // Garantiza Customer
    const customerId = booking.customer_id || (await ensureStripeCustomerId(connection, {
      userId: booking.user_id,
      email: booking.customer_email,
    }));
    const usablePaymentMethodId = payment_method_id || await getBookingSelectedStripePaymentMethodId(connection, id, { forUpdate: true });

    // Verificar depósito succeeded
    const [dep] = await connection.query(
      `SELECT id, amount_cents, currency
       FROM payments
       WHERE booking_id = ? AND type = 'deposit' AND status = 'succeeded'
       LIMIT 1 FOR UPDATE`,
      [id]
    );
    if (dep.length === 0) {
      await connection.rollback();
      return res.status(412).json({ error: 'Depósito no confirmado (requerido).' });
    }
    depositPayment = dep[0];
    chargeCurrency = normalizeCurrencyCode(
      depositPayment?.currency,
      normalizeCurrencyCode(booking.customer_currency, normalizeCurrencyCode(booking.service_currency, 'EUR'))
    );
    const normalizedServiceStatus = normalizeServiceStatus(booking.service_status, 'pending_deposit');
    let normalizedSettlementStatus = normalizeSettlementStatus(booking.settlement_status, 'none');
    const hasClosureProposal = booking.closure_proposal_id !== null && booking.closure_proposal_id !== undefined;
    const isLegacyFinishedFlow = normalizedServiceStatus === 'finished' && !hasClosureProposal;

    if (normalizedSettlementStatus === 'pending_client_approval') {
      if (!hasClosureProposal || booking.closure_status !== 'active') {
        await connection.rollback();
        return res.status(409).json({ error: 'La propuesta de cierre ya no está disponible.' });
      }

      await updateClosureProposalStatus(
        connection,
        booking.closure_proposal_id,
        'accepted',
        new Date()
      );
      await transitionBookingStateRecord(connection, booking, {
        nextServiceStatus: 'finished',
        nextSettlementStatus: 'awaiting_payment',
        changedByUserId: req.user?.id || null,
        reasonCode: 'closure_proposal_accepted',
        extraPatch: {
          client_approval_deadline_at: null,
        },
      });

      booking = {
        ...booking,
        service_status: 'finished',
        settlement_status: 'awaiting_payment',
        client_approval_deadline_at: null,
        closure_status: 'accepted',
      };
      normalizedSettlementStatus = 'awaiting_payment';
    } else if (normalizedSettlementStatus !== 'awaiting_payment' && !isLegacyFinishedFlow) {
      await connection.rollback();
      return res.status(409).json({ error: 'La reserva no está lista para el pago final.' });
    }

    // Comprobar si ya hay cobro final existente en curso o realizado y devolver la info útil en vez de 409
    const [existingFinal] = await connection.query(
      `SELECT id, status, payment_intent_id, currency
      FROM payments
      WHERE booking_id = ? AND type = 'final'
        AND status IN ('requires_payment_method','requires_action','processing','succeeded')
      LIMIT 1 FOR UPDATE`,
      [id]
    );
    if (existingFinal.length > 0) {
      const row = existingFinal[0];
      // Ya no necesitamos seguir con la transacción de nuevo cobro
      await connection.commit();

      try {
        let intent = null;
        if (row.payment_intent_id) {
          intent = await stripe.paymentIntents.retrieve(row.payment_intent_id, {
            expand: ['payment_method', 'latest_charge.payment_method_details'],
          });
        }

        // Si no hay intent recuperable, intenta crearlo de nuevo usando los snapshots ya guardados
        if (!intent) {
          // Obtener snapshots ya guardados para este pago final
          const [paySnapRows] = await connection.query(
            `SELECT id, amount_cents, commission_snapshot_cents, final_price_snapshot_cents, currency, transfer_group
             FROM payments
             WHERE id = ? AND booking_id = ? AND type = 'final'
             LIMIT 1`,
            [row.id, id]
          );
          const paySnap = paySnapRows && paySnapRows[0];
          const amountEnsure = paySnap?.amount_cents || 0;
          const transferGroupEnsure = paySnap?.transfer_group || `booking-${id}`;
          const ensureCurrency = normalizeCurrencyCode(paySnap?.currency, chargeCurrency);

          if (amountEnsure > 0) {
            const pmIdBody = usablePaymentMethodId || null;
            const idemEnsureParts = ['payment', String(row.id), pmIdBody ? String(pmIdBody) : 'ensure'];
            const idemEnsure = stableKey(idemEnsureParts);
            let pmEnsure = null;

            if (pmIdBody) {
              // Adjuntar PM si es necesario
              pmEnsure = await stripe.paymentMethods.retrieve(pmIdBody);
              if (pmEnsure.customer && pmEnsure.customer !== customerId) {
                return res.status(409).json({ error: 'payment_method_id pertenece a otro customer.' });
              }
              if (!pmEnsure.customer) {
                try {
                  pmEnsure = await stripe.paymentMethods.attach(pmIdBody, { customer: customerId });
                } catch (eAttach) {
                  console.error('No se pudo adjuntar el PM al customer (ensure):', eAttach);
                  return res.status(400).json({ error: 'No se pudo adjuntar el método de pago al cliente.' });
                }
              }

              intent = await stripe.paymentIntents.create(
                {
                  amount: amountEnsure,
                  currency: toStripeCurrencyCode(ensureCurrency),
                  customer: customerId,
                  payment_method: pmIdBody,
                  confirm: true,
                  off_session: true,
                  receipt_email: booking.customer_email || undefined,
                  automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
                  transfer_group: transferGroupEnsure,
                  metadata: {
                    booking_id: String(id),
                    type: 'final',
                    save_payment_method: requestedSavePaymentMethod ? '1' : '0',
                  },
                },
                { idempotencyKey: idemEnsure }
              );
            } else {
              // Crear intent sin PM para devolver clientSecret y confirmar en el cliente
              intent = await stripe.paymentIntents.create(
                {
                  amount: amountEnsure,
                  currency: toStripeCurrencyCode(ensureCurrency),
                  customer: customerId,
                  receipt_email: booking.customer_email || undefined,
                  automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
                  transfer_group: transferGroupEnsure,
                  metadata: {
                    booking_id: String(id),
                    type: 'final',
                    save_payment_method: requestedSavePaymentMethod ? '1' : '0',
                  },
                },
                { idempotencyKey: idemEnsure }
              );
            }

            // Guardar el intent recién creado
            const paymentPersistence = await resolvePaymentIntentPersistence(intent, {
              paymentMethodFallback: pmEnsure,
            });
            intent = paymentPersistence.intent || intent;
            const connEnsure = await pool.promise().getConnection();
            try {
              await connEnsure.beginTransaction();
              await upsertPayment(connEnsure, {
                bookingId: id,
                type: 'final',
                paymentIntentId: intent.id,
                amountCents: amountEnsure,
                commissionSnapshotCents: paySnap?.commission_snapshot_cents ?? null,
                finalPriceSnapshotCents: paySnap?.final_price_snapshot_cents ?? null,
                status: mapStatus(intent.status),
                currency: ensureCurrency,
                transferGroup: transferGroupEnsure,
                paymentMethodId: paymentPersistence.paymentMethodId || usablePaymentMethodId || null,
                paymentMethodLast4: paymentPersistence.paymentMethodLast4 || null,
                lastErrorCode: paymentPersistence.lastErrorCode,
                lastErrorMessage: paymentPersistence.lastErrorMessage,
              });
              if (paymentPersistence.paymentMethodId) {
                await syncBookingSelectedPaymentMethodFromIntent(connEnsure, {
                  bookingId: id,
                  userId: booking.user_id,
                  intent,
                  paymentMethodFallback: pmEnsure,
                  saveForFuture: requestedSavePaymentMethod,
                });
              }
              await connEnsure.commit();
            } catch (eEns) {
              try { await connEnsure.rollback(); } catch { }
              console.error('Error guardando PaymentIntent re-creado (ensure):', eEns);
            } finally {
              connEnsure.release();
            }
          }
        }
        const status = intent ? intent.status : row.status;

        // Si falta método de pago y el cliente lo envía ahora, adjuntarlo y confirmar
        if (intent && status === 'requires_payment_method' && usablePaymentMethodId) {
          const pmId = usablePaymentMethodId;
          try {
            // Adjuntar PM al customer si viene suelto
            let pm2 = await stripe.paymentMethods.retrieve(pmId);
            if (pm2.customer && pm2.customer !== customerId) {
              return res.status(409).json({ error: 'payment_method_id pertenece a otro customer.' });
            }
            if (!pm2.customer) {
              try {
                pm2 = await stripe.paymentMethods.attach(pmId, { customer: customerId });
              } catch (e1) {
                console.error('No se pudo adjuntar el PM al customer:', e1);
                return res.status(400).json({ error: 'No se pudo adjuntar el método de pago al cliente.' });
              }
            }

            await stripe.paymentIntents.update(intent.id, {
              payment_method: pmId,
              customer: customerId,
              metadata: {
                booking_id: String(id),
                type: 'final',
                save_payment_method: requestedSavePaymentMethod ? '1' : '0',
              },
            });
            intent = await stripe.paymentIntents.confirm(intent.id, { off_session: true });

            const paymentPersistence = await resolvePaymentIntentPersistence(intent, {
              paymentMethodFallback: pm2,
            });
            intent = paymentPersistence.intent || intent;

            const conn3 = await pool.promise().getConnection();
            try {
              await conn3.beginTransaction();
              await upsertPayment(conn3, {
                bookingId: id,
                type: 'final',
                paymentIntentId: intent.id,
                amountCents: intent.amount,
                commissionSnapshotCents: null,
                finalPriceSnapshotCents: null,
                status: mapStatus(intent.status),
                currency: normalizeCurrencyCode(row.currency, chargeCurrency),
                transferGroup: intent.transfer_group || null,
                paymentMethodId: paymentPersistence.paymentMethodId || pmId,
                paymentMethodLast4: paymentPersistence.paymentMethodLast4 || null,
                lastErrorCode: paymentPersistence.lastErrorCode,
                lastErrorMessage: paymentPersistence.lastErrorMessage,
              });
              if (paymentPersistence.paymentMethodId) {
                await syncBookingSelectedPaymentMethodFromIntent(conn3, {
                  bookingId: id,
                  userId: booking.user_id,
                  intent,
                  paymentMethodFallback: pm2,
                  saveForFuture: requestedSavePaymentMethod,
                });
              }
              await conn3.commit();
            } catch (e2) {
              try { await conn3.rollback(); } catch { }
              console.error('No se pudo actualizar payment_method en BD para intent existente:', e2);
            } finally {
              conn3.release();
            }

            if (intent.status === 'succeeded') {
              const settlementResult = await finalizeBookingSettlementAfterSuccessfulPayment(id, {
                changedByUserId: req.user?.id || null,
                reasonCode: 'final_payment_succeeded_manual_retry',
              });
              if (!settlementResult.settled) {
                return res.status(202).json({ manualReviewRequired: true, paymentIntentId: intent.id });
              }
              return res.status(200).json({ message: 'Pago confirmado', paymentIntentId: intent.id });
            }
            if (intent.status === 'requires_action') {
              return res.status(202).json({ requiresAction: true, clientSecret: intent.client_secret, paymentIntentId: intent.id });
            }
            if (intent.status === 'processing') {
              return res.status(202).json({ processing: true, paymentIntentId: intent.id });
            }
          } catch (e1) {
            console.error('Error al adjuntar/confirmar PM en intent existente:', e1);
            // Continúa con el manejo estándar según estado actual
          }
        }

        // Si ya existe un intent y está esperando método de pago, devuelve el clientSecret para confirmarlo en el cliente
        if (intent && status === 'requires_payment_method') {
          return res.status(202).json({
            requiresPaymentMethod: true,
            clientSecret: intent.client_secret,
            paymentIntentId: intent.id,
          });
        }

        if (status === 'succeeded') {
          const settlementResult = await finalizeBookingSettlementAfterSuccessfulPayment(id, {
            changedByUserId: req.user?.id || null,
            reasonCode: 'final_payment_existing_intent_succeeded',
          });
          if (!settlementResult.settled) {
            return res.status(202).json({ manualReviewRequired: true, paymentIntentId: intent?.id || row.payment_intent_id });
          }
          return res.status(200).json({ message: 'Pago confirmado', paymentIntentId: intent?.id || row.payment_intent_id });
        }
        if (status === 'requires_action') {
          try {
            const pi = await stripe.paymentIntents.retrieve(row.payment_intent_id);
            return res.status(202).json({
              requiresAction: true,
              clientSecret: pi.client_secret,
              paymentIntentId: pi.id
            });
          } catch (e) {
            // Fallback: devuelve estado recuperable (el front podrá hacer un GET de rescate si quieres)
            return res.status(202).json({
              requiresAction: true,
              paymentIntentId: row.payment_intent_id
            });
          }
        }
        if (status === 'processing') {
          return res.status(202).json({ processing: true, paymentIntentId: intent?.id || row.payment_intent_id });
        }
        // Fallback: si llega aquí, informar del estado actual
        return res.status(409).json({ error: 'Ya existe un cobro final en curso o realizado.', status, paymentIntentId: row.payment_intent_id });
      } catch (e) {
        console.error('Error recuperando PaymentIntent existente:', e);
        return res.status(409).json({ error: 'Cobro final existente no recuperable', status: row.status, paymentIntentId: row.payment_intent_id });
      }
    }

    const actualSettlement = buildActualBookingSettlementSnapshot({
      booking,
      depositPayment,
    });
    finalChosenCents = actualSettlement.effectiveTotalAmountCents;
    commissionChosenCents = actualSettlement.platformAmountCents;
    finalCents = actualSettlement.totalAmountInBookingCurrency;
    commissionCents = Number(
      booking.proposed_commission_amount_cents
      ?? booking.estimated_commission_amount_cents
      ?? 0
    );
    finalCalcCents = actualSettlement.totalAmountInChargeCurrency;
    commissionCalcCents = actualSettlement.platformAmountCents;
    amountToCharge = actualSettlement.amountDueFromClientCents;

    // Congelar snapshots en la fila del pago final (para idempotencia por payment.id)
    await upsertPaymentRow(connection, {
      bookingId: id,
      type: 'final',
      amountCents: amountToCharge,
      commissionSnapshotCents: commissionChosenCents,
      finalPriceSnapshotCents: finalChosenCents,
      status: 'creating',
      currency: chargeCurrency,
    });
    payment = await getPaymentRow(connection, id, 'final');

    await connection.commit();

    if (amountToCharge <= 0) {
      const settlementResult = await finalizeBookingSettlementAfterSuccessfulPayment(id, {
        changedByUserId: req.user?.id || null,
        reasonCode: 'final_payment_covered_by_deposit',
      });
      if (!settlementResult.settled) {
        return res.status(500).json({ error: 'No se pudo cerrar el pago final.' });
      }

      return res.status(200).json({
        message: 'Pago final ya cubierto por el depósito.',
        amount_cents: 0,
        currency: chargeCurrency,
      });
    }

    // Determinar PM a usar
    const pmToUse = usablePaymentMethodId || null;
    if (!pmToUse) {
      return res.status(400).json({ error: 'No hay método de pago disponible. Proporcione payment_method_id.' });
    }

    // Recuperar y adjuntar si es necesario
    let pm = await stripe.paymentMethods.retrieve(pmToUse);

    if (pm.customer && pm.customer !== customerId) {
      return res.status(409).json({ error: 'payment_method_id pertenece a otro customer.' });
    }

    if (!pm.customer) {
      try {
        pm = await stripe.paymentMethods.attach(pmToUse, { customer: customerId });
      } catch (e) {
        console.error('No se pudo adjuntar el PM al customer:', e);
        return res.status(400).json({ error: 'No se pudo adjuntar el método de pago al cliente.' });
      }
    }

    // Stripe fuera de transacción: crear/confirmar Intent
    const transferGroup = `booking-${id}`;
    // Usar una clave de idempotencia que incluya el método de pago para permitir cambiar de tarjeta sin conflicto
    const idemKeyParts = ['payment', String(payment.id)];
    if (pmToUse) idemKeyParts.push(String(pmToUse));
    const idemKey = stableKey(idemKeyParts);

    let intent;
    try {
      if (payment.payment_intent_id) {
        intent = await stripe.paymentIntents.retrieve(payment.payment_intent_id, {
          expand: ['payment_method', 'latest_charge.payment_method_details'],
        });
        if (intent.status === 'requires_payment_method') {
          await stripe.paymentIntents.update(intent.id, { payment_method: pmToUse, customer: customerId });
          intent = await stripe.paymentIntents.confirm(intent.id, { off_session: true });
        }
      } else {
        intent = await stripe.paymentIntents.create(
          {
            amount: amountToCharge,
            currency: toStripeCurrencyCode(chargeCurrency),
            customer: customerId,
            payment_method: pmToUse,
            confirm: true,
            off_session: true,
            receipt_email: booking.customer_email || undefined,
            automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
            transfer_group: transferGroup,
            metadata: {
              booking_id: String(id),
              type: 'final',
              save_payment_method: requestedSavePaymentMethod ? '1' : '0',
            },
          },
          { idempotencyKey: idemKey }
        );
      }
      // Asegura expansión para capturar last4 y errores
      if (!intent.payment_method || !intent.latest_charge) {
        intent = await stripe.paymentIntents.retrieve(intent.id, { expand: ['payment_method', 'latest_charge.payment_method_details'] });
      }
    } catch (stripeError) {
      console.error('Error de Stripe al crear/recuperar PaymentIntent (pago final):', {
        bookingId: id,
        userId: req.user?.id,
        stripeError: {
          type: stripeError.type,
          code: stripeError.code,
          message: stripeError.message,
          declineCode: stripeError.decline_code,
          param: stripeError.param,
          requestId: stripeError.requestId,
        },
        paymentData: {
          amountCents: amountToCharge,
          customerId,
          transferGroup,
          idemKey,
          destination: booking.stripe_account_id,
        },
      });

      // Si Stripe ya creó un PaymentIntent, persistirlo para reutilizarlo en el siguiente intento
      const pi = stripeError?.payment_intent || stripeError?.raw?.payment_intent || null;
      if (pi && pi.id) {
        const paymentPersistence = await resolvePaymentIntentPersistence(pi, {
          paymentMethodFallback: pm || null,
        });
        const persistedPi = paymentPersistence.intent || pi;

        const connErr = await pool.promise().getConnection();
        try {
          await connErr.beginTransaction();
          await upsertPayment(connErr, {
            bookingId: id,
            type: 'final',
            paymentIntentId: persistedPi.id,
            amountCents: amountToCharge,
            commissionSnapshotCents: commissionChosenCents,
            finalPriceSnapshotCents: finalChosenCents,
            status: mapStatus(persistedPi.status),
            currency: chargeCurrency,
            transferGroup,
            paymentMethodId: paymentPersistence.paymentMethodId || pmToUse,
            paymentMethodLast4: paymentPersistence.paymentMethodLast4 || null,
            lastErrorCode: paymentPersistence.lastErrorCode,
            lastErrorMessage: paymentPersistence.lastErrorMessage,
          });
          if (paymentPersistence.paymentMethodId) {
            await syncBookingSelectedPaymentMethodFromIntent(connErr, {
              bookingId: id,
              userId: booking.user_id,
              intent: persistedPi,
              paymentMethodFallback: pm || null,
              saveForFuture: requestedSavePaymentMethod,
            });
          }
          await connErr.commit();
        } catch (e2) {
          try { await connErr.rollback(); } catch { }
          console.error('No se pudo persistir el PaymentIntent tras error:', e2);
        } finally {
          connErr.release();
        }

        // Responder acorde al estado del intent para guiar al frontend
        if (pi.status === 'requires_action') {
          return res.status(202).json({
            requiresAction: true,
            clientSecret: pi.client_secret,
            paymentIntentId: pi.id
          });
        }
        if (pi.status === 'requires_payment_method') {
          return res.status(202).json({
            requiresPaymentMethod: true,
            clientSecret: pi.client_secret,
            paymentIntentId: pi.id
          });
        }
      }

      // Si el conflicto es puramente de idempotencia, indícalo de forma explícita
      if (stripeError?.type === 'idempotency_error') {
        return res.status(409).json({
          error: 'Conflicto de idempotencia: ya existe una operación con esa clave. Reintenta reutilizando el PaymentIntent guardado si está disponible.'
        });
      }

      return res.status(400).json({ error: 'No se pudo procesar el pago final.' });
    }

    const paymentPersistence = await resolvePaymentIntentPersistence(intent, {
      paymentMethodFallback: pm || null,
    });
    intent = paymentPersistence.intent || intent;

    // Persistir intent/estado + snapshots + transfer_group + PM last4
    const conn2 = await pool.promise().getConnection();
    try {
      await conn2.beginTransaction();
      await upsertPayment(conn2, {
        bookingId: id,
        type: 'final',
        paymentIntentId: intent.id,
        amountCents: amountToCharge,
        commissionSnapshotCents: commissionChosenCents,
        finalPriceSnapshotCents: finalChosenCents,
        status: mapStatus(intent.status),
        currency: chargeCurrency,
        transferGroup,
        paymentMethodId: paymentPersistence.paymentMethodId || pmToUse,
        paymentMethodLast4: paymentPersistence.paymentMethodLast4,
        lastErrorCode: paymentPersistence.lastErrorCode,
        lastErrorMessage: paymentPersistence.lastErrorMessage,
      });
      if (paymentPersistence.paymentMethodId) {
        await syncBookingSelectedPaymentMethodFromIntent(conn2, {
          bookingId: id,
          userId: booking.user_id,
          intent,
          paymentMethodFallback: pm || null,
          saveForFuture: requestedSavePaymentMethod,
        });
      }
      await conn2.commit();
    } catch (e) {
      try { await conn2.rollback(); } catch { }
      console.error('Error al actualizar el estado del pago final en BD:', {
        bookingId: id,
        userId: req.user?.id,
        error: e.message,
        stack: e.stack,
      });
      return res.status(400).json({ error: 'No se pudo actualizar el estado del pago.' });
    } finally {
      conn2.release();
    }

    console.log('PaymentIntent final creado/actualizado:', {
      bookingId: id,
      paymentIntentId: intent.id,
      status: intent.status,
      amountCents: amountToCharge,
      transferGroup,
      destination: booking.stripe_account_id,
    });

    if (intent.status === 'succeeded') {
      const settlementResult = await finalizeBookingSettlementAfterSuccessfulPayment(id, {
        changedByUserId: req.user?.id || null,
        reasonCode: 'final_payment_succeeded',
      });
      if (!settlementResult.settled) {
        return res.status(202).json({ manualReviewRequired: true, paymentIntentId: intent.id });
      }
      return res.status(200).json({ message: 'Pago confirmado', paymentIntentId: intent.id });
    }
    if (intent.status === 'requires_action') {
      return res.status(202).json({ requiresAction: true, clientSecret: intent.client_secret, paymentIntentId: intent.id });
    }
    if (intent.status === 'processing') {
      return res.status(202).json({ processing: true, paymentIntentId: intent.id });
    }
    return res.status(402).json({ error: 'El pago no se pudo completar', paymentIntentId: intent.id, status: intent.status });
  } catch (err) {
    try { await connection.rollback(); } catch { }
    console.error('Error en pago final de reserva:', {
      bookingId: id,
      userId: req.user?.id,
      error: err.message,
      stack: err.stack,
      stripeError: err.type || err.code || null,
      stripeMessage: err.decline_code || err.param || null,
    });
    return res.status(400).json({ error: 'No se pudo procesar el pago final.' });
  } finally {
    connection.release(); // release solo en finally
  }
});

// Pago final de una reserva completada (NO ACTIVO!)
app.post('/api/bookings/:id/final-payment', authenticateToken, (req, res) => {
  return res.status(410).json({
    error: 'deprecated_endpoint',
    message: 'Usa /api/bookings/:id/final-payment-transfer para el flujo actual.',
  });
});

// Transferir el pago final al profesional con Stripe Connect (NO ACTIVO!)
app.post('/api/bookings/:id/transfer', authenticateToken, (req, res) => {
  return res.status(410).json({
    error: 'deprecated_endpoint',
    message: 'Usa /api/bookings/:id/final-payment-transfer para el flujo actual.',
  });
});

// Generar y descargar factura en PDF de una reserva pagada (2 facturas)
app.get('/api/bookings/:id/invoice', authenticateToken, (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error getting connection:', err);
      return res.status(500).json({ error: 'Connection error.' });
    }

    const query = `
      SELECT
        b.id AS booking_id,
        b.service_status,
        b.settlement_status,
        b.requested_start_datetime,
        b.requested_end_datetime,
        b.service_currency_snapshot,
        b.estimated_commission_amount_cents,
        b.estimated_total_amount_cents,
        b.description AS booking_description,
        COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
        s.description AS service_description,
        COALESCE(p.currency, b.service_currency_snapshot) AS service_currency,
        cp.proposed_commission_amount_cents,
        cp.proposed_total_amount_cents,
        cu.email AS customer_email,
        cu.phone AS customer_phone,
        cu.first_name AS customer_first_name,
        cu.surname AS customer_surname,
        sp.id AS provider_id,
        sp.email AS provider_email,
        sp.phone AS provider_phone,
        sp.first_name AS provider_first_name,
        sp.surname AS provider_surname,
        sp.nif AS provider_nif,
        a.address_1 AS provider_address_1,
        a.address_2 AS provider_address_2,
        a.street_number AS provider_street_number,
        a.postal_code AS provider_postal_code,
        a.city AS provider_city,
        a.state AS provider_state,
        a.country AS provider_country
      FROM booking b
      JOIN user_account cu ON b.client_user_id = cu.id
      JOIN service s ON b.service_id = s.id
      JOIN price p ON s.price_id = p.id
      JOIN user_account sp ON s.user_id = sp.id
      LEFT JOIN booking_closure_proposal cp ON cp.id = (
        SELECT cp2.id
        FROM booking_closure_proposal cp2
        WHERE cp2.booking_id = b.id
        ORDER BY cp2.id DESC
        LIMIT 1
      )
      LEFT JOIN (
        SELECT cm1.* FROM collection_method cm1
        JOIN (
          SELECT user_id, MAX(id) AS max_id
          FROM collection_method
          GROUP BY user_id
        ) cm2 ON cm1.user_id = cm2.user_id AND cm1.id = cm2.max_id
      ) cm ON cm.user_id = sp.id
      LEFT JOIN address a ON a.id = cm.address_id
      WHERE b.id = ?
      LIMIT 1;
    `;

    connection.query(query, [id], async (err, results) => {
      connection.release();
      if (err) {
        console.error('Error fetching booking:', err);
        return res.status(500).json({ error: 'Error fetching booking.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Booking not found.' });
      }

      const data = results[0];
      data.is_paid = deriveLegacyIsPaid(data.settlement_status);
      data.booking_start_datetime = data.requested_start_datetime;
      data.booking_end_datetime = data.requested_end_datetime;
      data.commission = data.proposed_commission_amount_cents != null
        ? fromMinorUnits(data.proposed_commission_amount_cents, data.service_currency || 'EUR')
        : (
          data.estimated_commission_amount_cents != null
            ? fromMinorUnits(data.estimated_commission_amount_cents, data.service_currency || 'EUR')
            : 0
        );
      data.final_price = data.proposed_total_amount_cents != null
        ? fromMinorUnits(data.proposed_total_amount_cents, data.service_currency || 'EUR')
        : (
          data.estimated_total_amount_cents != null
            ? fromMinorUnits(data.estimated_total_amount_cents, data.service_currency || 'EUR')
            : 0
        );
      let depositPayment = null;
      let finalPayment = null;

      try {
        const [paymentRows] = await promisePool.query(
          `SELECT type, amount_cents, commission_snapshot_cents, final_price_snapshot_cents, currency, status
           FROM payments
           WHERE booking_id = ?
           ORDER BY id DESC`,
          [id]
        );

        const pickPayment = (type) => (
          paymentRows.find((row) => row.type === type && row.status === 'succeeded')
          || paymentRows.find((row) => row.type === type)
          || null
        );

        depositPayment = pickPayment('deposit');
        finalPayment = pickPayment('final');
      } catch (paymentLookupError) {
        console.error('Error fetching payment snapshots for invoice:', paymentLookupError);
      }

      const doc = new PDFDocument({ margins: { top: 64, left: 64, right: 64, bottom: 64 } });

      // Resource paths
      const assetsPath = path.join(__dirname, 'assets');
      doc.registerFont('Inter', path.join(assetsPath, 'fonts', 'Inter-Regular.ttf'));
      doc.registerFont('Inter-Bold', path.join(assetsPath, 'fonts', 'Inter-Bold.ttf'));

      // Capture PDF data in memory
      const buffers = [];
      doc.on('data', buffers.push.bind(buffers));
      doc.on('end', () => {
        const pdfData = Buffer.concat(buffers);
        res.set({
          'Content-Type': 'application/pdf',
          'Content-Disposition': `attachment; filename=invoice_${id}.pdf`,
          'Content-Length': pdfData.length
        });
        res.send(pdfData);
      });

      // Helpers
      const formatDate = (value) => {
        try {
          const d = new Date(value);
          const dd = String(d.getDate()).padStart(2, '0');
          const mm = String(d.getMonth() + 1).padStart(2, '0');
          const yyyy = d.getFullYear();
          return `${dd}/${mm}/${yyyy}`;
        } catch (_) {
          return '';
        }
      };
      // Decide invoice type
      const typeParam = String(req.query.type || '').toLowerCase();
      const invoiceType = (typeParam === 'deposit' || typeParam === 'final')
        ? typeParam
        : (data.is_paid ? 'final' : 'deposit');
      const invoiceCurrency = normalizeCurrencyCode(
        invoiceType === 'deposit'
          ? depositPayment?.currency
          : (finalPayment?.currency || depositPayment?.currency),
        data.service_currency || 'EUR'
      );
      const toCurrency = (amount) => formatCurrencyAmount(amount, invoiceCurrency);

      // VAT configuration (only used for provider invoice)
      const vatRateParam = req.query.vat_rate;
      let vatRateProvider = 21; // default 21%
      if (vatRateParam !== undefined) {
        const parsed = parseInt(vatRateParam, 10);
        if (!Number.isNaN(parsed) && parsed >= 0 && parsed <= 21) vatRateProvider = parsed;
      }
      const isExempt = String(req.query.exempt || '').toLowerCase() === 'true';
      const isReverseCharge = String(req.query.reverse_charge || '').toLowerCase() === 'true';

      // Common header
      let logoX, logoY, logoWidth, logoHeight;
      logoWidth = 60;
      logoX = doc.page.width - doc.page.margins.right - logoWidth;
      logoY = doc.page.margins.top;
      logoHeight = 0; // por defecto

      try {
        const logoPath = path.join(assetsPath, 'wisdom.png');

        // Si tu versión de PDFKit soporta openImage, úsalo para conocer el alto real
        if (typeof doc.openImage === 'function') {
          const img = doc.openImage(logoPath);
          logoHeight = Math.round(logoWidth * (img.height / img.width));
        } else {
          // fallback razonable si no hay openImage (supón cuadrado)
          logoHeight = logoWidth;
        }

        doc.image(logoPath, logoX, logoY, { width: logoWidth });
      } catch (e) {
        console.warn('Logo not found:', e);
        // mantenemos logoHeight como esté (0 o fallback) y seguimos
      }

      // Título debajo del logo (o del hueco del logo si no se pudo cargar)
      const titleY = logoY + (logoHeight || logoWidth) + 8;
      doc.font('Inter-Bold').fontSize(20);
      doc.font('Inter-Bold').fontSize(20);
      const innerWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
      doc.text('INVOICE', doc.page.margins.left, titleY, {
        width: innerWidth,
        align: 'center'
      });

      // Vuelve el cursor al margen izquierdo para el resto de contenido
      doc.x = doc.page.margins.left;
      doc.moveDown(1.2);

      // (opcional, evita que un error de PDF mate el dyno)
      doc.on('error', (err) => {
        console.error('PDF error:', err);
        if (!res.headersSent) res.status(500).send('PDF generation error');
      });


      // Metadata
      const now = new Date();
      const yyyy = now.getFullYear();
      const mm = String(now.getMonth() + 1).padStart(2, '0');
      const seriesNumber = invoiceType === 'deposit'
        ? `WISDOM-${yyyy}-${mm}-${data.booking_id}`
        : `PRO-${data.provider_id}-${yyyy}-${mm}-${data.booking_id}`;

      doc.font('Inter-Bold').fontSize(11).text('Series & No.');
      doc.font('Inter').fontSize(11).text(seriesNumber);
      doc.moveDown(0.4);
      doc.font('Inter-Bold').text('Issue date');
      doc.font('Inter').text(formatDate(now));
      doc.moveDown(0.4);
      doc.font('Inter-Bold').text('Date of the transaction');
      doc.font('Inter').text(formatDate(now));

      doc.moveDown();
      doc.moveDown();

      // ISSUER
      doc.font('Inter-Bold').fontSize(12).text('ISSUER');
      doc.moveDown(0.3);
      if (invoiceType === 'deposit') {
        doc.font('Inter').fontSize(11);
        doc.text('Name or company name: WISDOM, S.L.');
        doc.text('Tax ID: 39414159W');
        doc.text('Address: Font dels Reis, 60, 008304, Mataró, Barcelona, Spain');
      } else {
        const providerFullName = `${data.provider_first_name || ''} ${data.provider_surname || ''}`.trim();
        const addrParts = [
          [data.provider_address_1, data.provider_street_number].filter(Boolean).join(' '),
          [data.provider_postal_code, data.provider_city].filter(Boolean).join(', '),
          [data.provider_state, data.provider_country].filter(Boolean).join(', ')
        ].filter(Boolean).join(', ');

        doc.font('Inter').fontSize(11);
        doc.text(`Name or company name: ${providerFullName || '—'}`);
        doc.text(`Tax ID: ${data.provider_nif || '__________'}`);
        doc.text(`Address: ${addrParts || '—'}`);
      }

      doc.moveDown();

      // RECIPIENT
      doc.font('Inter-Bold').fontSize(12).text('RECIPIENT');
      doc.moveDown(0.3);
      const customerFullName = `${data.customer_first_name || ''} ${data.customer_surname || ''}`.trim();
      doc.font('Inter').fontSize(11);
      doc.text(`Full name: ${customerFullName || '—'}`);
      doc.text('Tax ID: —');
      doc.text('Address: —');

      doc.moveDown();

      // DESCRIPTION
      doc.font('Inter-Bold').fontSize(12).text('DESCRIPTION');
      doc.moveDown(0.3);
      const serviceTitleQuoted = data.service_title ? `"${data.service_title}"` : '""';
      const bookingDescQuoted = data.booking_description ? ` with description "${data.booking_description}"` : '';
      const serviceSummary = `${serviceTitleQuoted}${bookingDescQuoted}`.trim();
      if (invoiceType === 'deposit') {
        doc.font('Inter').fontSize(11).text(
          `Item: Service fee for intermediation in booking ${data.booking_id} of the service ${serviceSummary}, with scheduled date of provision ${formatDate(data.booking_start_datetime)}.`
        );
      } else {
        doc.font('Inter').fontSize(11).text(
          `Item: Provision of the service ${serviceSummary} carried out on ${formatDate(data.booking_end_datetime || data.booking_start_datetime)}. Booking reference in Wisdom: ${data.booking_id}`
        );
      }

      doc.moveDown();

      // TAX DETAILS
      doc.font('Inter-Bold').fontSize(12).text('TAX DETAILS');
      doc.moveDown(0.3);

      let taxableBase = 0;
      let vatRate = 21;
      let vatAmount = 0;
      let invoiceTotal = 0;

      if (invoiceType === 'deposit') {
        invoiceTotal = depositPayment?.amount_cents != null
          ? fromMinorUnits(depositPayment.amount_cents, invoiceCurrency)
          : Number(data.commission || 0);
        vatRate = 21;
        taxableBase = Number((invoiceTotal / (1 + vatRate / 100)).toFixed(2));
        vatAmount = Number((invoiceTotal - taxableBase).toFixed(2));
      } else {
        if (
          finalPayment?.final_price_snapshot_cents != null
          && finalPayment?.commission_snapshot_cents != null
        ) {
          const finalPriceSnapshot = fromMinorUnits(finalPayment.final_price_snapshot_cents, invoiceCurrency);
          const commissionSnapshot = fromMinorUnits(finalPayment.commission_snapshot_cents, invoiceCurrency);
          invoiceTotal = Number((finalPriceSnapshot - commissionSnapshot).toFixed(2));
        } else {
          invoiceTotal = Number((Number(data.final_price || 0) - Number(data.commission || 0)).toFixed(2));
        }
        vatRate = isExempt || isReverseCharge ? 0 : vatRateProvider;
        if (vatRate > 0) {
          taxableBase = Number((invoiceTotal / (1 + vatRate / 100)).toFixed(2));
          vatAmount = Number((invoiceTotal - taxableBase).toFixed(2));
        } else {
          taxableBase = invoiceTotal;
          vatAmount = 0;
        }
      }

      doc.font('Inter').fontSize(11);
      doc.text(`Taxable base: ${toCurrency(taxableBase)}`);
      doc.text(`VAT rate: ${vatRate > 0 ? `${vatRate}%` : (isExempt ? 'exempt' : (isReverseCharge ? 'reverse charge' : '0%'))}`);
      doc.text(`VAT amount: ${toCurrency(vatAmount)}`);
      doc.text(`Invoice total: ${toCurrency(invoiceTotal)}`);

      doc.moveDown();
      doc.moveDown();

      if (invoiceType === 'deposit') {
        doc.font('Inter').fontSize(10).fillColor('#6B7280').text(
          `This invoice refers exclusively to Wisdom’s intermediation service fee. The professional service will be invoiced by the service provider upon completion.`,
          { align: 'left' }
        );
      } else {
        doc.font('Inter').fontSize(10).fillColor('#6B7280').text(
          `Issued by a third party on behalf of and in the name of the issuer (Wisdom), pursuant to Article 5 of the Spanish Invoicing Regulations.`,
          { align: 'left' }
        );
        if (isExempt) {
          doc.moveDown(0.2);
          doc.text(`exempt under Art. 20 LIVA`, { align: 'left' });
        }
        if (isReverseCharge) {
          doc.moveDown(0.2);
          doc.text(`reverse charge`, { align: 'left' });
        }
      }

      // Footer divider
      doc.moveDown(2);
      doc.fillColor('#000000');

      doc.end();
    });
  });
});

// Borrar una reserva por su id
app.delete('/api/delete_booking/:id', async (req, res) => {
  const bookingId = normalizeNullableInteger(req.params.id);
  if (!bookingId) {
    return res.status(400).json({ error: 'Id inválido.' });
  }

  try {
    const [[booking]] = await promisePool.query(
      'SELECT id FROM booking WHERE id = ? LIMIT 1',
      [bookingId]
    );

    if (!booking) {
      return res.status(404).json({ message: 'Reserva no encontrada' });
    }

    return res.status(409).json({
      error: 'Las reservas ya no se eliminan físicamente.',
      code: 'BOOKING_SOFT_DELETE_ONLY',
    });
  } catch (error) {
    console.error('Error al bloquear el borrado de la reserva:', error);
    return res.status(500).json({ error: 'Error al procesar la reserva.' });
  }
});

//Ruta para obtener las sugerencias de busqueda de servicios
app.get('/api/suggestions', async (req, res) => {
  const query = typeof req.query.query === 'string' ? req.query.query.trim() : '';

  if (!query) {
    return res.status(400).json({ error: 'La consulta de búsqueda es requerida.' });
  }

  try {
    const searchPlan = buildServiceSearchPlan(query);
    const searchClause = buildServiceSearchCandidateClause(searchPlan);
    const suggestionQuery = `
      SELECT
        service.id AS service_id,
        service.service_title,
        service.description,
        category.id AS service_category_id,
        family.id AS service_family_id,
        category_type.category_key,
        family.family_key,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        (
          SELECT JSON_ARRAYAGG(st.tag)
          FROM service_tags st
          WHERE st.service_id = service.id
        ) AS tags
      FROM service
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      WHERE service.is_hidden = 0
        ${searchClause.sql}
      ORDER BY service.service_created_datetime DESC
      LIMIT 80;
    `;

    const [candidateRows] = await promisePool.query(suggestionQuery, searchClause.params);
    const rankedRows = rankServiceSearchCandidates(searchPlan, candidateRows, { orderBy: 'recommend' });
    const suggestions = buildServiceSearchSuggestions(searchPlan, rankedRows, 8);

    if (!suggestions.length) {
      return res.status(200).json({ message: 'No se encontraron sugerencias.', notFound: true });
    }

    return res.status(200).json({ suggestions });
  } catch (error) {
    console.error('Error al obtener las sugerencias:', error);
    return res.status(500).json({ error: 'Error al obtener las sugerencias.' });
  }
});

app.get('/api/services/filter-categories', async (req, res) => {
  await ensureExchangeRatesFresh();
  const searchTerm = (req.query.query || '').trim();
  const hasSearchTerm = searchTerm.length > 0;
  const searchPattern = hasSearchTerm ? `%${searchTerm}%` : '%';
  const viewerId = Number(req.query.viewer_id ?? req.query.user_id ?? null);
  const categoryId = parseQueryNumber(req.query.category_id ?? req.query.categoryId ?? req.query.category);
  const limitRaw = parseQueryNumber(req.query.limit);
  const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 12) : 8;

  try {
    const filters = extractServiceFilters(req.query);
    const viewerCurrency = await resolveUserCurrency(viewerId, 'EUR');

    if (hasSearchTerm) {
      const searchPlan = buildServiceSearchPlan(searchTerm);
      const searchDistanceExpression = buildDistanceExpression({
        latColumn: 'service.latitude',
        lngColumn: 'service.longitude',
        originLat: filters.originLat,
        originLng: filters.originLng,
      });
      const searchFiltersClause = buildServiceFilterClause(filters, {
        distanceExpression: searchDistanceExpression,
        targetCurrency: viewerCurrency,
      });
      const searchClause = buildServiceSearchCandidateClause(searchPlan);
      const candidateQuery = `
        SELECT
          service.id AS service_id,
          service.service_title,
          service.description,
          category.id AS service_category_id,
          family.id AS service_family_id,
          category_type.category_key,
          family.family_key,
          user_account.username,
          user_account.first_name,
          user_account.surname,
          (
            SELECT JSON_ARRAYAGG(st.tag)
            FROM service_tags st
            WHERE st.service_id = service.id
          ) AS tags
        FROM service
        JOIN price ON service.price_id = price.id
        JOIN user_account ON service.user_id = user_account.id
        JOIN service_category category ON service.service_category_id = category.id
        JOIN service_family family ON category.service_family_id = family.id
        JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
        LEFT JOIN (
          SELECT
            service_id,
            COUNT(*) AS review_count,
            AVG(rating) AS average_rating
          FROM review
          GROUP BY service_id
        ) AS review_data ON service.id = review_data.service_id
        WHERE service.is_hidden = 0
          ${searchClause.sql}
          ${searchFiltersClause.sql}
        ORDER BY service.service_created_datetime DESC
        LIMIT 250;
      `;

      const [candidateRows] = await promisePool.query(candidateQuery, [
        ...searchClause.params,
        ...searchFiltersClause.params,
      ]);
      const rankedRows = rankServiceSearchCandidates(searchPlan, candidateRows, {
        orderBy: 'recommend',
        durationMinutes: filters.durationMinutes,
      });
      const recommendations = buildServiceSearchRecommendedTaxonomy(rankedRows, {
        limit,
        selectedCategoryId: categoryId,
      });

      return res.status(200).json(recommendations);
    }

    const mergedRows = [];
    const seenSuggestionIds = new Set();
    const addRows = (rows = [], suggestionType = 'category') => {
      rows.forEach((row) => {
        const numericId = suggestionType === 'family'
          ? Number(row?.service_family_id)
          : Number(row?.service_category_id);
        const uniqueKey = `${suggestionType}:${numericId}`;
        if (!Number.isFinite(numericId) || seenSuggestionIds.has(uniqueKey) || mergedRows.length >= limit) {
          return;
        }
        seenSuggestionIds.add(uniqueKey);
        mergedRows.push({ ...row, suggestion_type: suggestionType });
      });
    };

    const relatedFamilyIds = [];
    const relatedFamilySet = new Set();
    const pushFamilyId = (value) => {
      const numericFamilyId = Number(value);
      if (!Number.isFinite(numericFamilyId) || relatedFamilySet.has(numericFamilyId)) return;
      relatedFamilySet.add(numericFamilyId);
      relatedFamilyIds.push(numericFamilyId);
    };

    const requestedFamilyIds = Array.isArray(filters.serviceFamilyIds)
      ? filters.serviceFamilyIds.map((value) => Number(value)).filter((value) => Number.isFinite(value))
      : [];
    requestedFamilyIds.forEach(pushFamilyId);

    if (!relatedFamilyIds.length && Number.isFinite(categoryId)) {
      const [[categoryRow]] = await promisePool.query(
        'SELECT service_family_id FROM service_category WHERE id = ? LIMIT 1',
        [categoryId]
      );
      const familyCandidate = categoryRow?.service_family_id;
      pushFamilyId(familyCandidate);
    }

    const recommendationFilters = {
      ...filters,
      categoryIds: [],
      serviceFamilyIds: hasSearchTerm ? [] : relatedFamilyIds,
      serviceFamilyId: hasSearchTerm ? null : (relatedFamilyIds[0] ?? null),
    };

    const distanceExpression = buildDistanceExpression({
      latColumn: 'service.latitude',
      lngColumn: 'service.longitude',
      originLat: recommendationFilters.originLat,
      originLng: recommendationFilters.originLng,
    });
    const filtersClause = buildServiceFilterClause(recommendationFilters, {
      distanceExpression,
      targetCurrency: viewerCurrency,
    });

    const queryRecommendedCategories = `
      SELECT
        category.id AS service_category_id,
        category.service_family_id,
        category_type.id AS service_category_type_id,
        category_type.category_key AS service_category_name,
        family.family_key AS service_family_name,
        COUNT(DISTINCT service.id) AS matching_services
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      LEFT JOIN (
        SELECT
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.is_hidden = 0
        AND (
          service.service_title LIKE ?
          OR category_type.category_key LIKE ?
          OR family.family_key LIKE ?
          OR EXISTS (
            SELECT 1 FROM service_tags st WHERE st.service_id = service.id AND st.tag LIKE ?
          )
          OR service.description LIKE ?
        )
        ${filtersClause.sql}
      GROUP BY
        category.id,
        category.service_family_id,
        category_type.id,
        category_type.category_key,
        family.family_key
      ORDER BY
        CASE
          WHEN ? <> '' AND category_type.category_key LIKE ? THEN 0
          WHEN ? <> '' AND family.family_key LIKE ? THEN 1
          ELSE 2
        END,
        matching_services DESC,
        category_type.category_key ASC
      LIMIT ?;
    `;

    const params = [
      ...new Array(5).fill(searchPattern),
      ...filtersClause.params,
      searchTerm,
      searchPattern,
      searchTerm,
      searchPattern,
      limit,
    ];

    const [strictRowsRaw] = await promisePool.query(queryRecommendedCategories, params);
    const strictRows = Array.isArray(strictRowsRaw) ? strictRowsRaw : [];
    addRows(strictRows, 'category');
    strictRows.forEach((row) => pushFamilyId(row?.service_family_id));

    const familyFilters = {
      ...filters,
      categoryIds: [],
      serviceFamilyIds: [],
      serviceFamilyId: null,
    };
    const familyFiltersClause = buildServiceFilterClause(familyFilters, {
      distanceExpression,
      targetCurrency: viewerCurrency,
    });
    const restrictedFamilyIds = !hasSearchTerm && relatedFamilyIds.length > 0 ? relatedFamilyIds : [];
    const familyRestrictionClause = restrictedFamilyIds.length > 0
      ? `AND family.id IN (${restrictedFamilyIds.map(() => '?').join(', ')})`
      : '';
    const familyQuery = `
      SELECT
        family.id AS service_family_id,
        family.family_key AS service_family_name,
        COUNT(DISTINCT service.id) AS matching_services
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      LEFT JOIN (
        SELECT
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.is_hidden = 0
        AND (
          service.service_title LIKE ?
          OR category_type.category_key LIKE ?
          OR family.family_key LIKE ?
          OR EXISTS (
            SELECT 1 FROM service_tags st WHERE st.service_id = service.id AND st.tag LIKE ?
          )
          OR service.description LIKE ?
        )
        ${familyRestrictionClause}
        ${familyFiltersClause.sql}
      GROUP BY family.id, family.family_key
      ORDER BY
        CASE
          WHEN ? <> '' AND family.family_key LIKE ? THEN 0
          ELSE 1
        END,
        matching_services DESC,
        family.family_key ASC
      LIMIT ?;
    `;
    const familyParams = [
      ...new Array(5).fill(searchPattern),
      ...restrictedFamilyIds,
      ...familyFiltersClause.params,
      searchTerm,
      searchPattern,
      limit,
    ];
    const [familyRowsRaw] = await promisePool.query(familyQuery, familyParams);
    const familyRows = Array.isArray(familyRowsRaw) ? familyRowsRaw : [];
    addRows(familyRows, 'family');
    familyRows.forEach((row) => pushFamilyId(row?.service_family_id));

    if (mergedRows.length < limit && relatedFamilyIds.length > 0) {
      const broaderFilters = {
        ...recommendationFilters,
        categoryIds: [],
        serviceFamilyIds: [],
        serviceFamilyId: null,
      };
      const broaderFiltersClause = buildServiceFilterClause(broaderFilters, {
        distanceExpression,
        targetCurrency: viewerCurrency,
      });
      const familyPlaceholders = relatedFamilyIds.map(() => '?').join(', ');
      const selectedCategoryId = Number.isFinite(categoryId) ? Number(categoryId) : -1;
      const broaderQuery = `
        SELECT
          category.id AS service_category_id,
          category.service_family_id,
          category_type.id AS service_category_type_id,
          category_type.category_key AS service_category_name,
          family.family_key AS service_family_name,
          COUNT(DISTINCT service.id) AS matching_services
        FROM service
        JOIN price ON service.price_id = price.id
        JOIN user_account ON service.user_id = user_account.id
        JOIN service_category category ON service.service_category_id = category.id
        JOIN service_family family ON category.service_family_id = family.id
        JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
        LEFT JOIN (
          SELECT
            service_id,
            COUNT(*) AS review_count,
            AVG(rating) AS average_rating
          FROM review
          GROUP BY service_id
        ) AS review_data ON service.id = review_data.service_id
        WHERE service.is_hidden = 0
          AND family.id IN (${familyPlaceholders})
          ${broaderFiltersClause.sql}
        GROUP BY
          category.id,
          category.service_family_id,
          category_type.id,
          category_type.category_key,
          family.family_key
        ORDER BY
          CASE
            WHEN category.id = ? THEN 0
            ELSE 1
          END,
          matching_services DESC,
          category_type.category_key ASC
        LIMIT ?;
      `;

      const broaderParams = [
        ...relatedFamilyIds,
        ...broaderFiltersClause.params,
        selectedCategoryId,
        limit,
      ];

      const [broaderRowsRaw] = await promisePool.query(broaderQuery, broaderParams);
      addRows(Array.isArray(broaderRowsRaw) ? broaderRowsRaw : [], 'category');
    }

    if (mergedRows.length < limit && relatedFamilyIds.length > 0) {
      const familyPlaceholders = relatedFamilyIds.map(() => '?').join(', ');
      const selectedCategoryId = Number.isFinite(categoryId) ? Number(categoryId) : -1;
      const catalogQuery = `
        SELECT
          sc.id AS service_category_id,
          sc.service_family_id,
          sct.id AS service_category_type_id,
          sct.category_key AS service_category_name,
          sf.family_key AS service_family_name,
          0 AS matching_services
        FROM service_category sc
        JOIN service_category_type sct ON sc.service_category_type_id = sct.id
        JOIN service_family sf ON sc.service_family_id = sf.id
        WHERE sc.service_family_id IN (${familyPlaceholders})
        ORDER BY
          CASE
            WHEN sc.id = ? THEN 0
            ELSE 1
          END,
          sct.category_key ASC
        LIMIT ?;
      `;

      const catalogParams = [
        ...relatedFamilyIds,
        selectedCategoryId,
        limit * 2,
      ];

      const [catalogRowsRaw] = await promisePool.query(catalogQuery, catalogParams);
      addRows(Array.isArray(catalogRowsRaw) ? catalogRowsRaw : [], 'category');
    }

    return res.status(200).json(mergedRows);
  } catch (error) {
    console.error('Error al obtener las categorías recomendadas:', error);
    return res.status(500).json({ error: 'Error al obtener las categorías recomendadas.' });
  }
});

//Ruta para obtener todos los servicios de una busqueda
app.get('/api/services', async (req, res) => {
  await ensureExchangeRatesFresh();
  const searchTerm = (req.query.query || '').trim();
  const hasSearchTerm = searchTerm.length > 0;
  const searchPattern = hasSearchTerm ? `%${searchTerm}%` : '%';
  const viewerId = Number(req.query.viewer_id ?? req.query.user_id ?? null);
  const viewerCurrency = await resolveUserCurrency(viewerId, 'EUR');
  const orderByRaw = req.query.order_by ?? req.query.orderBy ?? '';
  const orderBy = typeof orderByRaw === 'string' ? orderByRaw.toLowerCase() : '';
  const categoryId = parseQueryNumber(req.query.category_id ?? req.query.categoryId ?? req.query.category);

  const filters = extractServiceFilters(req.query);
  const distanceExpression = buildDistanceExpression({
    latColumn: 'service.latitude',
    lngColumn: 'service.longitude',
    originLat: filters.originLat,
    originLng: filters.originLng,
  });
  const filtersClause = buildServiceFilterClause(filters, {
    distanceExpression,
    targetCurrency: viewerCurrency,
  });
  const includeDistanceColumn = Boolean(distanceExpression && distanceExpression.sql);
  const distanceSelect = includeDistanceColumn
    ? `,
        ${distanceExpression.sql} AS distance_km`
    : '';
  const distanceSelectParams = includeDistanceColumn ? [...distanceExpression.params] : [];
  const categoryClause = Number.isFinite(categoryId) ? 'AND service.service_category_id = ?' : '';

  if (hasSearchTerm) {
    const searchPlan = buildServiceSearchPlan(searchTerm);
    const searchClause = buildServiceSearchCandidateClause(searchPlan);
    const candidateQuery = `
      SELECT
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.is_hidden,
        service.service_created_datetime,
        service.last_edit_datetime,
        price.price,
        price.currency AS currency,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.is_verified,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        COALESCE(booking_data.confirmed_booking_count, 0) AS booking_count,
        COALESCE(repeat_data.repeated_bookings_count, 0) AS repeated_bookings_count,
        COALESCE(likes_data.likes_count, 0) AS likes_count,
        category_type.category_key,
        family.family_key,
        category_type.category_key AS service_category_name,
        family.family_key AS service_family,
        family.id AS service_family_id,
        tags_data.tags,
        images_data.images,
        language_data.languages
        ${distanceSelect}
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      LEFT JOIN (
        SELECT
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      LEFT JOIN (
        SELECT
          service_id,
          SUM(CASE WHEN service_status IN ('accepted', 'in_progress', 'finished') THEN 1 ELSE 0 END) AS confirmed_booking_count
        FROM booking
        GROUP BY service_id
      ) AS booking_data ON service.id = booking_data.service_id
      LEFT JOIN (
        SELECT
          service_id,
          COALESCE(SUM(completed_count) - COUNT(*), 0) AS repeated_bookings_count
        FROM (
          SELECT
            service_id,
            client_user_id,
            COUNT(*) AS completed_count
          FROM booking
          WHERE service_status = 'finished'
          GROUP BY service_id, client_user_id
        ) AS completed_by_user
        GROUP BY service_id
      ) AS repeat_data ON service.id = repeat_data.service_id
      LEFT JOIN (
        SELECT
          il.service_id,
          COUNT(DISTINCT sl.user_id) AS likes_count
        FROM item_list il
        JOIN service_list sl ON il.list_id = sl.id
        GROUP BY il.service_id
      ) AS likes_data ON likes_data.service_id = service.id
      LEFT JOIN (
        SELECT
          ordered_tags.service_id,
          JSON_ARRAYAGG(ordered_tags.tag) AS tags
        FROM (
          SELECT service_id, tag
          FROM service_tags
          ORDER BY service_id, tag
        ) AS ordered_tags
        GROUP BY ordered_tags.service_id
      ) AS tags_data ON tags_data.service_id = service.id
      LEFT JOIN (
        SELECT
          ordered_images.service_id,
          JSON_ARRAYAGG(
            JSON_OBJECT(
              'id', ordered_images.id,
              'image_url', ordered_images.image_url,
              'object_name', ordered_images.object_name,
              'order', ordered_images.\`order\`
            )
          ) AS images
        FROM (
          SELECT
            si.service_id,
            si.id,
            si.image_url,
            si.object_name,
            si.\`order\`
          FROM service_image si
          ORDER BY si.service_id, si.\`order\`
        ) AS ordered_images
        GROUP BY ordered_images.service_id
      ) AS images_data ON images_data.service_id = service.id
      LEFT JOIN (
        SELECT service_id, JSON_ARRAYAGG(language) AS languages
        FROM service_language
        GROUP BY service_id
      ) AS language_data ON language_data.service_id = service.id
      WHERE service.is_hidden = 0
        ${searchClause.sql}
        ${categoryClause}
        ${filtersClause.sql}
      ORDER BY service.service_created_datetime DESC;
    `;

    try {
      const params = [
        ...distanceSelectParams,
        ...searchClause.params,
      ];
      if (Number.isFinite(categoryId)) {
        params.push(categoryId);
      }
      params.push(...filtersClause.params);

      const [candidateRows] = await promisePool.query(candidateQuery, params);
      const rankedRows = rankServiceSearchCandidates(searchPlan, candidateRows, {
        orderBy,
        durationMinutes: filters.durationMinutes,
      });

      if (!rankedRows.length) {
        return res.status(200).json({
          notFound: true,
          message: 'No se encontraron servicios que coincidan con la búsqueda.'
        });
      }

      let likedServiceIds = new Set();
      if (Number.isFinite(viewerId)) {
        const serviceIds = rankedRows
          .map((service) => service.service_id)
          .filter((serviceId) => serviceId !== null && serviceId !== undefined);

        if (serviceIds.length > 0) {
          const placeholders = serviceIds.map(() => '?').join(', ');
          const likedQuery = `
            SELECT DISTINCT il.service_id
            FROM item_list il
            JOIN service_list sl ON il.list_id = sl.id
            LEFT JOIN shared_list sh ON sh.list_id = il.list_id
            WHERE (sl.user_id = ? OR sh.user_id = ?)
              AND il.service_id IN (${placeholders})
          `;

          try {
            const [likedRows] = await promisePool.query(
              likedQuery,
              [viewerId, viewerId, ...serviceIds]
            );
            likedServiceIds = new Set(likedRows.map((row) => Number(row.service_id)));
          } catch (likeError) {
            console.error('Error consultando liked services:', likeError);
          }
        }
      }

      const parseJsonSafe = (value, fallback) => {
        if (Array.isArray(value)) return value;
        if (value === null || value === undefined) return fallback;
        if (typeof value === 'string') {
          try {
            const parsed = JSON.parse(value);
            return Array.isArray(parsed) ? parsed : parsed ?? fallback;
          } catch (error) {
            return fallback;
          }
        }
        return value ?? fallback;
      };

      const services = rankedRows.map((service) => ({
        ...service,
        tags: parseJsonSafe(service.tags, []),
        images: parseJsonSafe(service.images, []),
        languages: parseJsonSafe(service.languages, []),
        distance_km: service.distance_km !== undefined && service.distance_km !== null ? Number(service.distance_km) : null,
        is_liked: likedServiceIds.has(Number(service.service_id)) ? 1 : 0,
      }));

      return res.status(200).json(services);
    } catch (error) {
      console.error('Error al obtener la información de los servicios:', error);
      return res.status(500).json({ error: 'Error al obtener la información de los servicios.' });
    }
  }

  const comparablePriceExpression = buildComparablePriceExpression({
    priceAlias: 'price',
    durationMinutes: filters.durationMinutes,
    targetCurrency: viewerCurrency,
  });
  const bayesianRatingExpression = buildBayesianRatingExpression({ reviewAlias: 'review_data' });
  const recommendDistanceBoostExpression = includeDistanceColumn
    ? `CASE
        WHEN distance_km IS NULL THEN 0
        ELSE GREATEST(0, 1 - LEAST(distance_km, 50) / 50) * 14
      END`
    : '0';
  const recommendSearchScoreExpression = `
    CASE
      WHEN ? <> '' AND LOWER(service.service_title) = LOWER(?) THEN 60
      WHEN ? <> '' AND service.service_title LIKE ? THEN 38
      WHEN ? <> '' AND category_type.category_key LIKE ? THEN 24
      WHEN ? <> '' AND family.family_key LIKE ? THEN 18
      WHEN ? <> '' AND EXISTS (
        SELECT 1 FROM service_tags st WHERE st.service_id = service.id AND st.tag LIKE ?
      ) THEN 16
      WHEN ? <> '' AND service.description LIKE ? THEN 8
      ELSE 0
    END
  `.trim();
  const recommendQualityScoreExpression = `
    (
      ((${bayesianRatingExpression} / 5.0) * 32)
      + (LEAST(LOG10(COALESCE(review_data.review_count, 0) + 1) / LOG10(31), 1) * 10)
      + (LEAST(LOG10(COALESCE(booking_data.confirmed_booking_count, 0) + 1) / LOG10(51), 1) * 18)
      + (LEAST(LOG10(COALESCE(repeat_data.repeated_bookings_count, 0) + 1) / LOG10(11), 1) * 10)
      + (
        CASE
          WHEN service.action_rate IS NULL THEN 6
          ELSE GREATEST(0, 1 - LEAST(service.action_rate, 1440) / 1440) * 10
        END
      )
      + (CASE WHEN user_account.is_verified = 1 THEN 8 ELSE 0 END)
      + (LEAST(LOG10(COALESCE(likes_data.likes_count, 0) + 1) / LOG10(21), 1) * 6)
      + ${recommendDistanceBoostExpression}
    )
  `.trim();
  const noveltyBoostExpression = `
    CASE
      WHEN TIMESTAMPDIFF(DAY, service.service_created_datetime, NOW()) BETWEEN 0 AND 21
        THEN 1 + ((21 - TIMESTAMPDIFF(DAY, service.service_created_datetime, NOW())) / 21.0) * 0.15
      ELSE 1
    END
  `.trim();
  const recommendOrderParams = [
    searchTerm,
    searchTerm,
    searchTerm,
    searchPattern,
    searchTerm,
    searchPattern,
    searchTerm,
    searchPattern,
    searchTerm,
    searchPattern,
    searchTerm,
    searchPattern,
  ];

  let orderClause = '';
  let orderParams = [];
  switch (orderBy) {
    case 'cheapest':
      orderClause = `ORDER BY
        CASE WHEN price.price_type = 'budget' THEN 1 ELSE 0 END ASC,
        ${comparablePriceExpression} ASC,
        service.service_created_datetime DESC`;
      break;
    case 'mostexpensive':
      orderClause = `ORDER BY
        CASE WHEN price.price_type = 'budget' THEN 1 ELSE 0 END ASC,
        ${comparablePriceExpression} DESC,
        service.service_created_datetime DESC`;
      break;
    case 'bestrated':
      orderClause = `ORDER BY
        ${bayesianRatingExpression} DESC,
        COALESCE(review_data.review_count, 0) DESC,
        COALESCE(review_data.average_rating, 0) DESC,
        service.service_created_datetime DESC`;
      break;
    case 'nearest':
      if (includeDistanceColumn) {
        orderClause = `ORDER BY 
          CASE WHEN distance_km IS NULL THEN 1 ELSE 0 END ASC,
          CASE
            WHEN distance_km IS NULL THEN NULL
            WHEN COALESCE(service.user_can_consult, 0) = 1 THEN distance_km + 1000
            ELSE distance_km
          END ASC,
          distance_km ASC,
          COALESCE(review_data.average_rating, 0) DESC,
          service.service_created_datetime DESC`;
      } else {
        orderClause = `ORDER BY
        CASE
          WHEN service.service_title LIKE ? THEN 1
          WHEN category_type.category_key LIKE ? THEN 1
          WHEN family.family_key LIKE ? THEN 1
          WHEN EXISTS (
            SELECT 1 FROM service_tags st WHERE st.service_id = service.id AND st.tag LIKE ?
          ) THEN 1
          WHEN service.description LIKE ? THEN 2
          ELSE 3
        END,
        service.service_created_datetime DESC`;
        orderParams = new Array(5).fill(searchPattern);
      }
      break;
    case 'availability':
      orderClause = `ORDER BY
        CASE WHEN service.action_rate IS NULL THEN 1 ELSE 0 END ASC,
        service.action_rate ASC,
        service.service_created_datetime DESC`;
      break;
    case 'recommend':
    default:
      orderClause = `ORDER BY
        (
          ((${recommendSearchScoreExpression}) * 1.35)
          + ${recommendQualityScoreExpression}
        ) * ${noveltyBoostExpression} DESC,
        ${bayesianRatingExpression} DESC,
        COALESCE(booking_data.confirmed_booking_count, 0) DESC,
        service.service_created_datetime DESC`;
      orderParams = [...recommendOrderParams];
      break;
  }
  if (!orderClause) {
    orderClause = `ORDER BY service.service_created_datetime DESC`;
  }

  const queryServices = `
      SELECT
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.is_hidden,
        service.service_created_datetime,
        service.last_edit_datetime,
        price.price,
        price.currency AS currency,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.is_verified,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        COALESCE(booking_data.confirmed_booking_count, 0) AS booking_count,
        COALESCE(repeat_data.repeated_bookings_count, 0) AS repeated_bookings_count,
        COALESCE(likes_data.likes_count, 0) AS likes_count,
        category_type.category_key,
        family.family_key,
        category_type.category_key AS service_category_name,
        family.family_key AS service_family,
        family.id AS service_family_id,
        tags_data.tags,
        images_data.images,
        language_data.languages
        ${distanceSelect}
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      LEFT JOIN (
        SELECT
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      LEFT JOIN (
        SELECT
          service_id,
          SUM(CASE WHEN service_status IN ('accepted', 'in_progress', 'finished') THEN 1 ELSE 0 END) AS confirmed_booking_count
        FROM booking
        GROUP BY service_id
      ) AS booking_data ON service.id = booking_data.service_id
      LEFT JOIN (
        SELECT
          service_id,
          COALESCE(SUM(completed_count) - COUNT(*), 0) AS repeated_bookings_count
        FROM (
          SELECT
            service_id,
            client_user_id,
            COUNT(*) AS completed_count
          FROM booking
          WHERE service_status = 'finished'
          GROUP BY service_id, client_user_id
        ) AS completed_by_user
        GROUP BY service_id
      ) AS repeat_data ON service.id = repeat_data.service_id
      LEFT JOIN (
        SELECT
          il.service_id,
          COUNT(DISTINCT sl.user_id) AS likes_count
        FROM item_list il
        JOIN service_list sl ON il.list_id = sl.id
        GROUP BY il.service_id
      ) AS likes_data ON likes_data.service_id = service.id
      LEFT JOIN (
        SELECT
          ordered_tags.service_id,
          JSON_ARRAYAGG(ordered_tags.tag) AS tags
        FROM (
          SELECT service_id, tag
          FROM service_tags
          ORDER BY service_id, tag
        ) AS ordered_tags
        GROUP BY ordered_tags.service_id
      ) AS tags_data ON tags_data.service_id = service.id
      LEFT JOIN (
        SELECT
          ordered_images.service_id,
          JSON_ARRAYAGG(
            JSON_OBJECT(
              'id', ordered_images.id,
              'image_url', ordered_images.image_url,
              'object_name', ordered_images.object_name,
              'order', ordered_images.\`order\`
            )
          ) AS images
        FROM (
          SELECT
            si.service_id,
            si.id,
            si.image_url,
            si.object_name,
            si.\`order\`
          FROM service_image si
          ORDER BY si.service_id, si.\`order\`
        ) AS ordered_images
        GROUP BY ordered_images.service_id
      ) AS images_data ON images_data.service_id = service.id
      LEFT JOIN (
        SELECT service_id, JSON_ARRAYAGG(language) AS languages
        FROM service_language
        GROUP BY service_id
      ) AS language_data ON language_data.service_id = service.id
      WHERE service.is_hidden = 0
        AND (
          service.service_title LIKE ?
          OR category_type.category_key LIKE ?
          OR family.family_key LIKE ?
          OR EXISTS (
            SELECT 1 FROM service_tags st WHERE st.service_id = service.id AND st.tag LIKE ?
          )
          OR service.description LIKE ?
        )
        ${categoryClause}
        ${filtersClause.sql}
      ${orderClause};`;

  try {
    const whereSearchParams = new Array(5).fill(searchPattern);
    const params = [
      ...distanceSelectParams,
      ...whereSearchParams,
    ];
    if (Number.isFinite(categoryId)) {
      params.push(categoryId);
    }
    params.push(...filtersClause.params);
    params.push(...orderParams);
    const [servicesData] = await promisePool.query(queryServices, params);

    if (servicesData.length > 0) {
      // Marcar is_liked si llega un viewerId válido 
      let likedServiceIds = new Set();
      if (Number.isFinite(viewerId)) {
        const serviceIds = servicesData
          .map(s => s.service_id)
          .filter(id => id !== null && id !== undefined);

        if (serviceIds.length > 0) {
          const placeholders = serviceIds.map(() => '?').join(', ');
          const likedQuery = ` 
            SELECT DISTINCT il.service_id 
            FROM item_list il 
            JOIN service_list sl ON il.list_id = sl.id 
            LEFT JOIN shared_list sh ON sh.list_id = il.list_id 
            WHERE (sl.user_id = ? OR sh.user_id = ?) 
              AND il.service_id IN (${placeholders}) 
          `;
          try {
            const [likedRows] = await promisePool.query(
              likedQuery,
              [viewerId, viewerId, ...serviceIds]
            );
            likedServiceIds = new Set(likedRows.map(r => Number(r.service_id)));
          } catch (e) {
            console.error('Error consultando liked services:', e);
          }
        }
      }

      const parseJsonSafe = (value, fallback) => {
        if (Array.isArray(value)) return value;
        if (value === null || value === undefined) return fallback;
        if (typeof value === 'string') {
          try {
            const parsed = JSON.parse(value);
            return Array.isArray(parsed) ? parsed : parsed ?? fallback;
          } catch (err) {
            return fallback;
          }
        }
        return value ?? fallback;
      };

      const withLiked = servicesData.map(s => ({
        ...s,
        tags: parseJsonSafe(s.tags, []),
        images: parseJsonSafe(s.images, []),
        languages: parseJsonSafe(s.languages, []),
        distance_km: s.distance_km !== undefined && s.distance_km !== null ? Number(s.distance_km) : null,
        is_liked: likedServiceIds.has(Number(s.service_id)) ? 1 : 0,
      }));
      return res.status(200).json(withLiked);
    }

    if (!hasSearchTerm) {
      return res.status(200).json([]);
    }

    return res.status(200).json({
      notFound: true,
      message: 'No se encontraron servicios que coincidan con la búsqueda.'
    });
  } catch (error) {
    console.error('Error al obtener la información de los servicios:', error);
    return res.status(500).json({ error: 'Error al obtener la información de los servicios.' });
  }
});

app.get('/api/services/count', async (req, res) => {
  await ensureExchangeRatesFresh();
  const searchTerm = (req.query.query || '').trim();
  const hasSearchTerm = searchTerm.length > 0;
  const searchPattern = hasSearchTerm ? `%${searchTerm}%` : '%';
  const viewerId = Number(req.query.viewer_id ?? req.query.user_id ?? null);
  const categoryId = parseQueryNumber(req.query.category_id ?? req.query.categoryId ?? req.query.category);
  const filters = extractServiceFilters(req.query);
  const viewerCurrency = await resolveUserCurrency(viewerId, 'EUR');
  const distanceExpression = buildDistanceExpression({
    latColumn: 'service.latitude',
    lngColumn: 'service.longitude',
    originLat: filters.originLat,
    originLng: filters.originLng,
  });
  const filtersClause = buildServiceFilterClause(filters, {
    distanceExpression,
    targetCurrency: viewerCurrency,
  });
  const categoryClause = Number.isFinite(categoryId) ? 'AND service.service_category_id = ?' : '';

  if (hasSearchTerm) {
    try {
      const searchPlan = buildServiceSearchPlan(searchTerm);
      const searchClause = buildServiceSearchCandidateClause(searchPlan);
      const candidateQuery = `
        SELECT
          service.id AS service_id,
          service.service_title,
          service.description,
          category.id AS service_category_id,
          family.id AS service_family_id,
          category_type.category_key,
          family.family_key,
          user_account.username,
          user_account.first_name,
          user_account.surname,
          (
            SELECT JSON_ARRAYAGG(st.tag)
            FROM service_tags st
            WHERE st.service_id = service.id
          ) AS tags
        FROM service
        JOIN price ON service.price_id = price.id
        JOIN user_account ON service.user_id = user_account.id
        JOIN service_category category ON service.service_category_id = category.id
        JOIN service_family family ON category.service_family_id = family.id
        JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
        LEFT JOIN (
          SELECT service_id, COUNT(*) AS review_count, AVG(rating) AS average_rating
          FROM review
          GROUP BY service_id
        ) AS review_data ON service.id = review_data.service_id
        WHERE service.is_hidden = 0
          ${searchClause.sql}
          ${categoryClause}
          ${filtersClause.sql};
      `;

      const params = [
        ...searchClause.params,
      ];
      if (Number.isFinite(categoryId)) params.push(categoryId);
      params.push(...filtersClause.params);

      const [candidateRows] = await promisePool.query(candidateQuery, params);
      const rankedRows = rankServiceSearchCandidates(searchPlan, candidateRows, {
        orderBy: 'recommend',
        durationMinutes: filters.durationMinutes,
      });

      return res.status(200).json({ count: rankedRows.length });
    } catch (error) {
      console.error('Error al contar servicios:', error);
      return res.status(500).json({ error: 'Error al contar los servicios.' });
    }
  }

  const countQuery = `
    SELECT COUNT(DISTINCT service.id) AS total
    FROM service
    JOIN price ON service.price_id = price.id
    JOIN user_account ON service.user_id = user_account.id
    JOIN service_category category ON service.service_category_id = category.id
    JOIN service_family family ON category.service_family_id = family.id
    JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
    LEFT JOIN (
      SELECT service_id, COUNT(*) AS review_count, AVG(rating) AS average_rating
      FROM review
      GROUP BY service_id
    ) AS review_data ON service.id = review_data.service_id
    WHERE service.is_hidden = 0
      AND (
        service.service_title LIKE ?
        OR category_type.category_key LIKE ?
        OR family.family_key LIKE ?
        OR EXISTS (
          SELECT 1 FROM service_tags st WHERE st.service_id = service.id AND st.tag LIKE ?
        )
        OR service.description LIKE ?
      )
      ${categoryClause}
      ${filtersClause.sql};
  `;

  try {
    const params = [];
    params.push(...new Array(5).fill(searchPattern));
    if (Number.isFinite(categoryId)) params.push(categoryId);
    params.push(...filtersClause.params);

    const [[row]] = await promisePool.query(countQuery, params);
    const total = row && row.total !== undefined && row.total !== null ? Number(row.total) : 0;
    return res.status(200).json({ count: Number.isFinite(total) ? total : 0 });
  } catch (error) {
    console.error('Error al contar servicios:', error);
    return res.status(500).json({ error: 'Error al contar los servicios.' });
  }
});

//Ruta para obtener la información de un servicio por su id
app.get('/api/services/:id', (req, res) => {
  const { id } = req.params; // ID del servicio

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    const query = `
      SELECT
        service.id AS service_id,
        service.service_title,
        service.description,
        service.service_category_id,
        service.price_id,
        service.latitude,
        service.longitude,
        service.action_rate,
        service.user_can_ask,
        service.user_can_consult,
        service.price_consult,
        service.consult_via_id,
        service.is_individual,
        service.is_hidden,
        service.service_created_datetime,
        service.last_edit_datetime,
        price.price,
        price.currency AS currency,
        price.price_type,
        user_account.id AS user_id,
        user_account.email,
        user_account.phone,
        user_account.username,
        user_account.first_name,
        user_account.surname,
        user_account.profile_picture,
        user_account.is_professional,
        user_account.language,
        COALESCE(review_data.review_count, 0) AS review_count,
        COALESCE(review_data.average_rating, 0) AS average_rating,
        category_type.category_key,
        family.family_key,
        category_type.category_key AS service_category_name,
        family.family_key AS service_family,
        (SELECT JSON_ARRAYAGG(tag)
        FROM service_tags
        WHERE service_tags.service_id = service.id) AS tags,
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'object_name', si.object_name, 'order', si.order))
        FROM service_image si
        WHERE si.service_id = service.id) AS images
      FROM service
      JOIN price ON service.price_id = price.id
      JOIN user_account ON service.user_id = user_account.id
      JOIN service_category category ON service.service_category_id = category.id
      JOIN service_family family ON category.service_family_id = family.id
      JOIN service_category_type category_type ON category.service_category_type_id = category_type.id
      LEFT JOIN (
        SELECT
          service_id,
          COUNT(*) AS review_count,
          AVG(rating) AS average_rating
        FROM review
        GROUP BY service_id
      ) AS review_data ON service.id = review_data.service_id
      WHERE service.id = ?;`;

    connection.query(query, [id], (err, serviceData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información del servicio:', err);
        res.status(500).json({ error: 'Error al obtener la información del servicio.' });
        return;
      }

      if (serviceData.length > 0) {
        const rawService = serviceData[0];

        const service = {
          ...rawService,
          is_hidden: Boolean(rawService.is_hidden),
        };

        res.status(200).json(service); // Devolver la información del servicio
      } else {
        res.status(404).json({ notFound: true, message: 'Servicio no encontrado.' });
      }
    });
  });
});

app.post('/api/services/:id/reviews', (req, res) => {
  const { id } = req.params;
  const { rating, comment } = req.body;
  const userId = req.user.id;

  if (!rating) {
    return res.status(400).json({ error: 'rating es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      INSERT INTO review (user_id, service_id, rating, comment, review_datetime)
      VALUES (?, ?, ?, ?, NOW());
    `;
    connection.query(query, [userId, id, rating, comment], (err, result) => {
      connection.release();

      if (err) {
        console.error('Error al añadir la review:', err);
        return res.status(500).json({ error: 'Error al añadir la review.' });
      }

      res.status(201).json({ message: 'Review añadida con éxito', reviewId: result.insertId });
    });
  });
});

// Crear denuncia de servicio
app.post('/api/service_reports', authenticateToken, async (req, res) => {
  const { service_id, reason_code, reason_text, description, attachments } = req.body;
  const validReasons = ['fraud', 'spam', 'incorrect_info', 'pricing_issue', 'external_contact', 'inappropriate', 'duplicate', 'other'];

  if (!service_id || !reason_code) {
    return res.status(400).json({ error: 'service_id and reason_code are required' });
  }
  if (!validReasons.includes(reason_code)) {
    return res.status(400).json({ error: 'Invalid reason_code' });
  }

  const conn = await pool.promise().getConnection();
  try {
    await conn.beginTransaction();

    const [result] = await conn.query(
      `INSERT INTO service_report (service_id, reporter_user_id, reason_code, reason_text, description)
       VALUES (?, ?, ?, ?, ?)`,
      [service_id, req.user.id, reason_code, reason_text || null, description || null]
    );
    const reportId = result.insertId;

    if (Array.isArray(attachments) && attachments.length > 0) {
      // Bulk insert seguro
      const values = attachments.map(a => [reportId, a.file_url, a.file_type]);
      const placeholders = values.map(() => '(?, ?, ?)').join(', ');
      const flat = values.flat();
      await conn.query(
        `INSERT INTO service_report_attachment (report_id, file_url, file_type) VALUES ${placeholders}`,
        flat
      );
    }

    await conn.commit();
    res.status(201).json({ id: reportId });
  } catch (err) {
    await conn.rollback();
    console.error('Error al crear la denuncia:', err);
    res.status(500).json({ error: 'Error al crear la denuncia.' });
  } finally {
    conn.release();
  }
});

// Listar denuncias
app.get('/api/service_reports', authenticateToken, async (req, res) => {
  const { mine, status, service_id, reporter_user_id, limit = 50, offset = 0 } = req.query;
  const isStaff = req.user && ['admin', 'support'].includes(req.user.role);

  const filters = [];
  const params = [];

  if (mine === 'true' || !isStaff) {
    filters.push('reporter_user_id = ?');
    params.push(req.user.id);
  } else {
    if (status) { filters.push('status = ?'); params.push(status); }
    if (service_id) { filters.push('service_id = ?'); params.push(service_id); }
    if (reporter_user_id) { filters.push('reporter_user_id = ?'); params.push(reporter_user_id); }
  }

  const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

  try {
    const [reports] = await pool.promise().query(
      `SELECT * FROM service_report ${whereClause} ORDER BY report_datetime DESC LIMIT ? OFFSET ?`,
      [...params, Number(limit), Number(offset)]
    );

    if (reports.length) {
      const ids = reports.map(r => r.id);
      const [atts] = await pool.promise().query(
        `SELECT id, report_id, file_url, file_type 
           FROM service_report_attachment 
          WHERE report_id IN (?)`,
        [ids]
      );
      const grouped = atts.reduce((acc, a) => {
        (acc[a.report_id] ||= []).push({ id: a.id, file_url: a.file_url, file_type: a.file_type });
        return acc;
      }, {});
      reports.forEach(r => { r.attachments = grouped[r.id] || []; });
    }

    res.json(reports);
  } catch (err) {
    console.error('Error al listar denuncias:', err);
    res.status(500).json({ error: 'Error al listar denuncias.' });
  }
});

// Moderar denuncia
app.patch('/api/service_reports/:id', authenticateToken, async (req, res) => {
  const reportId = req.params.id;
  const { status, resolution_notes } = req.body;

  const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
  if (!isStaff) return res.status(403).json({ error: 'Forbidden' });

  const validStatuses = ['pending', 'in_review', 'resolved', 'dismissed'];
  if (status && !validStatuses.includes(status)) {
    return res.status(400).json({ error: 'Invalid status' });
  }
  const fields = [];
  const params = [];
  if (status) { fields.push('status = ?'); params.push(status); }
  if (resolution_notes !== undefined) { fields.push('resolution_notes = ?'); params.push(resolution_notes); }
  if (!fields.length) return res.status(400).json({ error: 'No fields to update' });

  fields.push('handled_by_user_id = ?'); params.push(req.user.id);
  fields.push('handled_datetime = NOW()');

  try {
    const [result] = await pool.promise().query(
      `UPDATE service_report SET ${fields.join(', ')} WHERE id = ?`,
      [...params, reportId]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (err) {
    console.error('Error al actualizar la denuncia:', err);
    res.status(500).json({ error: 'Error al actualizar la denuncia.' });
  }
});

app.post('/api/admin/terms-update-email', async (req, res) => {
  const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
  if (!isStaff) return res.status(403).json({ error: 'Forbidden' });

  const {
    termsUrl,
    privacyUrl,
    effectiveDate,
    productName,
    dryRun = false,
    testEmail = null,
    limit = null
  } = req.body || {};

  try {
    const summary = await sendEmailToAll(pool, transporter, {
      termsUrl,
      privacyUrl,
      effectiveDate,
      productName,
      dryRun,
      testEmail,
      limit
    });
    res.json(summary);
  } catch (err) {
    console.error('Error al mandar la actualización de términos:', err);
    res.status(500).json({ error: 'Error al mandar la actualización de términos.' });
  }
});

app.post('/api/upload-dni', (req, res) => {
  uploadDni.single('file')(req, res, async (err) => {
    if (err) {
      if (err.message === 'INVALID_FILE_TYPE') {
        return res.status(400).json({ error: 'Invalid file type' });
      }
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: 'File too large' });
      }
      return res.status(400).json({ error: err.message });
    }
    if (!req.file) {
      return res.status(400).json({ error: 'File is required' });
    }

    const filePath = req.file.path;
    try {
      const stripeFile = await stripe.files.create({
        purpose: 'identity_document',
        file: {
          data: fs.createReadStream(filePath),
          name: req.file.originalname,
          type: req.file.mimetype,
        },
      });
      await fs.promises.unlink(filePath);
      return res.status(201).json({ fileToken: stripeFile.id });
    } catch (stripeErr) {
      await fs.promises.unlink(filePath).catch(() => { });
      console.error('Stripe file upload error:', stripeErr);
      return res.status(500).json({ error: 'Stripe upload failed' });
    }
  });
});







// Webhook Stripe: añade payment_intent.canceled y charge.dispute.*, guarda last4 y corrige el send(...)
app.post('/webhooks/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    console.error('Webhook signature verification failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  const connection = await pool.promise().getConnection();
  try {
    switch (event.type) {
      case 'payment_intent.succeeded':
      case 'payment_intent.processing':
      case 'payment_intent.requires_action':
      case 'payment_intent.payment_failed':
      case 'payment_intent.canceled': {
        let pi = event.data.object;
        const bookingId = pi?.metadata?.booking_id;
        const type = pi?.metadata?.type;
        if (!bookingId || !type) break;

        const paymentPersistence = await resolvePaymentIntentPersistence(pi);
        pi = paymentPersistence.intent || pi;

        const amountCents = typeof pi.amount_received === 'number'
          ? pi.amount_received
          : (typeof pi.amount === 'number' ? pi.amount : 0);
        let depositNotification = null;
        let shouldReleaseEphemeralPaymentMethods = false;
        let closureFollowUpEmailMode = null;
        const shouldSavePaymentMethod = normalizeBooleanInput(pi?.metadata?.save_payment_method, false);

        await connection.beginTransaction();
        await upsertPayment(connection, {
          bookingId,
          type,
          paymentIntentId: pi.id,
          amountCents,
          status: mapStatus(pi.status),
          currency: normalizeCurrencyCode(pi?.currency, 'EUR'),
          transferGroup: pi.transfer_group || null,
          paymentMethodId: paymentPersistence.paymentMethodId || null,
          paymentMethodLast4: paymentPersistence.paymentMethodLast4 || null,
          lastErrorCode: paymentPersistence.lastErrorCode,
          lastErrorMessage: paymentPersistence.lastErrorMessage,
        });

        if (event.type === 'payment_intent.succeeded' && type === 'deposit') {
          const [[depositPaymentRow]] = await connection.query(
            `SELECT amount_cents, final_price_snapshot_cents, currency
             FROM payments
             WHERE payment_intent_id = ?
             LIMIT 1`,
            [pi.id]
          );
          const [[bookingRow]] = await connection.query(
            `SELECT
               b.id,
               b.client_user_id,
               b.service_status,
               b.settlement_status,
               b.requested_start_datetime,
               b.requested_end_datetime,
               b.service_currency_snapshot,
               b.estimated_total_amount_cents,
               COALESCE(s.service_title, b.service_title_snapshot) AS service_title,
               client.first_name AS client_first_name,
               client.surname AS client_surname,
               client.username AS client_username,
               client.email AS client_email,
               prof.first_name AS professional_first_name,
               prof.surname AS professional_surname,
               prof.username AS professional_username,
               prof.email AS professional_email
             FROM booking b
             LEFT JOIN user_account client ON client.id = b.client_user_id
             LEFT JOIN service s ON s.id = b.service_id
             LEFT JOIN user_account prof ON prof.id = COALESCE(s.user_id, b.provider_user_id_snapshot)
             WHERE b.id = ?
             LIMIT 1`,
            [bookingId]
          );
          if (bookingRow) {
            await syncBookingSelectedPaymentMethodFromIntent(connection, {
              bookingId,
              userId: bookingRow.client_user_id,
              intent: pi,
              saveForFuture: shouldSavePaymentMethod,
            });
            await transitionBookingStateRecord(connection, bookingRow, {
              nextServiceStatus: 'requested',
              nextSettlementStatus: bookingRow.settlement_status === 'payment_failed' ? 'none' : bookingRow.settlement_status,
              reasonCode: 'deposit_succeeded_webhook',
              extraPatch: {
                deposit_confirmed_at: new Date(),
                deposit_amount_cents_snapshot: depositPaymentRow?.amount_cents ?? amountCents,
                deposit_currency_snapshot: normalizeCurrencyCode(depositPaymentRow?.currency, pi?.currency || 'EUR'),
              },
            });
            depositNotification = {
              booking: {
                id: bookingRow.id,
                serviceTitle: bookingRow.service_title,
                start: bookingRow.requested_start_datetime,
                end: bookingRow.requested_end_datetime,
                finalPrice: bookingRow.estimated_total_amount_cents != null
                  ? fromMinorUnits(bookingRow.estimated_total_amount_cents, bookingRow.service_currency_snapshot || 'EUR')
                  : null,
                professional: {
                  firstName: bookingRow.professional_first_name,
                  surname: bookingRow.professional_surname,
                  username: bookingRow.professional_username,
                  email: bookingRow.professional_email,
                },
                client: {
                  firstName: bookingRow.client_first_name,
                  surname: bookingRow.client_surname,
                  username: bookingRow.client_username,
                  email: bookingRow.client_email,
                },
              },
              depositAmountCents: depositPaymentRow?.amount_cents ?? amountCents,
              finalPriceSnapshotCents: depositPaymentRow?.final_price_snapshot_cents ?? null,
              currency: normalizeCurrencyCode(depositPaymentRow?.currency, pi?.currency || 'EUR'),
            };
          }
        }
        if (event.type === 'payment_intent.succeeded' && type === 'final') {
          const [[bookingRow]] = await connection.query(
            'SELECT id, client_user_id, service_status, settlement_status FROM booking WHERE id = ? LIMIT 1 FOR UPDATE',
            [bookingId]
          );
          if (bookingRow) {
            await syncBookingSelectedPaymentMethodFromIntent(connection, {
              bookingId,
              userId: bookingRow.client_user_id,
              intent: pi,
              saveForFuture: shouldSavePaymentMethod,
            });
          }
        }
        if ((event.type === 'payment_intent.payment_failed' || event.type === 'payment_intent.canceled') && type === 'deposit') {
          const [[bookingRow]] = await connection.query(
            'SELECT id, service_status, settlement_status FROM booking WHERE id = ? LIMIT 1 FOR UPDATE',
            [bookingId]
          );
          if (bookingRow) {
            await transitionBookingStateRecord(connection, bookingRow, {
              nextSettlementStatus: 'payment_failed',
              reasonCode: event.type === 'payment_intent.canceled'
                ? 'deposit_canceled_webhook'
                : 'deposit_failed_webhook',
            });
          }
        }
        if ((event.type === 'payment_intent.payment_failed' || event.type === 'payment_intent.canceled') && type === 'final') {
          const bookingRow = await getBookingSettlementContext(connection, bookingId, { forUpdate: true });
          if (bookingRow && normalizeSettlementStatus(bookingRow.settlement_status, 'none') !== 'paid') {
            const paymentFailureDetails = event.type === 'payment_intent.canceled'
              ? 'El cobro final se ha cancelado automáticamente.'
              : 'El cobro final ha fallado automáticamente.';
            const isPendingClientApproval = normalizeSettlementStatus(
              bookingRow.settlement_status,
              'none'
            ) === 'pending_client_approval';

            if (isPendingClientApproval && !isSecondaryClientApprovalWindowActive(bookingRow)) {
              await transitionBookingStateRecord(connection, bookingRow, {
                nextServiceStatus: bookingRow.service_status,
                nextSettlementStatus: 'pending_client_approval',
                reasonCode: event.type === 'payment_intent.canceled'
                  ? 'final_payment_canceled_follow_up_webhook'
                  : 'final_payment_failed_follow_up_webhook',
                extraPatch: {
                  client_approval_deadline_at: buildSecondaryClientApprovalDeadline(),
                },
              });
              closureFollowUpEmailMode = 'approval_follow_up';
            } else if (isPendingClientApproval) {
              await upsertBookingIssueReport(connection, {
                bookingId,
                reportedAgainstUserId: bookingRow.provider_user_id_snapshot || bookingRow.effective_provider_user_id || null,
                issueType: 'payment_dispute',
                status: 'open',
                details: paymentFailureDetails,
              });
              await transitionBookingStateRecord(connection, bookingRow, {
                nextServiceStatus: bookingRow.service_status,
                nextSettlementStatus: 'in_dispute',
                reasonCode: event.type === 'payment_intent.canceled'
                  ? 'final_payment_canceled_webhook'
                  : 'final_payment_failed_webhook',
                extraPatch: {
                  client_approval_deadline_at: null,
                },
              });
              closureFollowUpEmailMode = 'manual_review_required';
            }
          }
        }
        if (event.type === 'payment_intent.canceled' && type === 'final') {
          // opcional: revertir is_paid si fuera necesario en tu dominio
        }

        await connection.commit();

        if (event.type === 'payment_intent.succeeded' && type === 'final') {
          const settlementResult = await finalizeBookingSettlementAfterSuccessfulPayment(bookingId, {
            reasonCode: 'final_payment_succeeded_webhook',
          });
          shouldReleaseEphemeralPaymentMethods = settlementResult.settled === true;
        }

        if (shouldReleaseEphemeralPaymentMethods) {
          try {
            await releaseEphemeralBookingPaymentMethodsIfClosed(bookingId);
          } catch (cleanupError) {
            console.error('Error releasing ephemeral payment methods after final payment webhook:', {
              bookingId,
              error: cleanupError.message,
            });
          }
        }

        if (closureFollowUpEmailMode) {
          try {
            await sendClosureAutoChargeNotificationEmail({
              bookingId,
              mode: closureFollowUpEmailMode,
            });
          } catch (emailError) {
            console.error('Error sending closure follow-up email after final payment webhook:', {
              bookingId,
              mode: closureFollowUpEmailMode,
              error: emailError.message,
            });
          }
        }

        if (depositNotification) {
          try {
            const { subject, text, html } = renderDepositReservationEmail(depositNotification);
            await sendEmail({
              from: '"Wisdom" <wisdom.helpcontact@gmail.com>',
              to: 'hernanz.reio@gmail.com',
              subject,
              text,
              html,
            });
          } catch (mailErr) {
            console.error('Error sending deposit notification email:', mailErr);
          }
        }
        break;
      }

      case 'charge.refunded': {
        const charge = event.data.object;
        const piId = charge.payment_intent;
        const fullyRefunded = charge.amount_refunded >= (charge.amount_captured || charge.amount);
        if (!piId) break;
        await connection.beginTransaction();
        const [[payment]] = await connection.query(
          'SELECT booking_id, type FROM payments WHERE payment_intent_id = ? FOR UPDATE',
          [piId]
        );
        if (payment) {
          await connection.query(
            'UPDATE payments SET status = ? WHERE payment_intent_id = ?',
            [fullyRefunded ? 'refunded' : 'partially_refunded', piId]
          );
          if (fullyRefunded) {
            const [[bookingRow]] = await connection.query(
              'SELECT id, service_status, settlement_status FROM booking WHERE id = ? LIMIT 1 FOR UPDATE',
              [payment.booking_id]
            );
            if (bookingRow) {
              await transitionBookingStateRecord(connection, bookingRow, {
                nextSettlementStatus: 'refunded',
                reasonCode: payment.type === 'final' ? 'final_refunded_webhook' : 'deposit_refunded_webhook',
              });
            }
          }
        }
        await connection.commit();
        if (payment?.booking_id && fullyRefunded) {
          try {
            await releaseEphemeralBookingPaymentMethodsIfClosed(payment.booking_id);
          } catch (cleanupError) {
            console.error('Error releasing ephemeral payment methods after refund webhook:', {
              bookingId: payment.booking_id,
              error: cleanupError.message,
            });
          }
        }
        break;
      }

      // Disputas
      case 'charge.dispute.created':
      case 'charge.dispute.funds_withdrawn': {
        const dispute = event.data.object;
        const piId = dispute.charge?.payment_intent || dispute.payment_intent;
        if (!piId) break;
        await connection.beginTransaction();
        await connection.query(
          "UPDATE payments SET status = 'dispute_open' WHERE payment_intent_id = ?",
          [piId]
        );
        await connection.commit();
        break;
      }
      case 'charge.dispute.closed':
      case 'charge.dispute.funds_reinstated': {
        const dispute = event.data.object;
        const piId = dispute.charge?.payment_intent || dispute.payment_intent;
        if (!piId) break;
        const won = dispute.status === 'won';
        await connection.beginTransaction();
        await connection.query(
          "UPDATE payments SET status = ? WHERE payment_intent_id = ?",
          [won ? 'dispute_won' : 'dispute_lost', piId]
        );
        await connection.commit();
        break;
      }

      default:
        break;
    }

    res.status(200).json({ received: true });
  } catch (e) {
    console.error('Webhook handling error:', e);
    try { await connection.rollback(); } catch { }
    res.status(500).end();
  } finally {
    connection.release();
  }
});

// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en el puerto ${port}`);
});
