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

function formatCurrencyEUR(amount) {
  if (!Number.isFinite(amount)) return 'No disponible';
  return new Intl.NumberFormat('es-ES', {
    style: 'currency',
    currency: 'EUR'
  }).format(amount);
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
} = {}) {
  const safeDurationMinutes = Number.isFinite(durationMinutes) && durationMinutes > 0
    ? durationMinutes
    : null;
  const durationFactor = safeDurationMinutes !== null
    ? safeDurationMinutes / 60
    : 1;

  return `
    CASE
      WHEN ${priceAlias}.price_type = 'fix' THEN ${priceAlias}.price
      WHEN ${priceAlias}.price_type = 'hour' THEN ${priceAlias}.price * ${durationFactor}
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
} = {}) {
  const clauses = [];
  const params = [];

  const {
    minPrice,
    maxPrice,
    durationMinutes,
    maxActionRate,
    minRating,
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
    const comparablePriceExpression = buildComparablePriceExpression({ priceAlias, durationMinutes });
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
function computePricing({ priceType, unitPrice, durationMinutes }) {
  const type = String(priceType || '').toLowerCase();
  const unit = Number.parseFloat(unitPrice) || 0;
  const minutes = Math.max(0, Math.round(Number(durationMinutes) || 0));
  const hours = minutes / 60;

  let base = 0;
  if (type === 'hour') base = unit * hours;
  else base = unit;
  base = round2(base);

  let commission;
  commission = Math.max(1, round1(base * 0.1));

  const shouldNullFinal = (type === 'hour' || type === 'budget') && minutes <= 0;
  const final = round2(base + commission);

  return { base, commission, final, minutes };
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

async function getPaymentRow(conn, bookingId, type) {
  const [rows] = await conn.query(
    `SELECT id, booking_id, type, payment_intent_id, amount_cents, status,
            payment_method_id, payment_method_last4
     FROM payments
     WHERE booking_id = ? AND type = ?
     ORDER BY id DESC
     LIMIT 1
     FOR UPDATE`,
    [bookingId, type]
  );
  return rows[0] || null;
}

async function upsertPaymentRow(conn, { bookingId, type, amountCents, commissionSnapshotCents, finalPriceSnapshotCents, status }) {
  await conn.query(
    `INSERT INTO payments (booking_id, type, amount_cents, commission_snapshot_cents, final_price_snapshot_cents, status)
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE amount_cents = VALUES(amount_cents), commission_snapshot_cents = VALUES(commission_snapshot_cents), final_price_snapshot_cents = VALUES(final_price_snapshot_cents), status = VALUES(status)`,
    [bookingId, type, amountCents, commissionSnapshotCents, finalPriceSnapshotCents, status]
  );
}

// UPSERT helper para la tabla payments
async function upsertPayment(conn, { bookingId, type, paymentIntentId, amountCents, commissionSnapshotCents, finalPriceSnapshotCents, status, transferGroup, paymentMethodId, paymentMethodLast4, lastErrorCode, lastErrorMessage }) {
  await conn.query(`
    INSERT INTO payments (booking_id, type, payment_intent_id, amount_cents, commission_snapshot_cents, final_price_snapshot_cents, status, transfer_group, payment_method_id, payment_method_last4, last_error_code, last_error_message, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
    ON DUPLICATE KEY UPDATE
      payment_intent_id = VALUES(payment_intent_id),
      amount_cents = VALUES(amount_cents),
      commission_snapshot_cents = COALESCE(VALUES(commission_snapshot_cents), commission_snapshot_cents),
      final_price_snapshot_cents = COALESCE(VALUES(final_price_snapshot_cents), final_price_snapshot_cents),
      status       = VALUES(status),
      transfer_group = COALESCE(VALUES(transfer_group), transfer_group),
      payment_method_id = COALESCE(VALUES(payment_method_id), payment_method_id),
      payment_method_last4 = COALESCE(VALUES(payment_method_last4), payment_method_last4),
      last_error_code = VALUES(last_error_code),
      last_error_message = VALUES(last_error_message)
  `, [bookingId, type, paymentIntentId, amountCents ?? 0, commissionSnapshotCents ?? null, finalPriceSnapshotCents ?? null, status, transferGroup || null, paymentMethodId || null, paymentMethodLast4 || null, lastErrorCode ?? null, lastErrorMessage ?? null]);
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

function renderDepositReservationEmail({ booking, depositAmountCents }) {
  if (!booking) {
    return {
      subject: 'Se ha confirmado una reserva',
      text: 'Hola equipo, se ha confirmado una reserva.',
      html: '<p>Hola equipo, se ha confirmado una reserva.</p>'
    };
  }

  const depositEuros = (typeof depositAmountCents === 'number' ? depositAmountCents : 0) / 100;
  const depositFormatted = formatCurrencyEUR(depositEuros);
  const finalPriceNumber = booking.finalPrice != null ? Number(booking.finalPrice) : null;
  const finalPriceFormatted = finalPriceNumber != null && Number.isFinite(finalPriceNumber)
    ? formatCurrencyEUR(finalPriceNumber)
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
  //socketPath: `/cloudsql/${process.env.INSTANCE_CONNECTION_NAME}`,
  waitForConnections: true,
  connectionLimit: 20,  // Número máximo de conexiones en el pool
  acquireTimeout: 20000,  // Tiempo máximo para adquirir una conexión
  connectTimeout: 20000,     // Tiempo máximo que una conexión puede estar inactiva antes de ser liberada.
});


const promisePool = pool.promise();
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
  return /^[A-Z]{3}$/.test(normalized) ? normalized : fallback;
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
    selectParts.push('p.price AS current_price', 'p.price_type AS current_price_type');
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
      WHERE b.booking_status = 'pending_deposit'
        AND b.order_datetime < (NOW() - INTERVAL 24 HOUR)
        AND (p.id IS NULL OR p.status IN ('requires_payment_method','canceled','payment_failed'));
    `);
    console.log('[CRON] Limpieza de reservas pending_deposit ejecutada');
  } catch (e) {
    console.error('Error en cron cleanup:', e);
  }
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
        user_account.currency,
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
  const categoryId = Number(req.params.id);
  const viewerId = Number(req.query.viewer_id ?? req.query.user_id ?? null);

  const filters = extractServiceFilters(req.query);
  const filtersClause = buildServiceFilterClause(filters);

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
  try {
    normalizedExperienceYears = parseExperienceYearsInput(experience_years, 1);
  } catch (parseError) {
    return res.status(parseError.status || 400).json({ error: parseError.message || 'invalid_experience_years' });
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

      const stripeAccountQuery = 'SELECT is_verified FROM user_account WHERE id = ?';
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
        const isHiddenValue = isVerified ? 0 : 1;

        // 1. Insertar en la tabla 'price'
        const priceQuery = 'INSERT INTO price (price, price_type) VALUES (?, ?)';
        connection.query(priceQuery, [price, price_type], (err, result) => {
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
                service_title, user_id, description, service_category_id, price_id, latitude, longitude, action_rate, user_can_ask, user_can_consult, price_consult, consult_via_id, is_individual, allow_discounts, discount_rate, hobbies, experience_years, service_created_datetime, is_hidden, last_edit_datetime
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?, ?)
            `;
            const serviceValues = [
              service_title, user_id, description, service_category_id, price_id, latitude, longitude,
              action_rate, normalizedUserCanAsk, normalizedUserCanConsult, normalizedPriceConsult, consult_via_id, normalizedIsIndividual, normalizedAllowDiscounts, normalizedDiscountRate, normalizedHobbies, normalizedExperienceYears, isHiddenValue, null
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
          AND (booking_status IS NULL OR LOWER(booking_status) NOT IN ('cancelled', 'completed'))`,
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
        s.is_hidden,
        s.service_created_datetime,
        s.last_edit_datetime,
        s.allow_discounts,
        s.discount_rate,
        s.hobbies,
        s.experience_years,
        p.price,
        p.price_type,
        ua.id AS user_id,
        ua.email,
        ua.phone,
        ua.username,
        ua.first_name,
        ua.surname,
        ua.profile_picture,
        ua.is_professional,
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
           AND LOWER(b.booking_status) IN ('accepted', 'confirmed', 'completed')) AS confirmed_booking_count,
        (SELECT COALESCE(SUM(completed_count) - COUNT(*), 0)
         FROM (
           SELECT COUNT(*) AS completed_count
           FROM booking b
           WHERE b.service_id = s.id
             AND LOWER(b.booking_status) = 'completed'
           GROUP BY b.user_id
         ) AS completed_by_user) AS repeated_bookings_count,
        (SELECT COALESCE(SUM(COALESCE(b.final_price, 0) - COALESCE(b.commission, 0)), 0)
         FROM booking b
         WHERE b.service_id = s.id
           AND LOWER(b.booking_status) = 'completed') AS total_earned_amount,
        (SELECT COUNT(DISTINCT sl.user_id)
         FROM item_list il
         JOIN service_list sl ON il.list_id = sl.id
         WHERE il.service_id = s.id) AS likes_count,
        (SELECT ROUND(COALESCE(SUM(
             CASE
               WHEN b.service_duration IS NOT NULL THEN b.service_duration
               WHEN b.booking_start_datetime IS NOT NULL AND b.booking_end_datetime IS NOT NULL
                 THEN GREATEST(TIMESTAMPDIFF(MINUTE, b.booking_start_datetime, b.booking_end_datetime), 0)
               ELSE 0
             END
           ), 0) / 60, 2)
         FROM booking b
         WHERE b.service_id = s.id
           AND LOWER(b.booking_status) = 'completed') AS total_hours_completed
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
app.get('/api/user/:userId/bookings', authenticateToken, (req, res) => {
  const requestedUserId = ensureSameUserOrRespond(req, res, req.params.userId);
  if (!requestedUserId) return;
  const { status } = req.query;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todas las reservas y servicios asociados
    let query = `
      SELECT
          booking.id AS booking_id,
          booking.booking_start_datetime,
          booking.booking_end_datetime,
          booking.service_duration,
          booking.final_price,
          booking.commission,
          booking.is_paid,
          booking.booking_status,
          booking.order_datetime,
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
          price.price_type,
          user_account.id AS service_user_id,
          user_account.email,
          user_account.phone,
          user_account.username,
          user_account.first_name,
          user_account.surname,
          user_account.profile_picture,
          user_account.is_professional,
          user_account.language,
          (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'object_name', si.object_name, 'order', si.order))
          FROM service_image si 
          WHERE si.service_id = service.id) AS images
      FROM booking
      LEFT JOIN service ON booking.service_id = service.id
      LEFT JOIN price ON service.price_id = price.id
      LEFT JOIN user_account ON service.user_id = user_account.id
      WHERE booking.user_id = ?`;

    const params = [requestedUserId];
    if (status) {
      query += ' AND booking.booking_status = ?';
      params.push(status);
    }
    query += ' ORDER BY booking.order_datetime DESC;';

    connection.query(query, params, (err, bookingsData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de las reservas:', err);
        res.status(500).json({ error: 'Error al obtener la información de las reservas.' });
        return;
      }

      if (bookingsData.length > 0) {
        res.status(200).json(bookingsData); // Devolver la lista de reservas con la información del servicio y usuario
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron reservas para este usuario.' });
      }
    });
  });
});

//Ruta para obtener todas las reservas de un profesional
app.get('/api/service-user/:userId/bookings', (req, res) => {
  const { userId } = req.params; // ID del usuario dentro de la tabla service
  const { status } = req.query;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      res.status(500).json({ error: 'Error al obtener la conexión.' });
      return;
    }

    // Consulta para obtener la información de todas las reservas donde el servicio pertenece a un usuario específico
    let query = `
      SELECT 
        booking.id AS booking_id,
        booking.user_id AS booking_user_id,
        booking.booking_start_datetime,
        booking.booking_end_datetime,
        booking.service_duration,
        booking.final_price,
        booking.commission,
        booking.is_paid,
        booking.booking_status,
        booking.order_datetime,
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
        service.experience_years,
        service.is_hidden,
        service.service_created_datetime,
        service.last_edit_datetime,
        price.price,
        price.price_type,
        -- Información del usuario que presta el servicio
        service_user.id AS service_user_id,
        service_user.email AS service_user_email,
        service_user.phone AS service_user_phone,
        service_user.username AS service_user_username,
        service_user.first_name AS service_user_first_name,
        service_user.surname AS service_user_surname,
        service_user.profile_picture AS service_user_profile_picture,
        service_user.is_professional AS service_user_is_professional,
        service_user.language AS service_user_language,
        -- Información del usuario que realizó la reserva
        booking_user.id AS booking_user_id,
        booking_user.email AS booking_user_email,
        booking_user.phone AS booking_user_phone,
        booking_user.username AS booking_user_username,
        booking_user.first_name AS booking_user_first_name,
        booking_user.surname AS booking_user_surname,
        booking_user.profile_picture AS booking_user_profile_picture,
        booking_user.is_professional AS booking_user_is_professional,
        booking_user.language AS booking_user_language,
        -- Subconsulta para obtener las imágenes del servicio
        (SELECT JSON_ARRAYAGG(JSON_OBJECT('id', si.id, 'image_url', si.image_url, 'object_name', si.object_name, 'order', si.order))
         FROM service_image si 
         WHERE si.service_id = service.id) AS images
      FROM booking
      JOIN service ON booking.service_id = service.id
      JOIN price ON service.price_id = price.id
      JOIN user_account AS service_user ON service.user_id = service_user.id -- Usuario que presta el servicio
      JOIN user_account AS booking_user ON booking.user_id = booking_user.id -- Usuario que realizó la reserva
      WHERE service.user_id = ?`;

    const params = [userId];
    if (status) {
      query += ' AND booking.booking_status = ?';
      params.push(status);
    }
    query += ' ORDER BY booking.order_datetime DESC;';

    connection.query(query, params, (err, bookingsData) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al obtener la información de las reservas:', err);
        res.status(500).json({ error: 'Error al obtener la información de las reservas.' });
        return;
      }

      if (bookingsData.length > 0) {
        res.status(200).json(bookingsData); // Devolver la lista de reservas con la información del servicio y usuario
      } else {
        res.status(200).json({ notFound: true, message: 'No se encontraron reservas para este usuario.' });
      }
    });
  });
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
        service.is_hidden,
        service.service_created_datetime,
        service.last_edit_datetime,
        price.price,
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
app.get('/api/user/:id/wallet', authenticateToken, (req, res) => {
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
      SELECT money_in_wallet
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
        res.status(200).json({ money_in_wallet: walletData[0].money_in_wallet });
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
  const { user_id, address_type, street_number, address_1, address_2, postal_code, city, state, country } = req.body;

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
    const addressQuery = 'INSERT INTO address (address_type, street_number, address_1, address_2, postal_code, city, state, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
    const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country];

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
      SELECT d.id AS direction_id, a.id AS address_id, a.address_type, a.street_number, a.address_1, a.address_2, a.postal_code, a.city, a.state, a.country
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
  const { address_type, street_number, address_1, address_2, postal_code, city, state, country } = req.body;

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
      SET address_type = ?, street_number = ?, address_1 = ?, address_2 = ?, postal_code = ?, city = ?, state = ?, country = ?
      WHERE id = ?`;
    const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country, id];

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
app.post('/api/bookings', (req, res) => {
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
    service_id,
    booking_start_datetime,
    booking_end_datetime,
    recurrent_pattern_id,
    promotion_id,
    service_duration,
    final_price,
    commission,
    description // Nueva propiedad para la descripción
  } = req.body;

  // Verificación de campos requeridos para el usuario
  if (!user_id) {
    return res.status(400).json({ error: 'El campo user_id es requerido.' });
  }

  // Si street_number o address_2 son undefined o vacíos, se establecen como NULL
  const streetNumberValue = street_number || null;
  const address2Value = address_2 || null;

  // Variable para almacenar el address_id
  let addressId = null;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Paso 1: Verificar si se necesita insertar la dirección
    if (address_type && address_1 && postal_code && city && state && country) {
      // Insertar la dirección en `address`
      const addressQuery = 'INSERT INTO address (address_type, street_number, address_1, address_2, postal_code, city, state, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
      const addressValues = [address_type, streetNumberValue, address_1, address2Value, postal_code, city, state, country];

      connection.query(addressQuery, addressValues, (err, result) => {
        if (err) {
          connection.release();
          console.error('Error al insertar la dirección:', err);
          return res.status(500).json({ error: 'Error al insertar la dirección.' });
        }

        addressId = result.insertId; // ID de la dirección recién insertada

        // Paso 2: Insertar en la tabla `booking`
        createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, description, res);
      });
    } else {
      // Si no se necesita una dirección, se usa NULL para address_id
      createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, description, res);
    }
  });
});

// Función para crear la reserva 
function createBooking(connection, user_id, service_id, addressId, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, description, res) {
  const bookingQuery = `
    INSERT INTO booking (user_id, service_id, address_id, payment_method_id, booking_start_datetime, booking_end_datetime, recurrent_pattern_id, promotion_id, service_duration, final_price, commission, is_paid, booking_status, description, order_datetime)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_deposit', ?, NOW())
  `;
  const bookingValues = [
    user_id,
    service_id,
    addressId, // Esto puede ser null
    null, // payment_method_id se establece en NULL
    booking_start_datetime || null,
    booking_end_datetime || null,
    recurrent_pattern_id || null,
    promotion_id || null,
    service_duration || null,
    final_price || null,
    commission || null,
    false, // is_paid
    description || null // Se establece la descripción, puede ser null
  ];

  connection.query(bookingQuery, bookingValues, (err, result) => {
    if (err) {
      connection.release();
      console.error('Error al insertar la reserva:', err);
      return res.status(500).json({ error: 'Error al insertar la reserva.' });
    }

    const newBookingId = result.insertId;
    const selectQuery = 'SELECT * FROM booking WHERE id = ?';
    connection.query(selectQuery, [newBookingId], (selectErr, bookingData) => {
      connection.release();
      if (selectErr) {
        console.error('Error al obtener la reserva creada:', selectErr);
        return res.status(500).json({ error: 'Error al obtener la reserva creada.' });
      }
      if (bookingData.length === 0) {
        return res.status(500).json({ error: 'No se encontró la reserva creada.' });
      }

      res.status(201).json({ message: 'Reserva creada con éxito', booking: bookingData[0] });
    });
  });
}

// Obtener detalles de una reserva
app.get('/api/bookings/:id', (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `
      SELECT
        b.id,
        b.user_id,
        b.service_id,
        b.address_id,
        b.payment_method_id,
        b.booking_start_datetime,
        b.booking_end_datetime,
        b.recurrent_pattern_id,
        b.promotion_id,
        b.service_duration,
        b.final_price,
        b.commission,
        b.is_paid,
        b.booking_status,
        b.order_datetime,
        b.description,
        pr.price,
        pr.price_type,
        pm.provider,
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
        a.country
      FROM booking b
      LEFT JOIN service s ON b.service_id = s.id
      LEFT JOIN price pr ON s.price_id = pr.id
      LEFT JOIN payment_method pm ON b.payment_method_id = pm.id
      LEFT JOIN address a ON b.address_id = a.id
      WHERE b.id = ?
    `;

    connection.query(query, [id], (err, result) => {
      connection.release();
      if (err) {
        console.error('Error al obtener la reserva:', err);
        return res.status(500).json({ error: 'Error al obtener la reserva.' });
      }

      if (result.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }

      res.status(200).json(result[0]);
    });
  });
});

// Actualizar una reserva
app.put('/api/bookings/:id', (req, res) => {
  const { id } = req.params;
  let {
    booking_start_datetime,
    booking_end_datetime,
    service_duration,
    final_price,
    commission,
    description,
    address_id
  } = req.body;

  // Normaliza posibles valores del front
  if (Object.prototype.hasOwnProperty.call(req.body, 'address_id')) {
    if (address_id === 'null' || address_id === '') address_id = null;
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const setParts = [
      'booking_start_datetime = ?',
      'booking_end_datetime = ?',
      'service_duration = ?',
      'final_price = ?',
      'description = ?'
    ];
    const values = [
      booking_start_datetime,
      booking_end_datetime,
      service_duration,
      final_price,
      description
    ];

    if (Object.prototype.hasOwnProperty.call(req.body, 'commission')) {
      setParts.push('commission = ?');
      values.push(commission);
    }

    // Si viene address_id en el body, lo actualizamos (puede ser null)
    if (Object.prototype.hasOwnProperty.call(req.body, 'address_id')) {
      setParts.push('address_id = ?');
      values.push(address_id); // <-- puede ser null y MySQL lo guardará como NULL
    }

    const runUpdate = () => {
      const sql = `UPDATE booking SET ${setParts.join(', ')} WHERE id = ?`;
      connection.query(sql, [...values, id], (err2) => {
        connection.release();
        if (err2) {
          console.error('Error al actualizar la reserva:', err2);
          return res.status(500).json({ error: 'Error al actualizar la reserva.' });
        }
        res.status(200).json({ message: 'Reserva actualizada con éxito' });
      });
    };

    // Solo validamos contra address si address_id NO es null
    if (Object.prototype.hasOwnProperty.call(req.body, 'address_id') && address_id !== null) {
      connection.query('SELECT id FROM address WHERE id = ?', [address_id], (err3, rows) => {
        if (err3) {
          connection.release();
          console.error('Error al validar address_id:', err3);
          return res.status(500).json({ error: 'Error al validar address_id.' });
        }
        if (!rows || rows.length === 0) {
          connection.release();
          return res.status(400).json({ error: 'address_id no existe.' });
        }
        runUpdate();
      });
    } else {
      // address_id no viene o es null => no validar, solo actualizar (quedará a NULL si lo pasaste)
      runUpdate();
    }
  });
});

// Actualizar datos de una reserva
app.patch('/api/bookings/:id/update-data', (req, res) => {
  const { id } = req.params;
  const { status, is_paid, booking_end_datetime, service_duration, final_price, commission, address_id } = req.body;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const fields = [];
    const values = [];

    if (typeof status !== 'undefined') {
      fields.push('booking_status = ?');
      values.push(status);
      if (String(status).toLowerCase() === 'accepted') {
        fields.push('booking_start_datetime = IF(booking_start_datetime IS NULL, NOW(), booking_start_datetime)');
      }
    }
    if (typeof is_paid !== 'undefined') {
      fields.push('is_paid = ?');
      values.push(is_paid);
    }

    // Campos extra de la reserva (fin, duración, precio final, comisión)
    if (typeof booking_end_datetime !== 'undefined') {
      fields.push('booking_end_datetime = ?');
      values.push(booking_end_datetime);
    }
    if (typeof service_duration !== 'undefined') {
      fields.push('service_duration = ?');
      values.push(service_duration);
    }
    if (typeof final_price !== 'undefined') {
      fields.push('final_price = ?');
      values.push(final_price);
    }
    if (typeof commission !== 'undefined') {
      fields.push('commission = ?');
      values.push(commission);
    }
    if (typeof address_id !== 'undefined') {
      fields.push('address_id = ?');
      values.push(address_id);
    }

    // Autocompletar fin  cuando se marque como completada
    if (typeof status !== 'undefined' && String(status).toLowerCase() === 'completed') {
      if (typeof booking_end_datetime === 'undefined') {
        fields.push('booking_end_datetime = IFNULL(booking_end_datetime, NOW())');
      }
    }

    if (fields.length === 0) {
      connection.release();
      return res.status(400).json({ error: 'No fields to update.' });
    }

    const proceedUpdate = () => {
      const query = `UPDATE booking SET ${fields.join(', ')} WHERE id = ?`;
      values.push(id);
      connection.query(query, values, (err, result) => {
        connection.release();
        if (err) {
          console.error('Error al actualizar el estado de la reserva:', err);
          return res.status(500).json({ error: 'Error al actualizar la reserva.' });
        }
        res.status(200).json({ message: 'Estado actualizado' });
      });
    };

    if (typeof address_id !== 'undefined') {
      connection.query('SELECT id FROM address WHERE id = ?', [address_id], (err, rows) => {
        if (err) {
          connection.release();
          console.error('Error al validar address_id:', err);
          return res.status(500).json({ error: 'Error al validar address_id.' });
        }
        if (!rows || rows.length === 0) {
          connection.release();
          return res.status(400).json({ error: 'address_id no existe.' });
        }
        proceedUpdate();
      });
    } else {
      proceedUpdate();
    }
  });
});

// Actualizar el pago de una reserva
app.patch('/api/bookings/:id/is_paid', (req, res) => {
  const { id } = req.params;
  const { is_paid } = req.body;

  if (typeof is_paid === 'undefined') {
    return res.status(400).json({ error: 'is_paid es requerido.' });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'UPDATE booking SET is_paid = ? WHERE id = ?';

    connection.query(query, [is_paid, id], (err, result) => {
      connection.release();
      if (err) {
        console.error('Error al actualizar el pago de la reserva:', err);
        return res.status(500).json({ error: 'Error al actualizar la reserva.' });
      }
      res.status(200).json({ message: 'Pago actualizado' });
    });
  });
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
      'SELECT user_id, booking_status FROM booking WHERE id = ? FOR UPDATE', [id]
    );
    if (!b) { await conn.rollback(); return res.status(404).json({ error: 'Reserva no encontrada' }); }
    const isOwner = req.user && Number(req.user.id) === Number(b.user_id);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isOwner && !isStaff) { await conn.rollback(); return res.status(403).json({ error: 'No autorizado' }); }
    const [[pdep]] = await conn.query(
      "SELECT status FROM payments WHERE booking_id = ? AND type='deposit' LIMIT 1", [id]
    );
    const succeeded = pdep && pdep.status === 'succeeded';
    if (!succeeded && b.booking_status === 'pending_deposit') {
      await conn.query("UPDATE booking SET booking_status='payment_failed' WHERE id=?", [id]);
    }
    await conn.commit();
    res.json({ ok: true });
  } catch (e) {
    try { await conn.rollback(); } catch { }
    res.status(500).json({ error: 'No se pudo cancelar la reserva impagada' });
  } finally { conn.release(); }
});

// Cobra la comisión 10% (mín 1€)
app.post('/api/bookings/:id/deposit', authenticateToken, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Id inválido' });
  const { payment_method_id } = req.body;

  console.log('Iniciando proceso de depósito:', {
    bookingId: id,
    userId: req.user?.id,
    userRole: req.user?.role,
    timestamp: new Date().toISOString(),
    paymentMethodId: payment_method_id || null
  });

  let booking;
  let payment;
  let commissionCents;

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();

    const [rows] = await connection.query(
      `
      SELECT b.id, b.user_id, b.final_price, b.commission, b.booking_status,
             b.service_duration, b.booking_start_datetime, b.booking_end_datetime,
             s.id AS service_id,
             p.price AS unit_price, p.price_type,
             u.email AS customer_email, u.stripe_customer_id
      FROM booking b
      JOIN service s ON b.service_id = s.id
      JOIN price   p ON s.price_id = p.id
      JOIN user_account u ON b.user_id = u.id
      WHERE b.id = ? FOR UPDATE
      `,
      [id]
    );
    booking = rows[0];
    if (!booking) throw new Error('Reserva no encontrada');
    if (booking.booking_status === 'cancelled') throw new Error('Reserva cancelada');

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

    // Pricing server-side
    const priceType = String(booking.price_type || '').toLowerCase();
    const unit = Number(booking.unit_price || 0);
    const hours = effectiveMinutes / 60;
    let base = 0;
    if (priceType === 'hour') base = unit * hours;
    else if (priceType === 'fix') base = unit;
    base = round2(base);

    const commissionEuros = (priceType === 'budget')
      ? 1
      : Math.max(1, round1(base * 0.1));

    const finalEuros =
      (priceType === 'budget' || (priceType === 'hour' && effectiveMinutes <= 0))
        ? null
        : round2(base + commissionEuros);

    commissionCents = Math.max(100, toCents(commissionEuros || 0));
    const finalCentsSnapshot = toCents(finalEuros || 0);

    // Garantiza Customer y persiste si no existe
    const customerId = await ensureStripeCustomerId(connection, {
      userId: booking.user_id,
      email: booking.customer_email,
    });

    // Congelar importes creando fila de pago (para poder atar idempotencia a payment.id)
    await upsertPaymentRow(connection, {
      bookingId: id,
      type: 'deposit',
      amountCents: commissionCents,
      commissionSnapshotCents: commissionCents,
      finalPriceSnapshotCents: finalCentsSnapshot,
      status: 'creating',
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

    try {
      if (payment.payment_intent_id) {
        // Recuperar intent existente
        intent = await stripe.paymentIntents.retrieve(payment.payment_intent_id, {
          expand: ['payment_method', 'latest_charge.payment_method_details'],
        });

        // Si falta método de pago y el cliente nos envía uno ahora, adjuntarlo y confirmar en servidor
        if (intent.status === 'requires_payment_method' && payment_method_id) {
          let pm = await stripe.paymentMethods.retrieve(payment_method_id);
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
          await stripe.paymentIntents.update(intent.id, { payment_method: payment_method_id, customer: customerId });
          intent = await stripe.paymentIntents.confirm(intent.id, { off_session: true });
        }
      } else {
        // Crear intent nuevo: si traen PM, confirmar server-side; si no, devolver client_secret para confirmar en el cliente
        if (payment_method_id) {
          intent = await stripe.paymentIntents.create(
            {
              amount: commissionCents,
              currency: 'eur',
              customer: customerId,
              payment_method: payment_method_id,
              confirm: true,
              off_session: true,
              receipt_email: booking.customer_email || undefined,
              automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
              transfer_group: transferGroup,
              setup_future_usage: 'off_session',
              metadata: { booking_id: String(id), type: 'deposit' },
            },
            { idempotencyKey: idemKey }
          );
        } else {
          intent = await stripe.paymentIntents.create(
            {
              amount: commissionCents,
              currency: 'eur',
              customer: customerId,
              setup_future_usage: 'off_session',
              receipt_email: booking.customer_email || undefined,
              automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
              transfer_group: transferGroup,
              metadata: { booking_id: String(id), type: 'deposit' },
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
        const last4Err =
          pi?.payment_method?.card?.last4 ||
          pi?.last_payment_error?.payment_method?.card?.last4 ||
          null;
        const lastErrorCode = pi?.last_payment_error?.code || pi?.last_payment_error?.decline_code || null;
        const lastErrorMessage = pi?.last_payment_error?.message || null;

        const connErr = await pool.promise().getConnection();
        try {
          await connErr.beginTransaction();
          await upsertPayment(connErr, {
            bookingId: id,
            type: 'deposit',
            paymentIntentId: pi.id,
            amountCents: commissionCents,
            commissionSnapshotCents: commissionCents,
            finalPriceSnapshotCents: finalCentsSnapshot,
            status: mapStatus(pi.status),
            transferGroup,
            paymentMethodId: payment_method_id || pi?.payment_method || null,
            paymentMethodLast4: last4Err || null,
            lastErrorCode,
            lastErrorMessage,
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

    // Persistir intent/estado + transfer_group + PM last4 si disponible
    const pmIdPersist =
      (intent?.payment_method && typeof intent.payment_method === 'object' ? intent.payment_method.id : intent?.payment_method) ??
      intent?.latest_charge?.payment_method ??
      payment_method_id ??
      null;

    // last4 con fallback al  PM que hayas recuperado/adjuntado previamente (pm)
    const last4Persist =
      intent?.payment_method?.card?.last4 ||
      intent?.latest_charge?.payment_method_details?.card?.last4 ||
      (typeof pm !== 'undefined' ? pm?.card?.last4 : null) ||
      null;

    const lastErrObj = intent?.last_payment_error || intent?.latest_charge?.last_payment_error || null;
    const lastErrorCode = lastErrObj?.code || lastErrObj?.decline_code || null;
    const lastErrorMessage = lastErrObj?.message || null;

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
        transferGroup,
        paymentMethodId: pmIdPersist,
        paymentMethodLast4: last4Persist,
        lastErrorCode: lastErrorCode,
        lastErrorMessage: lastErrorMessage,
      });
      if (intent.status === 'succeeded') {
        await conn2.query(
          'UPDATE booking SET booking_status = "requested" WHERE id = ?',
          [id]
        );
      } else if (intent.status === 'canceled') {
        await conn2.query(
          'UPDATE booking SET booking_status = "payment_failed" WHERE id = ? AND booking_status = "pending_deposit"',
          [id]
        );
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

// Cargo final (destination charge) - FALTA COMISION EXTRA DE WISDOM SI VARIA PRECIO FINAL 
app.post('/api/bookings/:id/final-payment-transfer', authenticateToken, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  const { payment_method_id } = req.body;
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Id inválido' });

  console.log('Iniciando proceso de pago final:', {
    bookingId: id,
    userId: req.user?.id,
    userRole: req.user?.role,
    timestamp: new Date().toISOString(),
    paymentMethodId: payment_method_id || null,
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

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();

    // Bloquear la reserva y obtener datos necesarios
    const [rows] = await connection.query(
      `
      SELECT b.id, b.user_id, b.final_price, b.commission, b.is_paid,
             b.service_duration, b.booking_start_datetime, b.booking_end_datetime,
             cust.email AS customer_email, cust.stripe_customer_id AS customer_id,
             provider.stripe_account_id,
             p.price AS unit_price, p.price_type
      FROM booking b
      JOIN service s ON b.service_id = s.id
      JOIN price p ON s.price_id = p.id
      JOIN user_account provider ON s.user_id = provider.id
      JOIN user_account cust ON b.user_id = cust.id
      WHERE b.id = ? FOR UPDATE
      `,
      [id]
    );
    booking = rows[0];
    if (!booking) throw new Error('Reserva no encontrada');
    if (booking.is_paid) throw new Error('Reserva ya pagada');

    const isOwner = req.user && Number(req.user.id) === Number(booking.user_id);
    const isStaff = req.user && ['admin', 'support'].includes(req.user.role);
    if (!isOwner && !isStaff) {
      await connection.rollback();
      return res.status(403).json({ error: 'No autorizado' });
    }
    if (!booking.stripe_account_id || !booking.stripe_account_id.startsWith('acct_')) {
      throw new Error('Cuenta Stripe inválida');
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

    // Verificar depósito succeeded
    const [dep] = await connection.query(
      `SELECT id FROM payments WHERE booking_id = ? AND type = 'deposit' AND status = 'succeeded' LIMIT 1 FOR UPDATE`,
      [id]
    );
    if (dep.length === 0) {
      await connection.rollback();
      return res.status(412).json({ error: 'Depósito no confirmado (requerido).' });
    }

    // Comprobar si ya hay cobro final existente en curso o realizado y devolver la info útil en vez de 409
    const [existingFinal] = await connection.query(
      `SELECT id, status, payment_intent_id
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
            `SELECT id, amount_cents, commission_snapshot_cents, final_price_snapshot_cents, transfer_group
             FROM payments
             WHERE id = ? AND booking_id = ? AND type = 'final'
             LIMIT 1`,
            [row.id, id]
          );
          const paySnap = paySnapRows && paySnapRows[0];
          const amountEnsure = paySnap?.amount_cents || 0;
          const transferGroupEnsure = paySnap?.transfer_group || `booking-${id}`;

          if (amountEnsure > 0) {
            const pmIdBody = req.body?.payment_method_id || null;
            const idemEnsureParts = ['payment', String(row.id), pmIdBody ? String(pmIdBody) : 'ensure'];
            const idemEnsure = stableKey(idemEnsureParts);

            if (pmIdBody) {
              // Adjuntar PM si es necesario
              let pm2 = await stripe.paymentMethods.retrieve(pmIdBody);
              if (pm2.customer && pm2.customer !== customerId) {
                return res.status(409).json({ error: 'payment_method_id pertenece a otro customer.' });
              }
              if (!pm2.customer) {
                try {
                  pm2 = await stripe.paymentMethods.attach(pmIdBody, { customer: customerId });
                } catch (eAttach) {
                  console.error('No se pudo adjuntar el PM al customer (ensure):', eAttach);
                  return res.status(400).json({ error: 'No se pudo adjuntar el método de pago al cliente.' });
                }
              }

              intent = await stripe.paymentIntents.create(
                {
                  amount: amountEnsure,
                  currency: 'eur',
                  customer: customerId,
                  payment_method: pmIdBody,
                  confirm: true,
                  off_session: true,
                  receipt_email: booking.customer_email || undefined,
                  automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
                  transfer_data: { destination: booking.stripe_account_id },
                  on_behalf_of: booking.stripe_account_id,
                  transfer_group: transferGroupEnsure,
                  metadata: { booking_id: String(id), type: 'final' },
                },
                { idempotencyKey: idemEnsure }
              );
            } else {
              // Crear intent sin PM para devolver clientSecret y confirmar en el cliente
              intent = await stripe.paymentIntents.create(
                {
                  amount: amountEnsure,
                  currency: 'eur',
                  customer: customerId,
                  receipt_email: booking.customer_email || undefined,
                  automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
                  transfer_data: { destination: booking.stripe_account_id },
                  on_behalf_of: booking.stripe_account_id,
                  transfer_group: transferGroupEnsure,
                  metadata: { booking_id: String(id), type: 'final' },
                },
                { idempotencyKey: idemEnsure }
              );
            }

            // Guardar el intent recién creado
            const last4Ensure =
              intent?.payment_method?.card?.last4 ||
              intent?.latest_charge?.payment_method_details?.card?.last4 ||
              null;
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
                transferGroup: transferGroupEnsure,
                paymentMethodId: req.body?.payment_method_id || null,
                paymentMethodLast4: last4Ensure || null,
              });
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
        if (intent && status === 'requires_payment_method' && req.body?.payment_method_id) {
          const pmId = req.body.payment_method_id;
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

            await stripe.paymentIntents.update(intent.id, { payment_method: pmId, customer: customerId });
            intent = await stripe.paymentIntents.confirm(intent.id, { off_session: true });

            const last4 =
              intent?.payment_method?.card?.last4 ||
              intent?.latest_charge?.payment_method_details?.card?.last4 ||
              null;

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
                transferGroup: intent.transfer_group || null,
                paymentMethodId: pmId,
                paymentMethodLast4: last4 || null,
              });
              await conn3.commit();
            } catch (e2) {
              try { await conn3.rollback(); } catch { }
              console.error('No se pudo actualizar payment_method en BD para intent existente:', e2);
            } finally {
              conn3.release();
            }

            if (intent.status === 'succeeded') {
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
          return res.status(202).json({ requiresPaymentMethod: true, clientSecret: intent.client_secret, paymentIntentId: intent.id });
        }

        if (status === 'succeeded') {
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

    // Recalcular pricing como en BookingScreen
    // Determinar minutos: usar service_duration si existe; si no, derivar de start/end
    const storedDurationMin = Number.isFinite(Number(booking.service_duration)) ? Number(booking.service_duration) : null;
    let derivedMinutes = null;
    try {
      if (!storedDurationMin && booking.booking_start_datetime && booking.booking_end_datetime) {
        const t0 = new Date(booking.booking_start_datetime);
        const t1 = new Date(booking.booking_end_datetime);
        if (!Number.isNaN(t0.getTime()) && !Number.isNaN(t1.getTime())) {
          const diffMs = Math.max(0, t1.getTime() - t0.getTime());
          derivedMinutes = Math.round(diffMs / 60000);
        }
      }
    } catch (_) { /* noop safe derive */ }

    const effectiveMinutes = storedDurationMin ?? derivedMinutes ?? 0;
    const pricing = computePricing({ priceType: booking.price_type, unitPrice: booking.unit_price, durationMinutes: effectiveMinutes });

    // Convertir a céntimos para comparar en entero
    finalCents = toCents(booking.final_price || 0);
    commissionCents = Math.max(100, toCents(booking.commission || 0));
    finalCalcCents = toCents(pricing.final || 0);
    commissionCalcCents = Math.max(100, toCents(pricing.commission || 0));

    // Elegir final y comisión a usar
    const commissionMatches = commissionCents === commissionCalcCents;
    const isBudget = String(booking.price_type || '').toLowerCase() === 'budget';
    if (isBudget) {
      // Para budget exclusivamente: el final_price de DB es el precio base y calcular comisión sobre esa base (10% mín 1€)
      finalChosenCents = finalCents;
      const commissionFromFinalEuros = Math.max(1, round1((finalCents / 100) * 0.1));
      commissionChosenCents = toCents(commissionFromFinalEuros);
    } else {
      // Resto: mantener final de DB y elegir comisión recalculada si difiere
      finalChosenCents = finalCents;
      commissionChosenCents = commissionMatches ? commissionCents : commissionCalcCents;
    }

    // Validar precondición con los elegidos
    if (!(commissionChosenCents > 0)) {
      await connection.rollback();
      return res.status(412).json({ error: 'Precondición: commission > 0 no cumplida.' });
    }

    //!ESTO EN UN FUTURO SE DEBE CAMBIAR POR UNA COMISION EXTRA DE WISDOM O DEBULUCION DE PARTE DE LA COMISION (amountToCharge incluira diferencia entre new comision y old comision)
    amountToCharge = isBudget ? finalChosenCents : (finalChosenCents - commissionChosenCents);

    if (amountToCharge <= 0) {
      await connection.rollback();
      return res.status(400).json({ error: 'Importe inválido para el pago final.' });
    }

    // Congelar snapshots en la fila del pago final (para idempotencia por payment.id)
    await upsertPaymentRow(connection, {
      bookingId: id,
      type: 'final',
      amountCents: amountToCharge,
      commissionSnapshotCents: commissionChosenCents,
      finalPriceSnapshotCents: finalChosenCents,
      status: 'creating',
    });
    payment = await getPaymentRow(connection, id, 'final');

    await connection.commit();

    // Validar capacidades de la conectada antes de crear el Intent
    const acct = await stripe.accounts.retrieve(booking.stripe_account_id);
    const canCard = acct.capabilities?.card_payments === 'active';
    const canTransfers = acct.capabilities?.transfers === 'active';
    const canPayouts = !!acct.payouts_enabled;
    const chargesEnabled = !!acct.charges_enabled;
    if (!chargesEnabled || !canCard || !canTransfers || !canPayouts) {
      return res.status(412).json({ error: 'La cuenta conectada no está lista para cobrar y transferir.' });
    }

    // Determinar PM a usar
    const pmToUse = payment_method_id || null;
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
            currency: 'eur',
            customer: customerId,
            payment_method: pmToUse,
            confirm: true,
            off_session: true,
            receipt_email: booking.customer_email || undefined,
            automatic_payment_methods: { enabled: true, allow_redirects: 'never' },
            transfer_data: { destination: booking.stripe_account_id },
            on_behalf_of: booking.stripe_account_id,
            transfer_group: transferGroup,
            metadata: { booking_id: String(id), type: 'final' },
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
        const last4Err =
          pi?.payment_method?.card?.last4 ||
          pi?.last_payment_error?.payment_method?.card?.last4 ||
          null;
        const lastErrorCode = pi?.last_payment_error?.code || pi?.last_payment_error?.decline_code || null;
        const lastErrorMessage = pi?.last_payment_error?.message || null;

        const connErr = await pool.promise().getConnection();
        try {
          await connErr.beginTransaction();
          await upsertPayment(connErr, {
            bookingId: id,
            type: 'final',
            paymentIntentId: pi.id,
            amountCents: amountToCharge,
            commissionSnapshotCents: commissionChosenCents,
            finalPriceSnapshotCents: finalChosenCents,
            status: mapStatus(pi.status),
            transferGroup,
            paymentMethodId: pmToUse,
            paymentMethodLast4: last4Err || null,
            lastErrorCode: lastErrorCode,
            lastErrorMessage: lastErrorMessage,
          });
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

    const last4Persist =
      intent?.payment_method?.card?.last4 ||
      intent?.latest_charge?.payment_method_details?.card?.last4 ||
      pm?.card?.last4 ||
      null;
    const lastErrObj = intent?.last_payment_error || intent?.latest_charge?.last_payment_error || null;
    const lastErrorCode = lastErrObj?.code || lastErrObj?.decline_code || null;
    const lastErrorMessage = lastErrObj?.message || null;

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
        transferGroup,
        paymentMethodId: pmToUse,
        paymentMethodLast4: last4Persist,
        lastErrorCode: lastErrorCode,
        lastErrorMessage: lastErrorMessage,
      });
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
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = 'SELECT final_price, commission FROM booking WHERE id = ?';
    connection.query(query, [id], async (err, results) => {
      connection.release();
      if (err) {
        console.error('Error al obtener la reserva:', err);
        return res.status(500).json({ error: 'Error al obtener la reserva.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }

      const finalPrice = parseFloat(results[0].final_price || 0);
      const commission = parseFloat(results[0].commission || 0);
      const amountToPay = Number((finalPrice - commission).toFixed(2));
      if (amountToPay <= 0) {
        return res.status(400).json({ error: 'El importe final es cero o negativo.' });
      }

      try {
        const intent = await stripe.paymentIntents.create({
          amount: Math.round(amountToPay * 100),
          currency: 'eur',
          metadata: { booking_id: id, type: 'final' }
        });
        res.status(200).json({ clientSecret: intent.client_secret });
      } catch (stripeErr) {
        console.error('Error al crear el pago final:', stripeErr);
        res.status(500).json({ error: 'Error al procesar el pago final.' });
      }
    });
  });
});

// Transferir el pago final al profesional con Stripe Connect (NO ACTIVO!)
app.post('/api/bookings/:id/transfer', authenticateToken, (req, res) => {
  const { id } = req.params;

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const query = `SELECT b.final_price, b.commission, s.user_id, u.stripe_account_id
                   FROM booking b
                   JOIN service s ON b.service_id = s.id
                   JOIN user_account u ON s.user_id = u.id
                   WHERE b.id = ?`;

    connection.query(query, [id], async (qErr, results) => {
      connection.release();
      if (qErr) {
        console.error('Error al obtener la reserva:', qErr);
        return res.status(500).json({ error: 'Error al obtener la reserva.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Reserva no encontrada.' });
      }

      const { final_price, commission, stripe_account_id } = results[0];

      if (!stripe_account_id) {
        return res.status(400).json({ error: 'El profesional no tiene cuenta Stripe.' });
      }

      const finalPrice = parseFloat(results[0].final_price || 0);
      const commissionAmount = parseFloat(results[0].commission || 0);
      const amount = Number((finalPrice - commissionAmount).toFixed(2));
      if (amount <= 0) {
        return res.status(400).json({ error: 'El importe a transferir es cero o negativo.' });
      }

      try {
        await stripe.transfers.create({
          amount: Math.round(amount * 100),
          currency: 'eur',
          destination: stripe_account_id,
          metadata: { booking_id: id }
        });
        res.status(200).json({ message: 'Transferencia realizada con éxito' });
      } catch (stripeErr) {
        console.error('Error al realizar la transferencia:', stripeErr);
        res.status(500).json({ error: 'Error al realizar la transferencia.' });
      }
    });
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
        b.final_price,
        b.commission,
        b.is_paid,
        b.booking_start_datetime,
        b.booking_end_datetime,
        b.description AS booking_description,
        s.service_title,
        s.description AS service_description,
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
      JOIN user_account cu ON b.user_id = cu.id
      JOIN service s ON b.service_id = s.id
      JOIN user_account sp ON s.user_id = sp.id
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

    connection.query(query, [id], (err, results) => {
      connection.release();
      if (err) {
        console.error('Error fetching booking:', err);
        return res.status(500).json({ error: 'Error fetching booking.' });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: 'Booking not found.' });
      }

      const data = results[0];
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
      const toCurrency = (amount) => `€${Number(amount || 0).toFixed(2)}`;

      // Decide invoice type
      const typeParam = String(req.query.type || '').toLowerCase();
      const invoiceType = (typeParam === 'deposit' || typeParam === 'final')
        ? typeParam
        : (data.is_paid ? 'final' : 'deposit');

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
        invoiceTotal = Number(data.commission || 0);
        vatRate = 21;
        taxableBase = Number((invoiceTotal / (1 + vatRate / 100)).toFixed(2));
        vatAmount = Number((invoiceTotal - taxableBase).toFixed(2));
      } else {
        invoiceTotal = Number((Number(data.final_price || 0) - Number(data.commission || 0)).toFixed(2));
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
app.delete('/api/delete_booking/:id', (req, res) => {
  const { id } = req.params; // ID de la reserva a eliminar

  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    const deleteQuery = 'DELETE FROM booking WHERE id = ?';

    connection.query(deleteQuery, [id], (err, result) => {
      connection.release(); // Liberar la conexión después de usarla

      if (err) {
        console.error('Error al eliminar la reserva:', err);
        return res.status(500).json({ error: 'Error al eliminar la reserva.' });
      }

      if (result.affectedRows > 0) {
        res.status(200).json({ message: 'Reserva eliminada con éxito' });
      } else {
        res.status(404).json({ message: 'Reserva no encontrada' });
      }
    });
  });
});

//Ruta para obtener las sugerencias de busqueda de servicios
app.get('/api/suggestions', (req, res) => {
  const { query } = req.query;

  // Validar que se reciba el término de búsqueda
  if (!query || typeof query !== 'string' || query.trim() === '') {
    return res.status(400).json({ error: 'La consulta de búsqueda es requerida.' });
  }

  // Conectar a la base de datos
  pool.getConnection((err, connection) => {
    if (err) {
      console.error('Error al obtener la conexión:', err);
      return res.status(500).json({ error: 'Error al obtener la conexión.' });
    }

    // Definir el patrón de búsqueda
    const searchPattern = `%${query}%`;

    // Consulta para obtener sugerencias de búsqueda, eliminando duplicados
    const searchQuery = `
      SELECT
        s.service_title,
        ct.category_key AS service_category_name,
        f.family_key AS service_family,
        t.tag
      FROM service s 
      LEFT JOIN service_category c ON s.service_category_id = c.id 
      LEFT JOIN service_family f ON c.service_family_id = f.id 
      LEFT JOIN service_category_type ct ON c.service_category_type_id = ct.id 
      LEFT JOIN service_tags t ON s.id = t.service_id 
      WHERE
        s.is_hidden = 0
        AND (
          s.service_title LIKE ?
          OR ct.category_key LIKE ?
          OR f.family_key LIKE ?
          OR t.tag LIKE ?
        )
      LIMIT 8
    `;

    // Ejecutar la consulta
    connection.query(searchQuery,
      [searchPattern, searchPattern, searchPattern, searchPattern],
      (err, results) => {
        connection.release(); // Liberar la conexión después de usarla

        if (err) {
          console.error('Error al obtener las sugerencias:', err);
          return res.status(500).json({ error: 'Error al obtener las sugerencias.' });
        }

        // Crear un array para almacenar las sugerencias únicas
        const suggestions = [];
        const uniqueKeys = new Set(); // Usamos un Set para asegurarnos de que no haya duplicados

        results.forEach(result => {
          // Agregar un solo valor por cada tipo de sugerencia si aún no ha sido agregado
          // y asegurarse de que contenga la palabra de búsqueda
          if (result.service_title && !uniqueKeys.has(result.service_title) && result.service_title.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ service_title: result.service_title });
            uniqueKeys.add(result.service_title);
          }
          if (result.service_category_name && !uniqueKeys.has(result.service_category_name) && result.service_category_name.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ service_category_name: result.service_category_name });
            uniqueKeys.add(result.service_category_name);
          }
          if (result.service_family && !uniqueKeys.has(result.service_family) && result.service_family.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ service_family: result.service_family });
            uniqueKeys.add(result.service_family);
          }
          if (result.tag && !uniqueKeys.has(result.tag) && result.tag.toLowerCase().includes(query.toLowerCase())) {
            suggestions.push({ tag: result.tag });
            uniqueKeys.add(result.tag);
          }
        });

        // Verificar si se encontraron resultados
        if (suggestions.length === 0) {
          return res.status(200).json({ message: 'No se encontraron sugerencias.', notFound: true });
        }

        // Devolver las sugerencias encontradas
        res.status(200).json({ suggestions });
      }
    );
  });
});

app.get('/api/services/filter-categories', async (req, res) => {
  const searchTerm = (req.query.query || '').trim();
  const hasSearchTerm = searchTerm.length > 0;
  const searchPattern = hasSearchTerm ? `%${searchTerm}%` : '%';
  const categoryId = parseQueryNumber(req.query.category_id ?? req.query.categoryId ?? req.query.category);
  const limitRaw = parseQueryNumber(req.query.limit);
  const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 12) : 8;

  try {
    const filters = extractServiceFilters(req.query);
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
    const filtersClause = buildServiceFilterClause(recommendationFilters, { distanceExpression });

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
    const familyFiltersClause = buildServiceFilterClause(familyFilters, { distanceExpression });
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
      const broaderFiltersClause = buildServiceFilterClause(broaderFilters, { distanceExpression });
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
  const searchTerm = (req.query.query || '').trim();
  const hasSearchTerm = searchTerm.length > 0;
  const searchPattern = hasSearchTerm ? `%${searchTerm}%` : '%';
  const viewerId = Number(req.query.viewer_id ?? req.query.user_id ?? null);
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
  const filtersClause = buildServiceFilterClause(filters, { distanceExpression });
  const includeDistanceColumn = Boolean(distanceExpression && distanceExpression.sql);
  const distanceSelect = includeDistanceColumn
    ? `,
        ${distanceExpression.sql} AS distance_km`
    : '';
  const distanceSelectParams = includeDistanceColumn ? [...distanceExpression.params] : [];
  const categoryClause = Number.isFinite(categoryId) ? 'AND service.service_category_id = ?' : '';
  const comparablePriceExpression = buildComparablePriceExpression({ priceAlias: 'price', durationMinutes: filters.durationMinutes });
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
          SUM(CASE WHEN LOWER(booking_status) IN ('accepted', 'confirmed', 'completed') THEN 1 ELSE 0 END) AS confirmed_booking_count
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
            user_id,
            COUNT(*) AS completed_count
          FROM booking
          WHERE LOWER(booking_status) = 'completed'
          GROUP BY service_id, user_id
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
  const searchTerm = (req.query.query || '').trim();
  const hasSearchTerm = searchTerm.length > 0;
  const searchPattern = hasSearchTerm ? `%${searchTerm}%` : '%';
  const categoryId = parseQueryNumber(req.query.category_id ?? req.query.categoryId ?? req.query.category);
  const filters = extractServiceFilters(req.query);
  const distanceExpression = buildDistanceExpression({
    latColumn: 'service.latitude',
    lngColumn: 'service.longitude',
    originLat: filters.originLat,
    originLng: filters.originLng,
  });
  const filtersClause = buildServiceFilterClause(filters, { distanceExpression });
  const categoryClause = Number.isFinite(categoryId) ? 'AND service.service_category_id = ?' : '';

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
        const pi = event.data.object;
        const bookingId = pi?.metadata?.booking_id;
        const type = pi?.metadata?.type;
        if (!bookingId || !type) break;

        const last4 =
          pi?.payment_method?.card?.last4 ||
          pi?.latest_charge?.payment_method_details?.card?.last4 ||
          null;
        const pmId = pi?.payment_method || pi?.latest_charge?.payment_method || null;

        const amountCents = typeof pi.amount_received === 'number'
          ? pi.amount_received
          : (typeof pi.amount === 'number' ? pi.amount : 0);
        let depositNotification = null;

        await connection.beginTransaction();
        await upsertPayment(connection, {
          bookingId,
          type,
          paymentIntentId: pi.id,
          amountCents,
          status: mapStatus(pi.status),
          transferGroup: pi.transfer_group || null,
          paymentMethodId: pmId || null,
          paymentMethodLast4: last4 || null,
        });

        if (event.type === 'payment_intent.succeeded' && type === 'deposit') {
          const [[bookingRow]] = await connection.query(
            `SELECT
               b.id,
               b.booking_start_datetime,
               b.booking_end_datetime,
               b.final_price,
               s.service_title,
               client.first_name AS client_first_name,
               client.surname AS client_surname,
               client.username AS client_username,
               client.email AS client_email,
               prof.first_name AS professional_first_name,
               prof.surname AS professional_surname,
               prof.username AS professional_username,
               prof.email AS professional_email
             FROM booking b
             LEFT JOIN user_account client ON client.id = b.user_id
             LEFT JOIN service s ON s.id = b.service_id
             LEFT JOIN user_account prof ON prof.id = s.user_id
             WHERE b.id = ?
             LIMIT 1`,
            [bookingId]
          );
          const [updateResult] = await connection.query(
            'UPDATE booking SET booking_status = "requested" WHERE id = ? AND booking_status <> "requested"',
            [bookingId]
          );
          if (bookingRow && updateResult.affectedRows > 0) {
            depositNotification = {
              booking: {
                id: bookingRow.id,
                serviceTitle: bookingRow.service_title,
                start: bookingRow.booking_start_datetime,
                end: bookingRow.booking_end_datetime,
                finalPrice: bookingRow.final_price,
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
              depositAmountCents: amountCents,
            };
          }
        }
        if (event.type === 'payment_intent.succeeded' && type === 'final') {
          await connection.query('UPDATE booking SET is_paid = 1 WHERE id = ?', [bookingId]);
        }
        if ((event.type === 'payment_intent.payment_failed' || event.type === 'payment_intent.canceled') && type === 'deposit') {
          await connection.query(
            'UPDATE booking SET booking_status = "payment_failed" WHERE id = ? AND booking_status = "pending_deposit"',
            [bookingId]
          );
        }
        if (event.type === 'payment_intent.canceled' && type === 'final') {
          // opcional: revertir is_paid si fuera necesario en tu dominio
        }

        await connection.commit();

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
            ['refunded', piId]
          );
          if (payment.type === 'final' && fullyRefunded) {
            await connection.query('UPDATE booking SET is_paid = 0 WHERE id = ?', [payment.booking_id]);
          }
        }
        await connection.commit();
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
