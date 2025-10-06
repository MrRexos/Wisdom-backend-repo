const { getFirestore } = require('./firestore');

const RESPONSE_WINDOW_DAYS = 180;
const HALF_LIFE_DAYS = 90;
const TRIM_PERCENT = 0.05;
const MIN_PAIRS = Math.max(
  Number.parseInt(process.env.SERVICE_RESPONSE_TIME_MIN_PAIRS || '3', 10) || 3,
  1,
);
const C_MAX_MINUTES = 7 * 24 * 60;

const DEFAULT_CONVERSATION_COLLECTIONS = (process.env.FIRESTORE_CONVERSATION_COLLECTIONS || 'conversations,chats,serviceChats')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);

const DEFAULT_MESSAGE_COLLECTIONS = (process.env.FIRESTORE_MESSAGE_COLLECTIONS || 'messages,chatMessages')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);

const DEFAULT_SERVICE_FIELD_NAMES = (process.env.FIRESTORE_SERVICE_FIELD_NAMES || 'serviceId,service_id,serviceID,service')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);

const DEFAULT_PARTICIPANT_FIELD_NAMES =
  (process.env.FIRESTORE_PARTICIPANT_FIELD_NAMES || 'participants')
    .split(',').map(s => s.trim()).filter(Boolean);

async function fetchConversationsByProfessional(db, collectionNames, participantFields, proValues, debug) {
  const out = new Map();
  for (const col of collectionNames) {
    for (const field of participantFields) {
      for (const val of proValues) {
        // prueba número y string porque tu array guarda números
        const needles = [val, Number(val), String(val)].filter(v => v !== null && v !== undefined);
        for (const needle of needles) {
          try {
            const snap = await db.collection(col).where(field, 'array-contains', needle).get();
            snap.forEach(doc => {
              const key = doc.ref.path;
              if (!out.has(key)) out.set(key, { id: doc.id, ref: doc.ref, data: doc.data() || {} });
            });
          } catch (error) {
            recordDebugStep(debug, 'fetch_conversations_by_professional_failed', { col, field, needle, error: error.message });
          }
        }
      }
    }
  }
  const arr = Array.from(out.values());
  recordDebugStep(debug, 'fetch_conversations_by_professional_done', { count: arr.length });
  return arr;
}

function normalizeIdentifier(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === 'object') {
    if (value.id !== undefined) return normalizeIdentifier(value.id);
    if (value.uid !== undefined) return normalizeIdentifier(value.uid);
    if (value.userId !== undefined) return normalizeIdentifier(value.userId);
    if (value.user_id !== undefined) return normalizeIdentifier(value.user_id);
  }
  return null;
}

function normalizeFirestoreDate(value) {
  if (!value) return null;
  if (value.toDate && typeof value.toDate === 'function') {
    const date = value.toDate();
    return Number.isNaN(date?.getTime()) ? null : date;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : new Date(value.getTime());
  }
  if (typeof value === 'string') {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
  }
  if (typeof value === 'object' && typeof value.seconds === 'number') {
    const milliseconds = value.seconds * 1000 + Math.floor((value.nanoseconds || 0) / 1e6);
    const date = new Date(milliseconds);
    return Number.isNaN(date.getTime()) ? null : date;
  }
  return null;
}

function clamp(value, min, max) {
  let result = value;
  if (typeof min === 'number' && result < min) result = min;
  if (typeof max === 'number' && result > max) result = max;
  return result;
}

function computePercentile(sortedValues, percentile) {
  if (!sortedValues.length) return null;
  if (sortedValues.length === 1) return sortedValues[0];
  const index = (sortedValues.length - 1) * percentile;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sortedValues[lower];
  const weight = index - lower;
  return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * weight;
}

function parseTimeToMinutes(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.min(Math.max(Math.round(value), 0), 24 * 60);
  }
  if (typeof value === 'string') {
    const match = value.trim().match(/^(\d{1,2})(?::(\d{1,2}))?/);
    if (!match) return null;
    const hours = Number(match[1]);
    const minutes = match[2] !== undefined ? Number(match[2]) : 0;
    if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return null;
    const total = hours * 60 + minutes;
    if (total < 0) return 0;
    if (total > 24 * 60) return 24 * 60;
    return total;
  }
  return null;
}

function mergeSegments(segments) {
  if (!segments.length) return [];
  const sorted = segments
    .map((segment) => ({ ...segment }))
    .sort((a, b) => a.startMinutes - b.startMinutes);
  const result = [];
  for (const segment of sorted) {
    if (!result.length) {
      result.push(segment);
      continue;
    }
    const last = result[result.length - 1];
    if (segment.startMinutes <= last.endMinutes) {
      last.endMinutes = Math.max(last.endMinutes, segment.endMinutes);
    } else {
      result.push(segment);
    }
  }
  return result;
}

function buildDefaultAvailability() {
  return Array.from({ length: 7 }, () => [
    { startMinutes: 0, endMinutes: 24 * 60 },
  ]);
}

async function loadProfessionalCalendar(pool, professionalId) {
  const availability = Array.from({ length: 7 }, () => []);
  let hasAvailability = false;

  if (pool?.promise) {
    try {
      const [rows] = await pool.promise().query(
        `SELECT day_of_week, start_time, end_time FROM user_availability WHERE user_id = ?`,
        [professionalId]
      );
      for (const row of rows || []) {
        const day = Number(row.day_of_week);
        const startMinutes = parseTimeToMinutes(row.start_time);
        const endMinutes = parseTimeToMinutes(row.end_time);
        if (
          Number.isInteger(day) && day >= 0 && day < 7 &&
          typeof startMinutes === 'number' && typeof endMinutes === 'number' &&
          endMinutes > startMinutes
        ) {
          availability[day].push({ startMinutes, endMinutes });
          hasAvailability = true;
        }
      }
    } catch (error) {
      if (process.env.NODE_ENV !== 'production') {
        console.warn('loadProfessionalCalendar availability query failed:', error.message);
      }
    }
  }

  const mergedAvailability = availability.map((segments) => mergeSegments(segments));
  const finalAvailability = hasAvailability ? mergedAvailability : buildDefaultAvailability();

  const unavailable = [];
  if (pool?.promise) {
    try {
      const [rows] = await pool.promise().query(
        `SELECT start_datetime, end_datetime FROM user_not_available WHERE user_id = ?`,
        [professionalId]
      );
      for (const row of rows || []) {
        const start = row.start_datetime ? new Date(row.start_datetime) : null;
        const end = row.end_datetime ? new Date(row.end_datetime) : null;
        if (start && end && !Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime()) && end > start) {
          unavailable.push({ start, end });
        }
      }
    } catch (error) {
      if (process.env.NODE_ENV !== 'production') {
        console.warn('loadProfessionalCalendar unavailability query failed:', error.message);
      }
    }
  }

  return {
    availability: finalAvailability,
    unavailable,
  };
}

function startOfDay(date) {
  const copy = new Date(date);
  copy.setHours(0, 0, 0, 0);
  return copy;
}

function addMinutes(date, minutes) {
  return new Date(date.getTime() + minutes * 60000);
}

function calculateBusinessMinutes(start, end, calendar) {
  if (!(start instanceof Date) || !(end instanceof Date)) return 0;
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return 0;
  if (end <= start) return 0;

  const availability = Array.isArray(calendar?.availability) && calendar.availability.length === 7
    ? calendar.availability
    : buildDefaultAvailability();
  const unavailable = Array.isArray(calendar?.unavailable) ? calendar.unavailable : [];

  let totalMinutes = 0;
  let cursor = new Date(start);

  while (cursor < end) {
    const dayStart = startOfDay(cursor);
    const dayEnd = addMinutes(dayStart, 24 * 60);
    const limit = end < dayEnd ? end : dayEnd;
    const segments = availability[cursor.getDay()] || [];

    for (const segment of segments) {
      const segmentStart = addMinutes(dayStart, segment.startMinutes);
      const segmentEnd = addMinutes(dayStart, segment.endMinutes);
      const windowStart = segmentStart > cursor ? segmentStart : cursor;
      const windowEnd = segmentEnd < limit ? segmentEnd : limit;
      if (windowEnd <= windowStart) continue;

      let segmentMinutes = (windowEnd - windowStart) / 60000;
      if (segmentMinutes <= 0) continue;

      for (const blocked of unavailable) {
        if (!blocked?.start || !blocked?.end) continue;
        const overlapStart = blocked.start > windowStart ? blocked.start : windowStart;
        const overlapEnd = blocked.end < windowEnd ? blocked.end : windowEnd;
        if (overlapEnd > overlapStart) {
          segmentMinutes -= (overlapEnd - overlapStart) / 60000;
        }
      }

      if (segmentMinutes > 0) {
        totalMinutes += segmentMinutes;
      }
    }

    cursor = dayEnd;
  }

  return Math.max(0, totalMinutes);
}

function collectIdentifiersByKey(data, keywords) {
  const identifiers = new Set();
  const visit = (node, depth = 0) => {
    if (!node || typeof node !== 'object' || depth > 3) return;
    if (Array.isArray(node)) {
      for (const item of node) visit(item, depth + 1);
      return;
    }
    for (const [key, value] of Object.entries(node)) {
      const lowerKey = key.toLowerCase();
      const matches = keywords.some((keyword) => lowerKey.includes(keyword));
      if (matches) {
        if (typeof value === 'string' || typeof value === 'number') {
          const normalized = normalizeIdentifier(value);
          if (normalized) identifiers.add(normalized);
        } else if (Array.isArray(value)) {
          for (const item of value) {
            const normalized = normalizeIdentifier(item);
            if (normalized) identifiers.add(normalized);
            if (typeof item === 'object') {
              const nested = normalizeIdentifier(item?.id || item?.uid || item?.userId || item?.user_id);
              if (nested) identifiers.add(nested);
            }
          }
        } else if (typeof value === 'object') {
          const nested = normalizeIdentifier(value.id || value.uid || value.userId || value.user_id);
          if (nested) identifiers.add(nested);
          visit(value, depth + 1);
        }
      } else if (typeof value === 'object') {
        visit(value, depth + 1);
      }
    }
  };
  visit(data, 0);
  return Array.from(identifiers);
}

function augmentProIdentifiersFromConversation(data, proIdentifiers) {
  if (!data || typeof data !== 'object') return;

  // NO añadir todos los participants al set de pros

  // Usa participantsMeta para añadir únicamente los que son pro
  const meta = data.participantsMeta || data.participants_meta || data.participantsInfo;
  if (meta && typeof meta === 'object') {
    for (const [key, val] of Object.entries(meta)) {
      const isPro = val?.is_professional === true || val?.isProfessional === true || val?.professional === true;
      if (isPro) {
        const norm = normalizeIdentifier(key);
        if (norm) proIdentifiers.add(norm);
      }
    }
  }

  // Extra (opcional): si hay otras claves con "professional"
  const additional = collectIdentifiersByKey(data, ['professional', 'provider', 'pro']);
  for (const id of additional) proIdentifiers.add(id);
}

function augmentProIdentifiersFromMessage(messageData, proIdentifiers) {
  if (!messageData || typeof messageData !== 'object') return;
  const candidates = collectIdentifiersByKey(messageData, ['professional', 'provider', 'pro']);
  for (const id of candidates) {
    proIdentifiers.add(id);
  }
}

function determineIsFromProfessional(rawMessage, normalizedSenderId, proIdentifiers) {
  if (rawMessage === null || rawMessage === undefined) return null;

  // Preferencias explícitas si existen
  if (typeof rawMessage.isFromProfessional === 'boolean') return rawMessage.isFromProfessional;
  if (typeof rawMessage.fromProfessional === 'boolean') return rawMessage.fromProfessional;
  if (typeof rawMessage.fromPro === 'boolean') return rawMessage.fromPro;
  if (typeof rawMessage.isPro === 'boolean') return rawMessage.isPro;

  const sender = rawMessage.sender || rawMessage.user || rawMessage.author;
  if (sender) {
    if (typeof sender.isProfessional === 'boolean') return sender.isProfessional;
    if (typeof sender.professional === 'boolean') return sender.professional;
    if (typeof sender.isPro === 'boolean') return sender.isPro;
    const senderRole = sender.role || sender.type;
    if (typeof senderRole === 'string') {
      const role = senderRole.toLowerCase();
      if (role.includes('pro') || role.includes('professional') || role.includes('provider')) return true;
      if (role.includes('client') || role.includes('customer') || role.includes('user')) return false;
    }
  }

  const explicitRole = rawMessage.senderRole || rawMessage.role || rawMessage.sender_type || rawMessage.type;
  if (typeof explicitRole === 'string') {
    const role = explicitRole.toLowerCase();
    if (role.includes('pro') || role.includes('professional') || role.includes('provider')) return true;
    if (role.includes('client') || role.includes('customer') || role.includes('user')) return false;
  }

  // Fallback robusto: si tengo el set de profesionales, clasifica por senderId
  if (normalizedSenderId) {
    if (proIdentifiers.has(normalizedSenderId)) return true;
    if (proIdentifiers.size > 0) return false; // si no es pro, es cliente
  }

  return null;
}

function extractSenderId(rawMessage) {
  const candidates = [
    rawMessage?.senderId,
    rawMessage?.sender_id,
    rawMessage?.userId,
    rawMessage?.user_id,
    rawMessage?.from,
    rawMessage?.fromUserId,
    rawMessage?.authorId,
    rawMessage?.author_id,
    rawMessage?.participantId,
    rawMessage?.participant_id,
    rawMessage?.sender?.id,
    rawMessage?.sender?.uid,
    rawMessage?.sender?.userId,
    rawMessage?.sender?.user_id,
    rawMessage?.user?.id,
    rawMessage?.user?.uid,
    rawMessage?.user?.userId,
    rawMessage?.user?.user_id,
    rawMessage?.author?.id,
    rawMessage?.author?.uid,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeIdentifier(candidate);
    if (normalized) return normalized;
  }
  return null;
}

function extractConversationId(rawMessage, context) {
  const candidates = [
    rawMessage?.conversationId,
    rawMessage?.conversation_id,
    rawMessage?.chatId,
    rawMessage?.chat_id,
    rawMessage?.threadId,
    rawMessage?.thread_id,
    rawMessage?.roomId,
    rawMessage?.room_id,
    rawMessage?.channelId,
    rawMessage?.channel_id,
    context?.conversationId,
    context?.id,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeIdentifier(candidate);
    if (normalized) return normalized;
  }
  return context?.fallbackConversationId || 'conversation';
}

function normalizeMessage(rawMessage, context) {
  if (!rawMessage || typeof rawMessage !== 'object') return null;

  const timestamp = normalizeFirestoreDate(
    rawMessage.createdAt ?? rawMessage.created_at ??
    rawMessage.sentAt ?? rawMessage.sent_at ??
    rawMessage.timestamp ?? rawMessage.time ??
    rawMessage.date ?? rawMessage.createdOn ?? rawMessage.sent_on
  );
  if (!timestamp) return null;

  const senderId = extractSenderId(rawMessage);
  const isFromProfessional = determineIsFromProfessional(rawMessage, senderId, context.proIdentifiers);
  if (typeof isFromProfessional !== 'boolean') return null;

  const messageId = normalizeIdentifier(
    rawMessage.id ?? rawMessage.messageId ?? rawMessage.message_id ?? rawMessage.localId ?? rawMessage.local_id
  ) || `${timestamp.getTime()}::${isFromProfessional}`;

  const conversationId = extractConversationId(rawMessage, context);

  return {
    id: messageId,
    conversationId,
    timestamp,
    isFromProfessional,
  };
}

async function extractMessagesFromConversation(entry, globalProIdentifiers, targetProfessionalId) {
  const messages = [];
  const seenIds = new Set();

  // 1) Set de pros por conversación 
  const convPro = new Set(); 
  const meta = entry.data?.participantsMeta || entry.data?.participants_meta || entry.data?.participantsInfo; 
  if (meta && typeof meta === 'object') { 
    for (const [key, val] of Object.entries(meta)) { 
      const isPro = val?.is_professional === true || val?.isProfessional === true || val?.professional === true; 
      if (isPro) { 
        const norm = normalizeIdentifier(key); 
        if (norm) convPro.add(norm); 
      } 
    } 
  } 
  
  // 2) Si hay pros declarados y el pro objetivo NO está, saltar conversación 
  if (convPro.size && targetProfessionalId && !convPro.has(targetProfessionalId)) { 
    return []; 
  } 
  
  const context = {
    id: entry.id,
    conversationId: normalizeIdentifier(entry.data?.conversationId) || entry.id,
    fallbackConversationId: entry.id,
    proIdentifiers: convPro.size ? convPro : globalProIdentifiers,
  };

  if (Array.isArray(entry.data?.messages)) {
    entry.data.messages.forEach((rawMessage, index) => {
      augmentProIdentifiersFromMessage(rawMessage, context.proIdentifiers);
      const normalized = normalizeMessage(rawMessage, {
        ...context,
        fallbackConversationId: context.conversationId,
        fallbackId: `inline-${index}`,
      });
      if (normalized && !seenIds.has(normalized.conversationId + '::' + normalized.id)) {
        seenIds.add(normalized.conversationId + '::' + normalized.id);
        messages.push(normalized);
      }
    });
  }

  if (entry.ref?.collection) {
    try {
      const snapshot = await entry.ref.collection('messages').get();
      snapshot.forEach((doc) => {
        const data = doc.data() || {};
        augmentProIdentifiersFromMessage(data, context.proIdentifiers);
        const normalized = normalizeMessage({ id: doc.id, ...data }, context);
        if (normalized && !seenIds.has(normalized.conversationId + '::' + normalized.id)) {
          seenIds.add(normalized.conversationId + '::' + normalized.id);
          messages.push(normalized);
        }
      });
    } catch (error) {
      if (process.env.NODE_ENV !== 'production') {
        console.warn('Failed to read messages subcollection:', entry.ref.path, error.message);
      }
    }
  }

  return messages;
}

async function fetchConversations(db, collectionNames, fieldNames, serviceValues) {
  const conversations = new Map();
  for (const collectionName of collectionNames) {
    for (const fieldName of fieldNames) {
      for (const value of serviceValues) {
        if (value === null || value === undefined) continue;
        try {
          const snapshot = await db.collection(collectionName).where(fieldName, '==', value).get();
          snapshot.forEach((doc) => {
            const key = doc.ref.path;
            if (!conversations.has(key)) {
              conversations.set(key, {
                id: doc.id,
                ref: doc.ref,
                data: doc.data() || {},
              });
            }
          });
        } catch (error) {
          if (process.env.NODE_ENV !== 'production') {
            console.warn(`Firestore query failed for ${collectionName}.${fieldName}:`, error.message);
          }
        }
      }
    }
  }
  return Array.from(conversations.values());
}

async function fetchMessagesFromCollectionGroup(db, fieldNames, serviceValues, proIdentifiers) {
  const messages = [];
  const seenIds = new Set();
  try {
    for (const fieldName of fieldNames) {
      for (const value of serviceValues) {
        if (value === null || value === undefined) continue;
        try {
          const snapshot = await db.collectionGroup('messages').where(fieldName, '==', value).get();
          snapshot.forEach((doc) => {
            const data = doc.data() || {};
            augmentProIdentifiersFromMessage(data, proIdentifiers);
            const parentId = doc.ref.parent?.parent?.id;
            const normalized = normalizeMessage({ id: doc.id, ...data }, {
              id: parentId,
              conversationId: parentId,
              fallbackConversationId: parentId || 'conversation',
              proIdentifiers,
            });
            if (normalized) {
              const key = normalized.conversationId + '::' + normalized.id;
              if (!seenIds.has(key)) {
                seenIds.add(key);
                messages.push(normalized);
              }
            }
          });
        } catch (error) {
          if (process.env.NODE_ENV !== 'production') {
            console.warn('collectionGroup query failed:', error.message);
          }
        }
      }
    }
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('collectionGroup processing failed:', error.message);
    }
  }
  return messages;
}

async function fetchMessagesFromCollections(db, collectionNames, fieldNames, serviceValues, proIdentifiers) {
  const messages = [];
  const seenIds = new Set();
  for (const collectionName of collectionNames) {
    for (const fieldName of fieldNames) {
      for (const value of serviceValues) {
        if (value === null || value === undefined) continue;
        try {
          const snapshot = await db.collection(collectionName).where(fieldName, '==', value).get();
          snapshot.forEach((doc) => {
            const data = doc.data() || {};
            augmentProIdentifiersFromMessage(data, proIdentifiers);
            const normalized = normalizeMessage({ id: doc.id, ...data }, {
              id: doc.id,
              conversationId: normalizeIdentifier(data.conversationId) || doc.id,
              fallbackConversationId: doc.id,
              proIdentifiers,
            });
            if (normalized) {
              const key = normalized.conversationId + '::' + normalized.id;
              if (!seenIds.has(key)) {
                seenIds.add(key);
                messages.push(normalized);
              }
            }
          });
        } catch (error) {
          if (process.env.NODE_ENV !== 'production') {
            console.warn(`Direct message collection query failed for ${collectionName}.${fieldName}:`, error.message);
          }
        }
      }
    }
  }
  return messages;
}

async function collectServiceMessages(db, serviceId, proIdentifiers, targetProfessionalId) {
  const serviceValues = Array.from(new Set([
    serviceId,
    normalizeIdentifier(serviceId),
    Number.isFinite(Number(serviceId)) ? Number(serviceId) : null,
  ].filter((value) => value !== null && value !== undefined)));

  const conversations = await fetchConversations(db, DEFAULT_CONVERSATION_COLLECTIONS, DEFAULT_SERVICE_FIELD_NAMES, serviceValues);
  const messages = [];

  for (const conversation of conversations) { 
    const conversationMessages = await extractMessagesFromConversation( 
      conversation, 
      proIdentifiers, 
      targetProfessionalId
    );
    messages.push(...conversationMessages);
  }

  if (!messages.length) {
    const fromCollectionGroup = await fetchMessagesFromCollectionGroup(db, DEFAULT_SERVICE_FIELD_NAMES, serviceValues, proIdentifiers);
    messages.push(...fromCollectionGroup);
  }

  if (!messages.length) {
    const fromMessageCollections = await fetchMessagesFromCollections(db, DEFAULT_MESSAGE_COLLECTIONS, DEFAULT_SERVICE_FIELD_NAMES, serviceValues, proIdentifiers);
    messages.push(...fromMessageCollections);
  }

  if (!messages.length && proIdentifiers.size) {
    const dbConvs = await fetchConversationsByProfessional(
      db,
      DEFAULT_CONVERSATION_COLLECTIONS,
      DEFAULT_PARTICIPANT_FIELD_NAMES,
      Array.from(proIdentifiers),
      /*debug*/ null
    );
    for (const c of dbConvs) {
      augmentProIdentifiersFromConversation(c.data, proIdentifiers);
      messages.push(...await extractMessagesFromConversation(c, proIdentifiers, normalizedProfessionalId));
    }
  }

  return messages;
}

function buildResponsePairs(messages, calendar, windowStart, now) {
  const grouped = new Map();
  for (const message of messages) {
    if (!grouped.has(message.conversationId)) grouped.set(message.conversationId, []);
    grouped.get(message.conversationId).push(message);
  }

  const pairs = [];
  const capMilliseconds = C_MAX_MINUTES * 60000;

  for (const conversationMessages of grouped.values()) {
    conversationMessages.sort((a, b) => a.timestamp - b.timestamp);
    const pending = [];

    for (const message of conversationMessages) {
      if (message.isFromProfessional) {
        if (!pending.length) continue;
        const clientMessage = pending.shift();
        if (clientMessage.timestamp < windowStart) {
          continue;
        }
        const naturalResponse = message.timestamp;
        const capDate = new Date(clientMessage.timestamp.getTime() + capMilliseconds);
        const effectiveResponse = naturalResponse > capDate ? capDate : naturalResponse;
        const rawMinutes = calculateBusinessMinutes(clientMessage.timestamp, effectiveResponse, calendar);
        const deltaRaw = clamp(rawMinutes, 0, C_MAX_MINUTES);
        const ageDays = (now - clientMessage.timestamp) / (1000 * 60 * 60 * 24);
        if (Number.isFinite(ageDays) && ageDays >= 0) {
          pairs.push({ deltaRaw, ageDays });
        }
      } else {
        pending.push(message);
      }
    }

    for (const clientMessage of pending) {
      if (clientMessage.timestamp < windowStart) continue;
      const capDate = new Date(clientMessage.timestamp.getTime() + capMilliseconds);
      const rawMinutes = calculateBusinessMinutes(clientMessage.timestamp, capDate, calendar);
      const deltaRaw = clamp(rawMinutes, 0, C_MAX_MINUTES);
      const ageDays = (now - clientMessage.timestamp) / (1000 * 60 * 60 * 24);
      if (Number.isFinite(ageDays) && ageDays >= 0) {
        pairs.push({ deltaRaw, ageDays });
      }
    }
  }

  return pairs;
}

function recordDebugStep(debug, stage, data) {
  if (!debug) return;
  debug.steps.push({ stage, data, timestamp: new Date().toISOString() });
}

function computeWeightedResponseTime(pairs, debug) {
  let numerator = 0;
  let denominator = 0;

  for (const item of pairs) {
    const weight = Math.pow(0.5, item.ageDays / HALF_LIFE_DAYS);
    if (!Number.isFinite(weight) || weight <= 0) continue;
    numerator += weight * item.delta;
    denominator += weight;
  }

  if (!denominator) {
    recordDebugStep(debug, 'weighted_response_time_denominator_zero', {
      numerator,
      denominator,
    });
    return null;
  }

  const responseTime = numerator / denominator;
  const result = Number.isFinite(responseTime) ? responseTime : null;
  recordDebugStep(debug, 'weighted_response_time_computed', {
    numerator,
    denominator,
    responseTime: result,
  });
  return result;
}

async function computeServiceResponseTime({ serviceId, professionalId, pool }) {
  const debug = { steps: [] };
  const firestore = getFirestore();
  if (!firestore) {
    recordDebugStep(debug, 'firestore_unavailable', {});
    return { value: null, debug };
  }

  const proIdentifiers = new Set();
  const normalizedProfessionalId = normalizeIdentifier(professionalId);
  if (normalizedProfessionalId) {
    proIdentifiers.add(normalizedProfessionalId);
  }

  const messages = await collectServiceMessages( 
    firestore, 
    serviceId, 
    proIdentifiers, 
    normalizedProfessionalId
  );

  recordDebugStep(debug, 'messages_collected', {
    serviceId,
    professionalId,
    normalizedProfessionalId,
    proIdentifierCount: proIdentifiers.size,
    messageCount: messages.length,
  });
  if (!messages.length) {
    recordDebugStep(debug, 'no_messages_found', {});
    return { value: null, debug };
  }

  const calendar = await loadProfessionalCalendar(pool, professionalId);
  recordDebugStep(debug, 'calendar_loaded', {
    hasCalendar: Boolean(calendar),
  });

  const now = new Date();
  const windowStart = new Date(now.getTime() - RESPONSE_WINDOW_DAYS * 24 * 60 * 60 * 1000);
  const pairs = buildResponsePairs(messages, calendar, windowStart, now);
  recordDebugStep(debug, 'response_pairs_built', {
    pairCount: pairs.length,
  });

  const rawValues = pairs.map((pair) => pair.deltaRaw).sort((a, b) => a - b);

  if (pairs.length < MIN_PAIRS) {
    recordDebugStep(debug, 'insufficient_pairs_for_trimmed_mean', {
      pairCount: pairs.length,
      minimumRequired: MIN_PAIRS,
    });
    const fallback = computeWeightedResponseTime(
      pairs.map((pair) => ({ delta: pair.deltaRaw, ageDays: pair.ageDays })),
      debug,
    );
    if (fallback !== null) {
      recordDebugStep(debug, 'fallback_weighted_response_time', {
        value: fallback,
      });
      return { value: fallback, debug };
    }
    recordDebugStep(debug, 'fallback_weighted_response_time_null', {});
    return { value: null, debug };
  }

  const p5 = computePercentile(rawValues, 0.05);
  const p95 = computePercentile(rawValues, 0.95);
  recordDebugStep(debug, 'winsorization_bounds', {
    p5,
    p95,
  });

  const winsorized = pairs.map((pair) => ({
    delta: clamp(pair.deltaRaw, p5, p95),
    ageDays: pair.ageDays,
  }));

  const sortedByDelta = winsorized.slice().sort((a, b) => a.delta - b.delta);
  const trimCount = Math.floor(sortedByDelta.length * TRIM_PERCENT);
  const trimmed = sortedByDelta.slice(trimCount, sortedByDelta.length - trimCount || sortedByDelta.length);
  recordDebugStep(debug, 'trimmed_pairs', {
    originalCount: winsorized.length,
    trimCount,
    resultingCount: trimmed.length,
  });

  if (!trimmed.length) {
    recordDebugStep(debug, 'trimmed_pairs_empty', {});
    return { value: null, debug };
  }

  const finalValue = computeWeightedResponseTime(trimmed, debug);
  recordDebugStep(debug, 'final_response_time', { value: finalValue });
  return { value: finalValue, debug };
}

module.exports = {
  computeServiceResponseTime,
};
