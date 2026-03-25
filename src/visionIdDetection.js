const axios = require('axios');
const sharp = require('sharp');
const { GoogleAuth } = require('google-auth-library');

const VISION_API_URL = 'https://vision.googleapis.com/v1/images:annotate';
const VISION_SCOPES = ['https://www.googleapis.com/auth/cloud-platform'];
const INVALID_DOCUMENT_NUMBER_WORDS = new Set([
  'NACIONAL',
  'NACIONALIDAD',
  'IDENTIDAD',
  'IDENTIFICATION',
  'IDENTITE',
  'DOCUMENT',
  'DOCUMENTO',
  'DOCUMENTS',
  'NUMERO',
  'NUMBER',
  'NUM',
  'APELLIDOS',
  'SURNAME',
  'NOMBRE',
  'NAME',
  'PRENOM',
  'PASSPORT',
  'PASSEPORT',
  'LICENSE',
  'LICENCE',
  'CARD',
  'CARTE',
  'PERMIS',
  'ESPANOLA',
  'ESPANOL',
  'REPUBLICA',
  'ESPANA',
]);
const DOCUMENT_LABEL_REGEX = /\b(?:DNI|NIF|NIE|DOCUMENT(?:\s*(?:NUMBER|NO|NUM))?|DOC(?:\s*(?:NUMBER|NO|NUM))?|ID(?:ENTITY|ENTIFICATION)?(?:\s*(?:NUMBER|NO|NUM))?|IDENTITY\s+CARD(?:\s*(?:NUMBER|NO|NUM))?|PASSPORT(?:\s*(?:NUMBER|NO|NUM))?|PASSEPORT(?:\s*(?:NUMBER|NO|NUM))?|LICENSE(?:\s*(?:NUMBER|NO|NUM))?|LICENCE(?:\s*(?:NUMBER|NO|NUM))?|NUM(?:BER|ERO)?\s+(?:DE|DO|DU|DEL)\s+(?:DOCUMENTO|DOCUMENT|DOC|IDENTIDAD|IDENTITE|PASAPORTE|PASSEPORT|PASSAPORTE|CARTE|CARD|PERMIS)|DOCUMENTO\s+NACIONAL\s+DE\s+IDENTIDAD)\b/;
const CANDIDATE_TOKEN_REGEX = /\b[A-Z0-9<]{5,20}\b/g;

function normalizeDocumentNumber(value) {
  return String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function normalizeOcrText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
}

function getDocumentNumberProfile(value) {
  const normalizedValue = normalizeDocumentNumber(value);
  const digitCount = (normalizedValue.match(/\d/g) || []).length;
  const letterCount = (normalizedValue.match(/[A-Z]/g) || []).length;

  return {
    normalizedValue,
    digitCount,
    letterCount,
    length: normalizedValue.length,
    hasDigits: digitCount > 0,
    hasLetters: letterCount > 0,
  };
}

function isLikelyDocumentNumber(value) {
  const profile = getDocumentNumberProfile(value);

  if (profile.length < 5 || profile.length > 20) {
    return false;
  }

  if (!profile.hasDigits) {
    return false;
  }

  if (INVALID_DOCUMENT_NUMBER_WORDS.has(profile.normalizedValue)) {
    return false;
  }

  if (/(.)\1{5,}/.test(profile.normalizedValue)) {
    return false;
  }

  return true;
}

function scoreDocumentNumberCandidate(value, source, baseScore) {
  const profile = getDocumentNumberProfile(value);

  if (!isLikelyDocumentNumber(profile.normalizedValue)) {
    return null;
  }

  let score = baseScore;

  if (profile.hasLetters && profile.hasDigits) {
    score += 14;
  } else if (profile.hasDigits) {
    score += 8;
  }

  if (profile.length >= 7 && profile.length <= 10) {
    score += 8;
  } else if (profile.length >= 6 && profile.length <= 12) {
    score += 5;
  }

  if (/^[A-Z]{1,4}\d{4,12}[A-Z]{0,3}$/.test(profile.normalizedValue)) {
    score += 12;
  } else if (/^\d{4,12}[A-Z]{1,4}$/.test(profile.normalizedValue)) {
    score += 10;
  } else if (/^\d{6,16}$/.test(profile.normalizedValue)) {
    score += 6;
  }

  if (source.startsWith('mrz_')) {
    score += 8;
  }

  return {
    value: profile.normalizedValue,
    source,
    score,
  };
}

function collectCandidates(text) {
  const normalizedText = normalizeOcrText(text);
  const candidates = new Map();
  const pushCandidate = (rawValue, score, source) => {
    const candidate = scoreDocumentNumberCandidate(rawValue, source, score);
    if (!candidate) {
      return;
    }

    const currentCandidate = candidates.get(candidate.value);
    if (!currentCandidate || candidate.score > currentCandidate.score) {
      candidates.set(candidate.value, candidate);
    }
  };

  const labeledPatterns = [
    {
      score: 120,
      source: 'labeled_spanish_id',
      regex: /\b(?:DNI|NIF|NIE)\b[^\r\nA-Z0-9]{0,8}([0-9]{8}[A-Z]|[XYZ][0-9]{7}[A-Z])\b/g,
    },
    {
      score: 118,
      source: 'spanish_document_header',
      regex: /\bDOCUMENTO\s+NACIONAL\s+DE\s+IDENTIDAD\b[^\r\nA-Z0-9]{0,12}([0-9]{8}[A-Z]|[XYZ][0-9]{7}[A-Z])\b/g,
    },
    {
      score: 110,
      source: 'labeled_document_number',
      regex: /\b(?:DOCUMENT(?:\s*(?:NUMBER|NO))?|DOC(?:\s*(?:NUMBER|NO))?|ID(?:ENTITY)?(?:\s*(?:NUMBER|NO))?|PASSPORT(?:\s*(?:NUMBER|NO))?|NUM(?:ERO)?\s+DE\s+DOCUMENTO|NUM(?:ERO)?\s+DE\s+PASAPORTE)\b[^\r\nA-Z0-9]{0,10}([A-Z0-9<]{5,20})/g,
    },
    {
      score: 108,
      source: 'labeled_passport_number',
      regex: /\b(?:PASSPORT|PASSEPORT|PASAPORTE|PASSAPORTE)\b[^\r\nA-Z0-9]{0,10}([A-Z0-9<]{5,20})\b/g,
    },
  ];

  for (const { score, source, regex } of labeledPatterns) {
    for (const match of normalizedText.matchAll(regex)) {
      pushCandidate(match[1], score, source);
    }
  }

  for (const match of normalizedText.matchAll(/\b[0-9]{8}[A-Z]\b/g)) {
    pushCandidate(match[0], 100, 'standalone_dni');
  }

  for (const match of normalizedText.matchAll(/\b[XYZ][0-9]{7}[A-Z]\b/g)) {
    pushCandidate(match[0], 100, 'standalone_nie');
  }

  for (const match of normalizedText.matchAll(/\b[A-Z]{1,4}\d{4,12}[A-Z]{0,3}\b/g)) {
    pushCandidate(match[0], 96, 'standalone_mixed_prefix');
  }

  for (const match of normalizedText.matchAll(/\b\d{4,12}[A-Z]{1,4}\b/g)) {
    pushCandidate(match[0], 94, 'standalone_mixed_suffix');
  }

  const lines = normalizedText
    .split(/\r?\n/)
    .map((line) => line.replace(/\s+/g, ' ').trim())
    .filter(Boolean);

  for (let index = 0; index < lines.length; index += 1) {
    const currentLine = lines[index];
    if (!DOCUMENT_LABEL_REGEX.test(currentLine)) {
      continue;
    }

    for (const match of currentLine.matchAll(CANDIDATE_TOKEN_REGEX)) {
      pushCandidate(match[0], 104, 'line_label_same');
    }

    const nextLine = lines[index + 1] || '';
    for (const match of nextLine.matchAll(CANDIDATE_TOKEN_REGEX)) {
      pushCandidate(match[0], 101, 'line_label_next');
    }
  }

  const mrzLines = normalizedText
    .split(/\r?\n/)
    .map((line) => line.replace(/\s+/g, ''))
    .filter((line) => /^[A-Z0-9<]{28,44}$/.test(line));

  for (let index = 0; index < mrzLines.length; index += 1) {
    const currentLine = mrzLines[index];
    const nextLine = mrzLines[index + 1] || '';

    if (/^P</.test(currentLine) && nextLine.length >= 9) {
      pushCandidate(nextLine.slice(0, 9).replace(/</g, ''), 95, 'mrz_passport');
    }

    if (/^(ID|I<|A<|C<)/.test(currentLine) && currentLine.length >= 14) {
      pushCandidate(currentLine.slice(5, 14).replace(/</g, ''), 93, 'mrz_id_card');
    }
  }

  return [...candidates.values()];
}

function chooseBestCandidate(candidates = []) {
  return candidates
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }

      return right.value.length - left.value.length;
    })[0] || null;
}

function createVisionIdDetector(credentials) {
  const auth = new GoogleAuth({
    credentials,
    scopes: VISION_SCOPES,
  });

  async function getAccessToken() {
    const client = await auth.getClient();
    const tokenResponse = await client.getAccessToken();
    return typeof tokenResponse === 'string' ? tokenResponse : tokenResponse?.token || '';
  }

  async function prepareImageForVision(imageBuffer) {
    return sharp(imageBuffer)
      .rotate()
      .resize({ width: 1800, withoutEnlargement: true })
      .jpeg({ quality: 92 })
      .toBuffer();
  }

  async function detectIdNumberFromImageBuffer(imageBuffer) {
    const optimizedImageBuffer = await prepareImageForVision(imageBuffer);
    const accessToken = await getAccessToken();

    if (!accessToken) {
      throw new Error('VISION_ACCESS_TOKEN_MISSING');
    }

    const response = await axios.post(
      VISION_API_URL,
      {
        requests: [
          {
            image: {
              content: optimizedImageBuffer.toString('base64'),
            },
            imageContext: {
              languageHints: ['en', 'es', 'fr', 'pt', 'it', 'de'],
            },
            features: [
              { type: 'DOCUMENT_TEXT_DETECTION', maxResults: 1 },
              { type: 'TEXT_DETECTION', maxResults: 1 },
            ],
          },
        ],
      },
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 30000,
      }
    );

    const visionResponse = response.data?.responses?.[0] || {};
    const ocrText =
      visionResponse?.fullTextAnnotation?.text
      || visionResponse?.textAnnotations?.[0]?.description
      || '';

    const bestCandidate = chooseBestCandidate(collectCandidates(ocrText));

    return {
      detectedIdNumber: bestCandidate?.value || '',
      ocrText,
    };
  }

  return {
    detectIdNumberFromImageBuffer,
  };
}

module.exports = {
  createVisionIdDetector,
};
