const axios = require('axios');
const sharp = require('sharp');
const { GoogleAuth } = require('google-auth-library');

const VISION_API_URL = 'https://vision.googleapis.com/v1/images:annotate';
const VISION_SCOPES = ['https://www.googleapis.com/auth/cloud-platform'];

function normalizeDocumentNumber(value) {
  return String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function normalizeOcrText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
}

function collectCandidates(text) {
  const normalizedText = normalizeOcrText(text);
  const candidates = [];
  const pushCandidate = (rawValue, score, source) => {
    const normalizedValue = normalizeDocumentNumber(rawValue);

    if (normalizedValue.length < 5 || normalizedValue.length > 20) {
      return;
    }

    candidates.push({ value: normalizedValue, score, source });
  };

  const labeledPatterns = [
    {
      score: 120,
      source: 'labeled_spanish_id',
      regex: /\b(?:DNI|NIF|NIE)\b[^\r\nA-Z0-9]{0,8}([0-9]{8}[A-Z]|[XYZ][0-9]{7}[A-Z])\b/g,
    },
    {
      score: 110,
      source: 'labeled_document_number',
      regex: /\b(?:DOCUMENT(?:\s*(?:NUMBER|NO))?|DOC(?:\s*(?:NUMBER|NO))?|ID(?:ENTITY)?(?:\s*(?:NUMBER|NO))?|PASSPORT(?:\s*(?:NUMBER|NO))?|NUMERO(?:\s+DE)?(?:\s+DOCUMENTO)?|NUMERO(?:\s+DE)?(?:\s+PASAPORTE)?)\b[^\r\nA-Z0-9]{0,10}([A-Z0-9<]{5,20})/g,
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

    if (/^(ID|I<|A<|C<)/.test(currentLine) && nextLine.length >= 9) {
      pushCandidate(nextLine.slice(0, 9).replace(/</g, ''), 92, 'mrz_id_card');
    }
  }

  return candidates;
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
              languageHints: ['es', 'en'],
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
