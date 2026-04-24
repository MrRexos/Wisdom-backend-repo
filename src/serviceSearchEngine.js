const path = require('path');

function isMissingModuleForPath(error, possiblePath) {
  if (error?.code !== 'MODULE_NOT_FOUND') {
    return false;
  }

  const normalizedPath = String(possiblePath || '').replace(/\\/g, '/');
  const errorMessage = String(error?.message || '').replace(/\\/g, '/');

  return errorMessage.includes(`'${normalizedPath}'`) || errorMessage.includes(`"${normalizedPath}"`);
}

function loadOptionalJson(possiblePaths = []) {
  for (const possiblePath of possiblePaths) {
    try {
      return require(possiblePath);
    } catch (error) {
      if (!isMissingModuleForPath(error, possiblePath)) {
        throw error;
      }
    }
  }

  return null;
}

function loadClassificationResources() {
  const candidateDirectories = [
    path.resolve(__dirname, 'classification'),
    path.resolve(__dirname, '..', '..', 'Wisdom-repo', 'Wisdom_expo', 'languages', 'classification'),
  ];
  const loadClassificationFile = (fileName) => loadOptionalJson(
    candidateDirectories.map((directory) => path.resolve(directory, fileName))
  );

  const arClassification = loadClassificationFile('ar_clas.json');
  const caClassification = loadClassificationFile('ca_clas.json');
  const enClassification = loadClassificationFile('en_clas.json');
  const esClassification = loadClassificationFile('es_clas.json');
  const frClassification = loadClassificationFile('fr_clas.json');
  const zhClassification = loadClassificationFile('zh_clas.json');

  return {
    ar: arClassification?.ar || {},
    ca: caClassification?.ca || {},
    en: enClassification?.en || {},
    es: esClassification?.es || {},
    fr: frClassification?.fr || {},
    zh: zhClassification?.['zh-CN'] || zhClassification?.zh || {},
  };
}

const CLASSIFICATION_RESOURCES = loadClassificationResources();

const STOP_WORDS = new Set([
  'a', 'al', 'and', 'are', 'at', 'be', 'by', 'con', 'de', 'del', 'des', 'do',
  'du', 'el', 'en', 'et', 'for', 'i', 'il', 'in', 'is', 'la', 'las', 'le',
  'les', 'lo', 'los', 'near', 'of', 'on', 'or', 'para', 'por', 'per', 'sa',
  'service', 'services', 'the', 'to', 'un', 'una', 'unas', 'uno', 'unos', 'y',
]);

const EXTRA_CONCEPT_ALIASES = Object.freeze({
  cat_physiotherapy: ['fisio', 'fisioterapeuta', 'physiotherapist'],
  cat_psychology_therapy: ['psico', 'psicologo', 'psicologa', 'psychologist', 'terapeuta', 'therapist'],
  cat_hairdressing: ['peluquero', 'peluquera', 'hairdresser', 'hairstylist'],
  cat_barbershop: ['barbero', 'barber'],
  cat_manicure_pedicure: ['unas', 'nails', 'nailtech'],
  cat_massages: ['masajista', 'massage therapist'],
  cat_plumbing: ['fontanero', 'fontanera', 'plomero'],
  cat_electricity: ['electricista', 'electrician'],
  cat_locksmithing: ['cerrajero', 'cerrajera', 'locksmith'],
  cat_carpentry_cabinetmaking: ['carpintero', 'carpintera', 'carpenter'],
  cat_gardening: ['jardinero', 'jardinera', 'gardener'],
  cat_photography: ['fotografo', 'fotografa', 'photographer'],
  cat_videography: ['videografo', 'videografa', 'videographer'],
  cat_personal_training: ['entrenador personal', 'personal trainer', 'trainer'],
  cat_lawyers: ['abogado', 'abogada', 'attorney', 'lawyer'],
  cat_dog_walking: ['paseador de perros', 'dog walker'],
  cat_pet_grooming: ['groomer', 'peluqueria canina'],
  cat_it_support: ['informatico', 'informatica', 'it technician'],
  cat_graphic_design: ['graphic designer', 'disenador grafico', 'disenadora grafica'],
  cat_video_editing: ['video editor', 'editor de video'],
  cat_web_development: ['frontend', 'backend', 'fullstack', 'full stack'],
  cat_app_development: ['app developer', 'mobile developer'],
  cat_social_media_management: ['community manager', 'social media manager'],
});

const FIELD_WEIGHTS = Object.freeze({
  title: {
    exactPhrase: 118,
    containsPhrase: 82,
    exactToken: 24,
    prefixToken: 18,
    rootToken: 14,
    fuzzyToken: 10,
    coverageBonus: 18,
    allTokensBonus: 20,
  },
  professional: {
    exactPhrase: 110,
    containsPhrase: 86,
    exactToken: 28,
    prefixToken: 22,
    rootToken: 17,
    fuzzyToken: 13,
    coverageBonus: 20,
    allTokensBonus: 22,
  },
  tags: {
    exactPhrase: 76,
    containsPhrase: 60,
    exactToken: 20,
    prefixToken: 15,
    rootToken: 12,
    fuzzyToken: 9,
    coverageBonus: 14,
    allTokensBonus: 14,
  },
  category: {
    exactPhrase: 98,
    containsPhrase: 74,
    exactToken: 24,
    prefixToken: 18,
    rootToken: 14,
    fuzzyToken: 11,
    coverageBonus: 18,
    allTokensBonus: 18,
  },
  family: {
    exactPhrase: 70,
    containsPhrase: 52,
    exactToken: 18,
    prefixToken: 14,
    rootToken: 10,
    fuzzyToken: 8,
    coverageBonus: 12,
    allTokensBonus: 12,
  },
  description: {
    exactPhrase: 28,
    containsPhrase: 18,
    exactToken: 7,
    prefixToken: 5,
    rootToken: 4,
    fuzzyToken: 3,
    coverageBonus: 6,
    allTokensBonus: 6,
  },
});

const CONTROLLED_TERM_GROUPS = Object.freeze([
  ['paseador', 'walker', 'walking'],
  ['paseador de perros', 'dog walker'],
  ['cuidador', 'cuidadora', 'caregiver', 'caretaker', 'carer', 'sitter'],
  ['cuidador de mascotas', 'pet sitter', 'pet care'],
  ['niñera', 'babysitter', 'nanny', 'childcare'],
  ['limpieza', 'cleaning', 'cleaner', 'housekeeping'],
  ['limpieza del hogar', 'home cleaning', 'house cleaning'],
  ['chef', 'cook', 'private chef'],
  ['fontanero', 'plumber', 'plumbing'],
  ['electricista', 'electrician', 'electrical'],
  ['cerrajero', 'locksmith'],
  ['jardinero', 'gardener', 'gardening'],
  ['peluquero', 'peluquera', 'hairdresser', 'hairstylist'],
  ['barbero', 'barber', 'barbershop'],
  ['maquillador', 'maquilladora', 'makeup artist', 'makeup'],
  ['masajista', 'massage therapist', 'massage'],
  ['fisioterapeuta', 'physiotherapist', 'physical therapist', 'physio'],
  ['psicologo', 'psicologa', 'psychologist', 'therapist', 'therapy'],
  ['entrenador', 'entrenadora', 'trainer', 'coach'],
  ['profesor', 'profesora', 'teacher', 'tutor', 'lessons'],
  ['fotografo', 'fotografa', 'photographer', 'photography'],
  ['videografo', 'videografa', 'videographer', 'videography'],
  ['abogado', 'abogada', 'lawyer', 'attorney', 'legal'],
  ['traductor', 'traductora', 'translator', 'translation'],
  ['conductor', 'conductora', 'driver', 'chauffeur'],
  ['mecanico', 'mecanica', 'mechanic'],
  ['mudanza', 'moving', 'mover'],
  ['disenador', 'disenadora', 'designer', 'design'],
  ['desarrollador', 'desarrolladora', 'developer', 'development'],
  ['programador', 'programadora', 'programmer', 'software'],
]);

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, '\'')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>');
}

function normalizeServiceSearchText(value) {
  return decodeHtmlEntities(value)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[_-]+/g, ' ')
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeText(value) {
  const normalized = normalizeServiceSearchText(value);
  return normalized ? normalized.split(' ').filter(Boolean) : [];
}

function uniqueValues(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function truncateText(value, maxChars = 0) {
  const text = String(value || '').trim();
  if (!maxChars || text.length <= maxChars) {
    return text;
  }
  return text.slice(0, maxChars);
}

function deriveRoots(token) {
  const normalized = normalizeServiceSearchText(token);
  if (!normalized) return [];

  const roots = new Set([normalized]);
  const suffixes = [
    'aciones', 'amientos', 'imientos', 'adores', 'adoras', 'antes', 'entes',
    'istas', 'ismos', 'logias', 'logos', 'logas', 'mente', 'ciones', 'siones',
    'idades', 'amente', 'acion', 'amiento', 'imiento', 'adora', 'ador', 'ante',
    'ente', 'ista', 'ismo', 'logia', 'logo', 'loga', 'eria', 'ario', 'aria',
    'eros', 'eras', 'ero', 'era', 'dor', 'dora', 'tion', 'sion', 'ment',
    'ings', 'ing', 'ers', 'ies', 'ied', 'ist', 'ism', 'ogy', 'es', 's', 'y',
  ];

  suffixes.forEach((suffix) => {
    if (!normalized.endsWith(suffix)) return;
    const candidate = normalized.slice(0, normalized.length - suffix.length).trim();
    if (candidate.length >= 4) {
      roots.add(candidate);
    }
  });

  if (normalized.length >= 6) {
    roots.add(normalized.slice(0, 4));
    roots.add(normalized.slice(0, 5));
    roots.add(normalized.slice(0, 6));
  }

  return Array.from(roots).filter((root) => root.length >= 3);
}

function createFieldIndex(values, { maxChars = 0 } = {}) {
  const sourceValues = Array.isArray(values) ? values : [values];
  const phrases = sourceValues
    .map((value) => truncateText(value, maxChars))
    .map((value) => normalizeServiceSearchText(value))
    .filter(Boolean);
  const tokens = uniqueValues(phrases.flatMap((phrase) => tokenizeText(phrase)));
  const compactPhrases = uniqueValues(phrases.map((phrase) => phrase.replace(/\s+/g, '')));

  return {
    phrases,
    phraseSet: new Set(phrases),
    compactPhrases,
    compactPhraseSet: new Set(compactPhrases),
    tokens,
  };
}

function humanizeConceptKey(key) {
  return String(key || '')
    .replace(/^(cat|fam)_/, '')
    .replace(/_/g, ' ')
    .trim();
}

function buildConceptStore(scope) {
  const store = new Map();
  const keys = new Set();
  const aliasPrefix = scope === 'families' ? 'fam_' : 'cat_';

  Object.values(CLASSIFICATION_RESOURCES).forEach((resource) => {
    const scopedValues = resource?.[scope] || {};
    Object.keys(scopedValues).forEach((key) => keys.add(key));
  });

  Object.keys(EXTRA_CONCEPT_ALIASES)
    .filter((key) => key.startsWith(aliasPrefix))
    .forEach((key) => keys.add(key));

  keys.forEach((key) => {
    const aliases = new Set();
    aliases.add(key);
    aliases.add(humanizeConceptKey(key));

    Object.values(CLASSIFICATION_RESOURCES).forEach((resource) => {
      const label = resource?.[scope]?.[key];
      if (label) {
        aliases.add(label);
      }
    });

    (EXTRA_CONCEPT_ALIASES[key] || []).forEach((alias) => aliases.add(alias));

    store.set(key, {
      key,
      aliases: Array.from(aliases),
      index: createFieldIndex(Array.from(aliases)),
    });
  });

  return store;
}

const CATEGORY_CONCEPTS = buildConceptStore('categories');
const FAMILY_CONCEPTS = buildConceptStore('families');

function buildControlledTermIndex(groups) {
  const index = new Map();

  groups.forEach((group) => {
    const normalizedGroup = uniqueValues(group.map((value) => normalizeServiceSearchText(value)).filter(Boolean));
    normalizedGroup.forEach((term) => {
      index.set(term, normalizedGroup);
    });
  });

  return index;
}

const CONTROLLED_TERM_INDEX = buildControlledTermIndex(CONTROLLED_TERM_GROUPS);

function buildTokenNgrams(tokens, maxSize = 3) {
  const list = Array.isArray(tokens) ? tokens.filter(Boolean) : [];
  const ngrams = [];

  for (let size = 1; size <= maxSize; size += 1) {
    for (let start = 0; start <= list.length - size; start += 1) {
      ngrams.push(list.slice(start, start + size).join(' '));
    }
  }

  return uniqueValues(ngrams);
}

function resolveControlledEquivalentTerms(values, maxTerms = 12) {
  const equivalents = [];
  const seen = new Set();

  (Array.isArray(values) ? values : [values]).forEach((value) => {
    const normalizedValue = normalizeServiceSearchText(value);
    if (!normalizedValue) return;

    const group = CONTROLLED_TERM_INDEX.get(normalizedValue);
    if (!group) return;

    group.forEach((candidate) => {
      if (!candidate || candidate === normalizedValue || seen.has(candidate)) {
        return;
      }
      seen.add(candidate);
      equivalents.push(candidate);
    });
  });

  return equivalents.slice(0, maxTerms);
}

function createTokenProfile(token, equivalentTokens = []) {
  const normalizedToken = normalizeServiceSearchText(token);
  const normalizedEquivalentTokens = uniqueValues(
    (equivalentTokens || [])
      .map((value) => normalizeServiceSearchText(value))
      .filter((value) => value && value !== normalizedToken)
  );

  return {
    token: normalizedToken,
    roots: deriveRoots(normalizedToken),
    equivalentProfiles: normalizedEquivalentTokens.map((value) => ({
      token: value,
      roots: deriveRoots(value),
    })),
  };
}

function commonPrefixLength(left, right) {
  const a = String(left || '');
  const b = String(right || '');
  const max = Math.min(a.length, b.length);
  let length = 0;

  while (length < max && a[length] === b[length]) {
    length += 1;
  }

  return length;
}

function boundedLevenshtein(left, right, maxDistance = 2) {
  const a = String(left || '');
  const b = String(right || '');
  const aLength = a.length;
  const bLength = b.length;

  if (Math.abs(aLength - bLength) > maxDistance) {
    return null;
  }

  if (a === b) {
    return 0;
  }

  const previous = Array.from({ length: bLength + 1 }, (_, index) => index);

  for (let row = 1; row <= aLength; row += 1) {
    const current = [row];
    let rowMinimum = current[0];

    for (let column = 1; column <= bLength; column += 1) {
      const substitutionCost = a[row - 1] === b[column - 1] ? 0 : 1;
      const value = Math.min(
        previous[column] + 1,
        current[column - 1] + 1,
        previous[column - 1] + substitutionCost
      );
      current[column] = value;
      if (value < rowMinimum) {
        rowMinimum = value;
      }
    }

    if (rowMinimum > maxDistance) {
      return null;
    }

    for (let column = 0; column <= bLength; column += 1) {
      previous[column] = current[column];
    }
  }

  return previous[bLength] <= maxDistance ? previous[bLength] : null;
}

function compareTokenVariantToFieldToken(tokenVariant, fieldToken, {
  scoreMultiplier = 1,
  allowFuzzy = true,
} = {}) {
  const queryToken = tokenVariant?.token || '';
  const candidateToken = normalizeServiceSearchText(fieldToken);

  if (!queryToken || !candidateToken) {
    return null;
  }

  if (queryToken === candidateToken) {
    return { type: 'exact', score: 1 * scoreMultiplier };
  }

  if (
    queryToken.length >= 4 &&
    (candidateToken.startsWith(queryToken) || queryToken.startsWith(candidateToken))
  ) {
    return { type: 'prefix', score: 0.86 * scoreMultiplier };
  }

  const sharedPrefix = commonPrefixLength(queryToken, candidateToken);
  if (sharedPrefix >= 5 && Math.min(queryToken.length, candidateToken.length) >= 5) {
    return { type: 'root', score: 0.72 * scoreMultiplier };
  }

  const candidateRoots = deriveRoots(candidateToken);
  const hasSharedRoot = (tokenVariant?.roots || []).some((root) => (
    root.length >= 4 && candidateRoots.includes(root)
  ));
  if (hasSharedRoot) {
    return { type: 'root', score: 0.74 * scoreMultiplier };
  }

  if (allowFuzzy && Math.min(queryToken.length, candidateToken.length) >= 5) {
    const maxDistance = queryToken.length >= 8 && candidateToken.length >= 8 ? 2 : 1;
    const distance = boundedLevenshtein(queryToken, candidateToken, maxDistance);
    if (distance !== null) {
      return {
        type: 'fuzzy',
        score: (distance === 1 ? 0.64 : 0.54) * scoreMultiplier,
      };
    }
  }

  return null;
}

function compareQueryTokenToFieldToken(tokenProfile, fieldToken) {
  const originalMatch = compareTokenVariantToFieldToken(tokenProfile, fieldToken);
  let bestMatch = originalMatch;

  (tokenProfile?.equivalentProfiles || []).forEach((equivalentProfile) => {
    const candidateMatch = compareTokenVariantToFieldToken(equivalentProfile, fieldToken, {
      scoreMultiplier: 0.78,
      allowFuzzy: false,
    });
    if (!candidateMatch) return;

    if (!bestMatch || candidateMatch.score > bestMatch.score) {
      bestMatch = candidateMatch;
    }
  });

  return bestMatch;
}

function getFieldWeightByMatchType(weights, type) {
  if (!weights) return 0;
  if (type === 'exact') return weights.exactToken || 0;
  if (type === 'prefix') return weights.prefixToken || 0;
  if (type === 'root') return weights.rootToken || 0;
  if (type === 'fuzzy') return weights.fuzzyToken || 0;
  return 0;
}

function scorePlanAgainstField(searchPlan, fieldIndex, weights) {
  if (!searchPlan?.hasMeaningfulSearch || !fieldIndex) {
    return {
      score: 0,
      matchedTokenIndexes: new Set(),
      matchedTokenCount: 0,
      hasPhraseMatch: false,
    };
  }

  let score = 0;
  let hasPhraseMatch = false;

  let bestPhraseScore = 0;
  (searchPlan.phraseProfiles || []).forEach((phraseProfile) => {
    const phrase = phraseProfile?.phrase || '';
    const compactPhrase = phraseProfile?.compactPhrase || '';
    const multiplier = Number.isFinite(Number(phraseProfile?.multiplier))
      ? Number(phraseProfile.multiplier)
      : 1;

    if (phrase.length < 2) return;

    let phraseScore = 0;
    if (
      fieldIndex.phraseSet.has(phrase)
      || (compactPhrase && fieldIndex.compactPhraseSet.has(compactPhrase))
    ) {
      phraseScore = (weights.exactPhrase || 0) * multiplier;
    } else {
      const containsQuery = fieldIndex.phrases.some((candidatePhrase) => (
        candidatePhrase.includes(phrase)
      ));
      const containsCompactQuery = compactPhrase.length >= 4
        && fieldIndex.compactPhrases.some((candidatePhrase) => candidatePhrase.includes(compactPhrase));

      if (containsQuery || containsCompactQuery) {
        phraseScore = (weights.containsPhrase || 0) * multiplier;
      }
    }

    if (phraseScore > bestPhraseScore) {
      bestPhraseScore = phraseScore;
      hasPhraseMatch = true;
    }
  });
  score += bestPhraseScore;

  const matchedTokenIndexes = new Set();
  searchPlan.tokenProfiles.forEach((tokenProfile, tokenIndex) => {
    let bestMatch = null;

    fieldIndex.tokens.forEach((fieldToken) => {
      const candidateMatch = compareQueryTokenToFieldToken(tokenProfile, fieldToken);
      if (!candidateMatch) return;

      if (!bestMatch || candidateMatch.score > bestMatch.score) {
        bestMatch = candidateMatch;
      }
    });

    if (bestMatch) {
      matchedTokenIndexes.add(tokenIndex);
      score += getFieldWeightByMatchType(weights, bestMatch.type);
    }
  });

  if (searchPlan.tokenProfiles.length > 0) {
    const coverage = matchedTokenIndexes.size / searchPlan.tokenProfiles.length;
    score += coverage * (weights.coverageBonus || 0);
    if (matchedTokenIndexes.size === searchPlan.tokenProfiles.length && matchedTokenIndexes.size > 0) {
      score += weights.allTokensBonus || 0;
    }
  }

  return {
    score,
    matchedTokenIndexes,
    matchedTokenCount: matchedTokenIndexes.size,
    hasPhraseMatch,
  };
}

function getConceptMatches(searchPlan, conceptStore, weights, minimumScore, limit) {
  const matches = [];

  conceptStore.forEach((concept) => {
    const result = scorePlanAgainstField(searchPlan, concept.index, weights);
    if (result.score >= minimumScore) {
      matches.push({
        key: concept.key,
        score: result.score,
      });
    }
  });

  return matches
    .sort((left, right) => right.score - left.score || left.key.localeCompare(right.key))
    .slice(0, limit);
}

function buildServiceSearchPlan(query) {
  const rawQuery = String(query || '').trim();
  const normalizedQuery = normalizeServiceSearchText(rawQuery);
  const rawTokens = tokenizeText(normalizedQuery);
  const primaryTokens = rawTokens.filter((token) => token.length >= 2 && !STOP_WORDS.has(token));
  const fallbackTokens = rawTokens.filter((token) => token.length >= 2);
  const significantTokens = uniqueValues(primaryTokens.length > 0 ? primaryTokens : fallbackTokens).slice(0, 8);
  const ngramCandidates = buildTokenNgrams(rawTokens, 3);
  const controlledEquivalentTerms = resolveControlledEquivalentTerms([
    normalizedQuery,
    ...ngramCandidates,
    ...significantTokens,
  ], 14);
  const equivalentSingleTokens = uniqueValues(
    controlledEquivalentTerms
      .filter((term) => !term.includes(' '))
      .filter((term) => term.length >= 3)
  ).slice(0, 10);
  const equivalentPhrases = uniqueValues(
    controlledEquivalentTerms
      .filter((term) => term.includes(' '))
      .filter((term) => term.length >= 4)
  ).slice(0, 8);
  const tokenProfiles = significantTokens.map((token) => (
    createTokenProfile(
      token,
      resolveControlledEquivalentTerms([token], 6)
        .filter((candidate) => !candidate.includes(' '))
    )
  ));
  const phraseProfiles = [
    ...(normalizedQuery ? [{
      phrase: normalizedQuery,
      compactPhrase: normalizedQuery.replace(/\s+/g, ''),
      multiplier: 1,
      isExpanded: false,
    }] : []),
    ...equivalentPhrases.map((phrase) => ({
      phrase,
      compactPhrase: phrase.replace(/\s+/g, ''),
      multiplier: 0.78,
      isExpanded: true,
    })),
  ];

  const searchPlan = {
    rawQuery,
    normalizedQuery,
    compactQuery: normalizedQuery.replace(/\s+/g, ''),
    phraseProfiles,
    tokenProfiles,
    hasMeaningfulSearch: normalizedQuery.length > 0,
    controlledEquivalentTerms,
  };

  const matchedCategories = getConceptMatches(searchPlan, CATEGORY_CONCEPTS, FIELD_WEIGHTS.category, 34, 8);
  const matchedFamilies = getConceptMatches(searchPlan, FAMILY_CONCEPTS, FIELD_WEIGHTS.family, 32, 6);

  return {
    ...searchPlan,
    sqlTokenPatterns: uniqueValues([
      ...tokenProfiles.map((profile) => profile.token),
      ...equivalentSingleTokens,
    ].filter((token) => token.length >= 3)).slice(0, 10),
    sqlExpandedPhrasePatterns: equivalentPhrases.slice(0, 6),
    matchedCategories,
    matchedFamilies,
    matchedCategoryKeys: matchedCategories.map((item) => item.key),
    matchedFamilyKeys: matchedFamilies.map((item) => item.key),
  };
}

function buildServiceSearchCandidateClause(searchPlan, {
  serviceAlias = 'service',
  userAlias = 'user_account',
  categoryAlias = 'category_type',
  familyAlias = 'family',
} = {}) {
  if (!searchPlan?.hasMeaningfulSearch) {
    return { sql: '', params: [] };
  }

  const clauses = [];
  const params = [];

  const pushClause = (sql, values = []) => {
    clauses.push(sql);
    params.push(...values);
  };

  const rawPattern = `%${searchPlan.rawQuery}%`;
  pushClause(`LOWER(${serviceAlias}.service_title) LIKE LOWER(?)`, [rawPattern]);
  pushClause(`LOWER(CONCAT_WS(' ', ${userAlias}.first_name, ${userAlias}.surname)) LIKE LOWER(?)`, [rawPattern]);
  pushClause(`LOWER(${userAlias}.username) LIKE LOWER(?)`, [rawPattern]);
  pushClause(`LOWER(${categoryAlias}.category_key) LIKE LOWER(?)`, [rawPattern]);
  pushClause(`LOWER(${familyAlias}.family_key) LIKE LOWER(?)`, [rawPattern]);
  pushClause(
    `EXISTS (
      SELECT 1
      FROM service_tags st
      WHERE st.service_id = ${serviceAlias}.id
        AND LOWER(st.tag) LIKE LOWER(?)
    )`,
    [rawPattern]
  );

  if (searchPlan.normalizedQuery.length >= 4) {
    pushClause(`LOWER(${serviceAlias}.description) LIKE LOWER(?)`, [rawPattern]);
  }

  (searchPlan.sqlExpandedPhrasePatterns || []).forEach((phrase) => {
    const phrasePattern = `%${phrase}%`;
    pushClause(`LOWER(${serviceAlias}.service_title) LIKE LOWER(?)`, [phrasePattern]);
    pushClause(
      `EXISTS (
        SELECT 1
        FROM service_tags st
        WHERE st.service_id = ${serviceAlias}.id
          AND LOWER(st.tag) LIKE LOWER(?)
      )`,
      [phrasePattern]
    );
    if (phrase.length >= 5) {
      pushClause(`LOWER(${serviceAlias}.description) LIKE LOWER(?)`, [phrasePattern]);
    }
  });

  if (searchPlan.matchedCategoryKeys.length > 0) {
    pushClause(
      `${categoryAlias}.category_key IN (${searchPlan.matchedCategoryKeys.map(() => '?').join(', ')})`,
      searchPlan.matchedCategoryKeys
    );
  }

  if (searchPlan.matchedFamilyKeys.length > 0) {
    pushClause(
      `${familyAlias}.family_key IN (${searchPlan.matchedFamilyKeys.map(() => '?').join(', ')})`,
      searchPlan.matchedFamilyKeys
    );
  }

  searchPlan.sqlTokenPatterns.forEach((token) => {
    const tokenPattern = `%${token}%`;

    pushClause(`LOWER(${serviceAlias}.service_title) LIKE LOWER(?)`, [tokenPattern]);
    pushClause(
      `EXISTS (
        SELECT 1
        FROM service_tags st
        WHERE st.service_id = ${serviceAlias}.id
          AND LOWER(st.tag) LIKE LOWER(?)
      )`,
      [tokenPattern]
    );
    pushClause(`LOWER(${userAlias}.first_name) LIKE LOWER(?)`, [`${token}%`]);
    pushClause(`LOWER(${userAlias}.surname) LIKE LOWER(?)`, [`${token}%`]);
    pushClause(`LOWER(${userAlias}.username) LIKE LOWER(?)`, [`${token}%`]);

    if (token.length >= 5) {
      pushClause(`LOWER(${serviceAlias}.description) LIKE LOWER(?)`, [tokenPattern]);
    }
  });

  const fallbackPrefixes = uniqueValues(
    searchPlan.sqlTokenPatterns
      .filter((token) => token.length >= 5)
      .map((token) => token.slice(0, 4))
  ).slice(0, 3);

  fallbackPrefixes.forEach((prefix) => {
    const prefixPattern = `%${prefix}%`;
    pushClause(`LOWER(${serviceAlias}.service_title) LIKE LOWER(?)`, [prefixPattern]);
    pushClause(
      `EXISTS (
        SELECT 1
        FROM service_tags st
        WHERE st.service_id = ${serviceAlias}.id
          AND LOWER(st.tag) LIKE LOWER(?)
      )`,
      [prefixPattern]
    );
    pushClause(`LOWER(${userAlias}.first_name) LIKE LOWER(?)`, [`${prefix}%`]);
    pushClause(`LOWER(${userAlias}.username) LIKE LOWER(?)`, [`${prefix}%`]);
  });

  return {
    sql: clauses.length > 0 ? ` AND (${clauses.join(' OR ')})` : '',
    params,
  };
}

function parseJsonArraySafe(value) {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  }

  return [];
}

function buildProfessionalSearchField(row) {
  const firstName = String(row?.first_name || '').trim();
  const surname = String(row?.surname || '').trim();
  const username = String(row?.username || '').trim();
  const displayName = [firstName, surname].filter(Boolean).join(' ').trim();

  return {
    values: uniqueValues([firstName, surname, displayName, username]),
    displayName: displayName || username,
  };
}

function getConceptIndex(store, key) {
  return store.get(String(key || '').trim())?.index || null;
}

function annotateServiceSearchCandidate(searchPlan, row) {
  if (!searchPlan?.hasMeaningfulSearch) {
    return {
      ...row,
      search_score: 0,
      search_tier: 0,
      _searchMeta: null,
    };
  }

  const tags = parseJsonArraySafe(row?.tags);
  const professionalField = buildProfessionalSearchField(row);
  const titleResult = scorePlanAgainstField(searchPlan, createFieldIndex(row?.service_title), FIELD_WEIGHTS.title);
  const professionalResult = scorePlanAgainstField(searchPlan, createFieldIndex(professionalField.values), FIELD_WEIGHTS.professional);
  const tagsResult = scorePlanAgainstField(searchPlan, createFieldIndex(tags), FIELD_WEIGHTS.tags);
  const categoryResult = scorePlanAgainstField(searchPlan, getConceptIndex(CATEGORY_CONCEPTS, row?.category_key), FIELD_WEIGHTS.category);
  const familyResult = scorePlanAgainstField(searchPlan, getConceptIndex(FAMILY_CONCEPTS, row?.family_key), FIELD_WEIGHTS.family);
  const descriptionResult = scorePlanAgainstField(searchPlan, createFieldIndex(row?.description, { maxChars: 420 }), FIELD_WEIGHTS.description);

  const matchedTokenIndexes = new Set([
    ...titleResult.matchedTokenIndexes,
    ...professionalResult.matchedTokenIndexes,
    ...tagsResult.matchedTokenIndexes,
    ...categoryResult.matchedTokenIndexes,
    ...familyResult.matchedTokenIndexes,
    ...descriptionResult.matchedTokenIndexes,
  ]);

  const matchedTokensCount = matchedTokenIndexes.size;
  const tokenCount = searchPlan.tokenProfiles.length;
  const coverage = tokenCount > 0 ? matchedTokensCount / tokenCount : 1;
  const hasStrongPrimarySignal = (
    titleResult.score >= 18
    || professionalResult.score >= 20
    || tagsResult.score >= 16
    || categoryResult.score >= 18
  );
  const hasPhraseMatch = (
    titleResult.hasPhraseMatch
    || professionalResult.hasPhraseMatch
    || tagsResult.hasPhraseMatch
    || categoryResult.hasPhraseMatch
    || familyResult.hasPhraseMatch
  );
  const strongFieldHits = [
    titleResult.score >= 18,
    professionalResult.score >= 20,
    tagsResult.score >= 16,
    categoryResult.score >= 18,
    familyResult.score >= 14,
  ].filter(Boolean).length;

  let totalScore = 0;
  totalScore += titleResult.score;
  totalScore += professionalResult.score;
  totalScore += tagsResult.score;
  totalScore += categoryResult.score;
  totalScore += familyResult.score * 0.82;
  totalScore += descriptionResult.score;
  totalScore += coverage * 22;

  if (strongFieldHits >= 2) {
    totalScore += Math.min(18, (strongFieldHits - 1) * 5);
  }

  if (tokenCount >= 2 && matchedTokensCount === 1 && !hasPhraseMatch) {
    totalScore *= 0.58;
  }

  const minimumScore = tokenCount <= 1 ? 24 : 36;
  let isMatch = totalScore >= minimumScore;

  if (isMatch && tokenCount >= 2 && coverage < 0.5 && !hasPhraseMatch && !hasStrongPrimarySignal) {
    isMatch = false;
  }

  if (isMatch && tokenCount >= 2 && coverage <= 0.5 && strongFieldHits < 2 && !hasPhraseMatch) {
    isMatch = false;
  }

  if (isMatch && !hasStrongPrimarySignal && descriptionResult.score > 0 && totalScore < minimumScore + 12) {
    isMatch = false;
  }

  if (isMatch && strongFieldHits === 0 && coverage < 1) {
    isMatch = false;
  }

  const roundedScore = Math.round(totalScore * 100) / 100;
  const searchTier = !isMatch
    ? 0
    : roundedScore >= 145 || titleResult.hasPhraseMatch || professionalResult.hasPhraseMatch
      ? 4
      : roundedScore >= 92
        ? 3
        : roundedScore >= 58
          ? 2
          : 1;

  return {
    ...row,
    tags,
    search_score: roundedScore,
    search_tier: searchTier,
    _searchMeta: {
      matchedTokensCount,
      coverage,
      hasStrongPrimarySignal,
      title: titleResult,
      professional: professionalResult,
      tags: tagsResult,
      category: categoryResult,
      family: familyResult,
      description: descriptionResult,
      professionalDisplayName: professionalField.displayName,
    },
  };
}

function roundSearchPrice(value, digits) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return 0;
  const factor = 10 ** digits;
  return Math.round(numericValue * factor) / factor;
}

function getComparablePrice(service, durationMinutes) {
  if (service?.price === null || service?.price === undefined || service?.price === '') {
    return null;
  }

  const numericPrice = Number(service?.price);
  if (!Number.isFinite(numericPrice)) {
    return null;
  }

  let basePrice = null;
  if (service?.price_type === 'fix') {
    basePrice = numericPrice;
  } else if (service?.price_type === 'hour') {
    const safeDurationMinutes = Number.isFinite(Number(durationMinutes)) && Number(durationMinutes) > 0
      ? Number(durationMinutes)
      : 60;
    basePrice = numericPrice * (safeDurationMinutes / 60);
  } else {
    return null;
  }

  const roundedBasePrice = roundSearchPrice(basePrice, 2);
  const commission = Math.max(1, roundSearchPrice(roundedBasePrice * 0.1, 1));
  return roundSearchPrice(roundedBasePrice + commission, 2);
}

function getBayesianRating(service) {
  const reviewCount = Math.max(0, Number(service?.review_count || 0));
  const averageRating = Math.max(0, Number(service?.average_rating || 0));
  const priorCount = 5;
  const priorMean = 4.0;

  return (
    (reviewCount / (reviewCount + priorCount)) * averageRating
    + (priorCount / (reviewCount + priorCount)) * priorMean
  );
}

function getRecommendQualityScore(service) {
  const reviewCount = Math.max(0, Number(service?.review_count || 0));
  const bookingCount = Math.max(0, Number(service?.booking_count || 0));
  const repeatedBookingsCount = Math.max(0, Number(service?.repeated_bookings_count || 0));
  const likesCount = Math.max(0, Number(service?.likes_count || 0));
  const actionRate = Number(service?.action_rate);
  const distanceKm = Number(service?.distance_km);

  const logScore = (value, maxBase, weight) => {
    if (value <= 0) return 0;
    return Math.min(Math.log10(value + 1) / Math.log10(maxBase), 1) * weight;
  };

  const distanceBoost = Number.isFinite(distanceKm)
    ? Math.max(0, 1 - Math.min(distanceKm, 50) / 50) * 14
    : 0;

  return (
    ((getBayesianRating(service) / 5) * 32)
    + logScore(reviewCount, 31, 10)
    + logScore(bookingCount, 51, 18)
    + logScore(repeatedBookingsCount, 11, 10)
    + (
      Number.isFinite(actionRate)
        ? Math.max(0, 1 - Math.min(actionRate, 1440) / 1440) * 10
        : 6
    )
    + (Number(service?.is_verified) === 1 ? 8 : 0)
    + logScore(likesCount, 21, 6)
    + distanceBoost
  );
}

function getNoveltyBoost(service) {
  const createdAt = Date.parse(service?.service_created_datetime);
  if (!Number.isFinite(createdAt)) {
    return 1;
  }

  const daysSinceCreation = (Date.now() - createdAt) / (24 * 60 * 60 * 1000);
  if (daysSinceCreation < 0 || daysSinceCreation > 21) {
    return 1;
  }

  return 1 + ((21 - daysSinceCreation) / 21) * 0.15;
}

function compareDatesDescending(left, right) {
  const leftDate = Date.parse(left?.service_created_datetime) || 0;
  const rightDate = Date.parse(right?.service_created_datetime) || 0;
  return rightDate - leftDate;
}

function compareSearchTier(left, right) {
  const tierDiff = Number(right?.search_tier || 0) - Number(left?.search_tier || 0);
  if (tierDiff !== 0) return tierDiff;
  return Number(right?.search_score || 0) - Number(left?.search_score || 0);
}

function sortRankedServiceSearchCandidates(rows, {
  orderBy = 'recommend',
  durationMinutes = null,
} = {}) {
  return [...rows].sort((left, right) => {
    const tierDiff = compareSearchTier(left, right);
    if (orderBy !== 'recommend' && tierDiff !== 0) {
      return tierDiff;
    }

    switch (orderBy) {
      case 'cheapest': {
        const leftBudgetPenalty = left?.price_type === 'budget' ? 1 : 0;
        const rightBudgetPenalty = right?.price_type === 'budget' ? 1 : 0;
        if (leftBudgetPenalty !== rightBudgetPenalty) {
          return leftBudgetPenalty - rightBudgetPenalty;
        }

        const leftPrice = getComparablePrice(left, durationMinutes);
        const rightPrice = getComparablePrice(right, durationMinutes);
        if (leftPrice === null && rightPrice !== null) return 1;
        if (leftPrice !== null && rightPrice === null) return -1;
        if (leftPrice !== null && rightPrice !== null && leftPrice !== rightPrice) {
          return leftPrice - rightPrice;
        }
        return compareDatesDescending(left, right);
      }
      case 'mostexpensive': {
        const leftBudgetPenalty = left?.price_type === 'budget' ? 1 : 0;
        const rightBudgetPenalty = right?.price_type === 'budget' ? 1 : 0;
        if (leftBudgetPenalty !== rightBudgetPenalty) {
          return leftBudgetPenalty - rightBudgetPenalty;
        }

        const leftPrice = getComparablePrice(left, durationMinutes);
        const rightPrice = getComparablePrice(right, durationMinutes);
        if (leftPrice === null && rightPrice !== null) return 1;
        if (leftPrice !== null && rightPrice === null) return -1;
        if (leftPrice !== null && rightPrice !== null && leftPrice !== rightPrice) {
          return rightPrice - leftPrice;
        }
        return compareDatesDescending(left, right);
      }
      case 'bestrated': {
        const leftBayesian = getBayesianRating(left);
        const rightBayesian = getBayesianRating(right);
        if (leftBayesian !== rightBayesian) {
          return rightBayesian - leftBayesian;
        }
        const reviewCountDiff = Number(right?.review_count || 0) - Number(left?.review_count || 0);
        if (reviewCountDiff !== 0) return reviewCountDiff;
        const averageRatingDiff = Number(right?.average_rating || 0) - Number(left?.average_rating || 0);
        if (averageRatingDiff !== 0) return averageRatingDiff;
        return compareDatesDescending(left, right);
      }
      case 'nearest': {
        const leftDistance = Number(left?.distance_km);
        const rightDistance = Number(right?.distance_km);
        const leftHasDistance = Number.isFinite(leftDistance);
        const rightHasDistance = Number.isFinite(rightDistance);

        if (leftHasDistance !== rightHasDistance) {
          return leftHasDistance ? -1 : 1;
        }

        if (leftHasDistance && rightHasDistance) {
          const leftAdjustedDistance = Number(left?.user_can_consult) === 1 ? leftDistance + 1000 : leftDistance;
          const rightAdjustedDistance = Number(right?.user_can_consult) === 1 ? rightDistance + 1000 : rightDistance;
          if (leftAdjustedDistance !== rightAdjustedDistance) {
            return leftAdjustedDistance - rightAdjustedDistance;
          }
          if (leftDistance !== rightDistance) {
            return leftDistance - rightDistance;
          }
        }

        const averageRatingDiff = Number(right?.average_rating || 0) - Number(left?.average_rating || 0);
        if (averageRatingDiff !== 0) return averageRatingDiff;
        return compareDatesDescending(left, right);
      }
      case 'availability': {
        const leftActionRate = Number(left?.action_rate);
        const rightActionRate = Number(right?.action_rate);
        const leftHasActionRate = Number.isFinite(leftActionRate);
        const rightHasActionRate = Number.isFinite(rightActionRate);

        if (leftHasActionRate !== rightHasActionRate) {
          return leftHasActionRate ? -1 : 1;
        }

        if (leftHasActionRate && rightHasActionRate && leftActionRate !== rightActionRate) {
          return leftActionRate - rightActionRate;
        }

        return compareDatesDescending(left, right);
      }
      case 'recommend':
      default: {
        const leftScore = ((Number(left?.search_score || 0) * 1.35) + getRecommendQualityScore(left)) * getNoveltyBoost(left);
        const rightScore = ((Number(right?.search_score || 0) * 1.35) + getRecommendQualityScore(right)) * getNoveltyBoost(right);
        if (leftScore !== rightScore) {
          return rightScore - leftScore;
        }
        const leftBayesian = getBayesianRating(left);
        const rightBayesian = getBayesianRating(right);
        if (leftBayesian !== rightBayesian) {
          return rightBayesian - leftBayesian;
        }
        const bookingDiff = Number(right?.booking_count || 0) - Number(left?.booking_count || 0);
        if (bookingDiff !== 0) return bookingDiff;
        return compareDatesDescending(left, right);
      }
    }
  });
}

function rankServiceSearchCandidates(searchPlan, rows, options = {}) {
  const annotatedRows = (Array.isArray(rows) ? rows : [])
    .map((row) => annotateServiceSearchCandidate(searchPlan, row))
    .filter((row) => Number(row?.search_tier || 0) > 0);

  return sortRankedServiceSearchCandidates(annotatedRows, options);
}

function getMatchingTagSuggestions(searchPlan, tags) {
  const results = [];
  const normalizedQuery = searchPlan?.normalizedQuery || '';

  parseJsonArraySafe(tags).forEach((tag) => {
    const normalizedTag = normalizeServiceSearchText(tag);
    if (!normalizedTag) return;

    if (normalizedQuery.length >= 3 && normalizedTag.includes(normalizedQuery)) {
      results.push(String(tag).trim());
      return;
    }

    const tagTokens = tokenizeText(normalizedTag);
    const hasTokenMatch = searchPlan.tokenProfiles.some((tokenProfile) => (
      tagTokens.some((tagToken) => compareQueryTokenToFieldToken(tokenProfile, tagToken))
    ));

    if (hasTokenMatch) {
      results.push(String(tag).trim());
    }
  });

  return uniqueValues(results);
}

function buildServiceSearchSuggestions(searchPlan, rankedRows, limit = 8) {
  const seen = new Set();
  const suggestions = [];

  const pushSuggestion = (key, suggestion, score) => {
    if (!key || seen.has(key) || suggestions.length >= limit) {
      return;
    }
    seen.add(key);
    suggestions.push({
      score,
      suggestion,
    });
  };

  rankedRows.slice(0, 30).forEach((row) => {
    const meta = row?._searchMeta || {};
    const professionalName = String(meta.professionalDisplayName || '').trim();

    if (professionalName && Number(meta.professional?.score || 0) >= 40) {
      pushSuggestion(
        `professional:${normalizeServiceSearchText(professionalName)}`,
        {
          name: professionalName,
          query: professionalName,
          suggestion_type: 'professional',
        },
        Number(meta.professional.score) + Number(row.search_score || 0) * 0.1
      );
    }

    if (row?.service_title && Number(meta.title?.score || 0) >= 30) {
      pushSuggestion(
        `service:${normalizeServiceSearchText(row.service_title)}`,
        { service_title: row.service_title },
        Number(meta.title.score) + Number(row.search_score || 0) * 0.05
      );
    }

    if (Number.isFinite(Number(row?.service_category_id)) && Number(meta.category?.score || 0) >= 36) {
      pushSuggestion(
        `category:${Number(row.service_category_id)}`,
        {
          suggestion_type: 'category',
          service_category_name: row.category_key,
          category_key: row.category_key,
          service_category_id: Number(row.service_category_id),
          service_family_id: Number(row.service_family_id),
          service_family_name: row.family_key,
          family_key: row.family_key,
        },
        Number(meta.category.score) + Number(row.search_score || 0) * 0.04
      );
    }

    if (Number.isFinite(Number(row?.service_family_id)) && Number(meta.family?.score || 0) >= 32) {
      pushSuggestion(
        `family:${Number(row.service_family_id)}`,
        {
          suggestion_type: 'family',
          service_family: row.family_key,
          service_family_name: row.family_key,
          family_key: row.family_key,
          service_family_id: Number(row.service_family_id),
        },
        Number(meta.family.score) + Number(row.search_score || 0) * 0.03
      );
    }

    getMatchingTagSuggestions(searchPlan, row?.tags).forEach((tag) => {
      pushSuggestion(
        `tag:${normalizeServiceSearchText(tag)}`,
        { tag },
        24 + Number(row.search_score || 0) * 0.02
      );
    });
  });

  return suggestions
    .sort((left, right) => right.score - left.score)
    .slice(0, limit)
    .map((item) => item.suggestion);
}

function buildServiceSearchRecommendedTaxonomy(rankedRows, {
  limit = 8,
  selectedCategoryId = null,
} = {}) {
  const categoryMap = new Map();
  const familyMap = new Map();

  rankedRows.slice(0, 250).forEach((row) => {
    const rowScore = Number(row?.search_score || 0);
    const meta = row?._searchMeta || {};

    const categoryId = Number(row?.service_category_id);
    if (Number.isFinite(categoryId)) {
      const previous = categoryMap.get(categoryId) || {
        suggestion_type: 'category',
        service_category_id: categoryId,
        service_family_id: Number(row?.service_family_id),
        service_category_name: row?.category_key || row?.service_category_name || '',
        category_key: row?.category_key || row?.service_category_name || '',
        service_family_name: row?.family_key || row?.service_family_name || '',
        family_key: row?.family_key || row?.service_family_name || '',
        matching_services: 0,
        weighted_score: 0,
        best_score: 0,
      };
      previous.matching_services += 1;
      previous.weighted_score += rowScore + Number(meta.category?.score || 0) * 0.3 + Number(meta.title?.score || 0) * 0.12;
      previous.best_score = Math.max(previous.best_score, rowScore);
      categoryMap.set(categoryId, previous);
    }

    const familyId = Number(row?.service_family_id);
    if (Number.isFinite(familyId)) {
      const previous = familyMap.get(familyId) || {
        suggestion_type: 'family',
        service_family_id: familyId,
        service_family_name: row?.family_key || row?.service_family_name || '',
        family_key: row?.family_key || row?.service_family_name || '',
        matching_services: 0,
        weighted_score: 0,
        best_score: 0,
      };
      previous.matching_services += 1;
      previous.weighted_score += (rowScore * 0.82) + Number(meta.family?.score || 0) * 0.28;
      previous.best_score = Math.max(previous.best_score, rowScore);
      familyMap.set(familyId, previous);
    }
  });

  const merged = [
    ...Array.from(categoryMap.values()),
    ...Array.from(familyMap.values()),
  ];

  return merged
    .sort((left, right) => {
      const leftSelected = left.suggestion_type === 'category' && Number(left.service_category_id) === Number(selectedCategoryId) ? 1 : 0;
      const rightSelected = right.suggestion_type === 'category' && Number(right.service_category_id) === Number(selectedCategoryId) ? 1 : 0;
      if (leftSelected !== rightSelected) {
        return rightSelected - leftSelected;
      }

      const scoreDiff = Number(right.weighted_score || 0) - Number(left.weighted_score || 0);
      if (scoreDiff !== 0) return scoreDiff;

      const typeDiff = (left.suggestion_type === 'category' ? 1 : 0) - (right.suggestion_type === 'category' ? 1 : 0);
      if (typeDiff !== 0) return -typeDiff;

      const matchDiff = Number(right.matching_services || 0) - Number(left.matching_services || 0);
      if (matchDiff !== 0) return matchDiff;

      const leftLabel = String(left.category_key || left.family_key || '');
      const rightLabel = String(right.category_key || right.family_key || '');
      return leftLabel.localeCompare(rightLabel);
    })
    .slice(0, limit)
    .map(({ weighted_score, best_score, ...item }) => item);
}

module.exports = {
  buildServiceSearchPlan,
  buildServiceSearchCandidateClause,
  buildServiceSearchSuggestions,
  buildServiceSearchRecommendedTaxonomy,
  normalizeServiceSearchText,
  rankServiceSearchCandidates,
};
