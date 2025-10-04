const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');

let firestoreInstance = null;
let initializationAttempted = false;

const METRICS_DEBUG = (() => {
  const v = String(process.env.METRICS_DEBUG || '').toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'debug';
})();

function dbg() {
  if (!METRICS_DEBUG) return;
  try {
    // eslint-disable-next-line prefer-rest-params
    console.log.apply(console, ['[METRICS][Firestore]'].concat(Array.prototype.slice.call(arguments)));
  } catch (_) { }
}

function readServiceAccountFromPath(filePath) {
  try {
    const absolutePath = path.isAbsolute(filePath)
      ? filePath
      : path.join(process.cwd(), filePath);
    const raw = fs.readFileSync(absolutePath, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    console.error('Failed to read Firebase service account file:', error.message);
    return null;
  }
}

function initializeFirebase() {
  if (firestoreInstance) return firestoreInstance;
  if (initializationAttempted) return null;

  initializationAttempted = true;

  try {
    dbg('initializeFirebase: starting');
    if (!admin.apps.length) {
      if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        dbg('initializeFirebase: using FIREBASE_SERVICE_ACCOUNT JSON from env');
        const parsed = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        admin.initializeApp({
          credential: admin.credential.cert(parsed),
        });
      } else if (process.env.FIREBASE_SERVICE_ACCOUNT_PATH) {
        dbg('initializeFirebase: using FIREBASE_SERVICE_ACCOUNT_PATH', process.env.FIREBASE_SERVICE_ACCOUNT_PATH);
        const serviceAccount = readServiceAccountFromPath(process.env.FIREBASE_SERVICE_ACCOUNT_PATH);
        if (!serviceAccount) {
          dbg('initializeFirebase: failed to read service account file');
          return null;
        }
        admin.initializeApp({
          credential: admin.credential.cert(serviceAccount),
        });
      } else {
        dbg('initializeFirebase: using applicationDefault credentials');
        admin.initializeApp({
          credential: admin.credential.applicationDefault(),
        });
      }
    }

    firestoreInstance = admin.firestore();
    dbg('initializeFirebase: Firestore instance created');
    return firestoreInstance;
  } catch (error) {
    console.error('Unable to initialize Firebase Admin SDK:', error.message);
    return null;
  }
}

function getFirestore() {
  if (firestoreInstance) {
    dbg('getFirestore: reusing cached instance');
    return firestoreInstance;
  }
  return initializeFirebase();
}

module.exports = {
  getFirestore,
};
