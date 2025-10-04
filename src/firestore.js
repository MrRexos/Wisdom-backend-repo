const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');

let firestoreInstance = null;
let initializationAttempted = false;

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
    if (!admin.apps.length) {
      if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        const parsed = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        admin.initializeApp({
          credential: admin.credential.cert(parsed),
        });
      } else if (process.env.FIREBASE_SERVICE_ACCOUNT_PATH) {
        const serviceAccount = readServiceAccountFromPath(process.env.FIREBASE_SERVICE_ACCOUNT_PATH);
        if (!serviceAccount) {
          return null;
        }
        admin.initializeApp({
          credential: admin.credential.cert(serviceAccount),
        });
      } else {
        admin.initializeApp({
          credential: admin.credential.applicationDefault(),
        });
      }
    }

    firestoreInstance = admin.firestore();
    return firestoreInstance;
  } catch (error) {
    console.error('Unable to initialize Firebase Admin SDK:', error.message);
    return null;
  }
}

function getFirestore() {
  if (firestoreInstance) return firestoreInstance;
  return initializeFirebase();
}

module.exports = {
  getFirestore,
};
