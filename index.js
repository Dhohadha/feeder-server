const express = require("express");
const cors = require("cors");
const mqtt = require("mqtt");
const admin = require("firebase-admin");

// ---------------- Firebase admin ----------------

const serviceAccount = JSON.parse(
  process.env.FIREBASE_SERVICE_ACCOUNT
);


admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

// ---------------- MQTT ----------------

const mqttClient = mqtt.connect("mqtt://broker.emqx.io:1883");

mqttClient.on("connect", () => {
  console.log("MQTT connected");
});

// ---------------- App ----------------

const app = express();
app.use(cors());
app.use(express.json());

// ---------------- helpers ----------------

function gramsToSeconds(grams) {
  return (grams / 1000) * 30;
}

function publishRelay(deviceID, state) {
  const payload = JSON.stringify({
    deviceID: deviceID,
    relay1: state,
    relay2: state, // only for format
  });

  mqttClient.publish("PMS/cmd", payload);
  console.log("Published:", payload);
}

// ---------------- Scheduler engine (robust, timezone-aware) ----------------

let lastTriggeredKey = new Set();

// returns current time in "HH:mm" for Asia/Kolkata
function currentTimeStringKolkata() {
  // build a Date string in IST using toLocaleString with the timezone
  const nowParts = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Kolkata' });
  // nowParts looks like "dd/mm/yyyy, HH:MM:SS"
  const parts = nowParts.split(',').map(s => s.trim());
  const timePart = parts[1] || '';
  const hm = timePart.split(':');
  if (hm.length < 2) return '';
  const h = hm[0].padStart(2, '0');
  const m = hm[1].padStart(2, '0');
  return `${h}:${m}`;
}

async function runSchedulerTick() {
  try {
    const nowKolkataStr = currentTimeStringKolkata();

    // debug log so you can see tick times and that scheduler runs
    console.log(`[scheduler tick] now(IST)=${nowKolkataStr}  local=${new Date().toISOString()}`);

    // load all enabled schedules and check in code (works for small scale, 20-50 devices)
    const snap = await db.collection("schedules").where("enabled", "==", true).get();

    console.log(`[scheduler] enabled schedules loaded: ${snap.size}`);

    for (const doc of snap.docs) {
      const data = doc.data();

      // validate fields
      if (!data || !data.time || !data.deviceID || !data.grams) continue;

      // ensure time formatted as HH:mm
      const scheduleTime = (data.time || '').toString().padLeft?.call ? data.time : String(data.time);
      // create a key for dedupe
      const key = `${doc.id}-${nowKolkataStr}`;

      // If this schedule matches current IST minute -> trigger
      if (scheduleTime === nowKolkataStr) {

        if (lastTriggeredKey.has(key)) {
          // already triggered this minute
          continue;
        }

        lastTriggeredKey.add(key);

        const deviceID = data.deviceID;
        const grams = Number(data.grams);
        const seconds = gramsToSeconds(grams);

        console.log(`Scheduled feed -> ${deviceID} at ${nowKolkataStr} for ${seconds}s (doc=${doc.id})`);

        // ON
        publishRelay(deviceID, 1);

        // log
        await db.collection("feed_logs").add({
          deviceID,
          grams,
          startedAt: admin.firestore.FieldValue.serverTimestamp(),
          mode: "schedule",
          scheduleId: doc.id,
        });

        // OFF after required seconds
        setTimeout(() => {
          publishRelay(deviceID, 0);
        }, Math.round(seconds * 1000));
      }
    }

    // clean old keys (keep only current minute keys)
    for (const k of Array.from(lastTriggeredKey)) {
      if (!k.endsWith(nowKolkataStr)) {
        lastTriggeredKey.delete(k);
      }
    }
  } catch (err) {
    console.error("Scheduler error", err);
  }
}

// ---------------- Log cleanup ----------------

async function cleanupOldLogs() {
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 7);

    const snap = await db
      .collection("feed_logs")
      .where(
        "startedAt",
        "<",
        admin.firestore.Timestamp.fromDate(cutoff)
      )
      .get();

    if (snap.empty) return;

    const batch = db.batch();

    snap.docs.forEach((doc) => {
      batch.delete(doc.ref);
    });

    await batch.commit();

    console.log(`Cleaned ${snap.size} old feed logs`);
  } catch (err) {
    console.error("Log cleanup error", err);
  }
}

// ---------------- API ----------------

app.post("/dispense-now", async (req, res) => {
  try {
    const { deviceID, grams } = req.body;

    if (!deviceID || !grams) {
      return res.status(400).json({ error: "deviceID and grams required" });
    }

    const seconds = gramsToSeconds(Number(grams));

    // turn ON
    publishRelay(deviceID, 1);

    // optional log
    await db.collection("feed_logs").add({
      deviceID,
      grams,
      startedAt: admin.firestore.FieldValue.serverTimestamp(),
      mode: "manual",
    });

    // turn OFF after time
    setTimeout(() => {
      publishRelay(deviceID, 0);
    }, Math.round(seconds * 1000));

    res.json({
      status: "ok",
      runSeconds: Math.round(seconds),
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "server error" });
  }
});

// ---------------- Health endpoint ----------------

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    mqtt: mqttClient.connected,
  });
});

// ---------------- start ----------------

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});

// scheduler every 30 seconds
setInterval(runSchedulerTick, 30 * 1000);

// cleanup logs every 6 hours
setInterval(cleanupOldLogs, 6 * 60 * 60 * 1000);

// ---------------- graceful shutdown ----------------

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

function shutdown() {
  console.log("Shutting down server...");

  try {
    mqttClient.end(true);
  } catch (e) {}

  process.exit(0);
}
