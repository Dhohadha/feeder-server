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

// ---------------- Scheduler engine ----------------

let lastTriggeredKey = new Set();

function currentTimeString() {

  const now = new Date();

  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).formatToParts(now);

  const h = parts.find(p => p.type === "hour").value;
  const m = parts.find(p => p.type === "minute").value;

  return `${h}:${m}`;
}


async function runSchedulerTick() {
  try {
    const nowStr = currentTimeString();

    const snap = await db
      .collection("schedules")
      .where("enabled", "==", true)
      .where("time", "==", nowStr)
      .get();

    for (const doc of snap.docs) {
      const data = doc.data();

      const key = `${doc.id}-${nowStr}`;

      // prevent multiple triggers in same minute
      if (lastTriggeredKey.has(key)) continue;

      lastTriggeredKey.add(key);

      const deviceID = data.deviceID;
      const grams = data.grams;

      const seconds = gramsToSeconds(Number(grams));

      console.log(
        `Scheduled feed -> ${deviceID} at ${nowStr} for ${seconds}s`
      );

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

      // OFF
      setTimeout(() => {
        publishRelay(deviceID, 0);
      }, Math.round(seconds * 1000));
    }

    // clean old keys (keep only current minute keys)
    for (const k of lastTriggeredKey) {
      if (!k.endsWith(nowStr)) {
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
