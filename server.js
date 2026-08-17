const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const ROOM = "main";
const MAX_USERS = 2;
const RECONNECT_GRACE = 30000;

const clients = new Map();

function newId() {
  return crypto.randomUUID();
}

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function connectedClients() {
  return [...clients.values()].filter(c => c.ws);
}

function getPeer(id) {
  return connectedClients().find(c => c.id !== id);
}

function iceServers() {
  const servers = [
    {
      urls: [
        "stun:stun.l.google.com:19302",
        "stun:stun1.l.google.com:19302"
      ]
    }
  ];

  if (
    process.env.TURN_URLS &&
    process.env.TURN_USERNAME &&
    process.env.TURN_CREDENTIAL
  ) {
    servers.push({
      urls: process.env.TURN_URLS
        .split(",")
        .map(x => x.trim()),
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL
    });
  }

  return servers;
}

function cleanup() {
  const now = Date.now();

  for (const [id, client] of clients) {
    if (
      !client.ws &&
      client.disconnectedAt &&
      now - client.disconnectedAt > RECONNECT_GRACE
    ) {
      clients.delete(id);
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/ice-config") {
    res.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    });

    res.end(JSON.stringify({
      iceServers: iceServers()
    }));

    return;
  }

  if (req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(JSON.stringify({
      ok: true,
      room: ROOM,
      users: connectedClients().length,
      maxUsers: MAX_USERS
    }));

    return;
  }

  const file = path.join(__dirname, "index.html");

  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(500);
      res.end("index.html not found.");
      return;
    }

    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8"
    });

    res.end(data);
  });
});

const wss = new WebSocket.Server({ server });

wss.on("connection", ws => {
  let clientId = null;

  ws.on("message", raw => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // ============================
    // JOIN ONE ROOM ONLY
    // ============================

    if (msg.type === "join") {
      cleanup();

      const requestedId =
        typeof msg.clientId === "string"
          ? msg.clientId
          : null;

      // Reconnect existing client
      if (requestedId && clients.has(requestedId)) {
        const existing = clients.get(requestedId);

        existing.ws = ws;
        existing.disconnectedAt = null;

        clientId = requestedId;
        ws.clientId = clientId;

        const peer = getPeer(clientId);

        send(ws, {
          type: "joined",
          room: ROOM,
          clientId,
          role: existing.role,
          peerPresent: !!peer
        });

        if (peer) {
          send(peer.ws, {
            type: "peer-reconnected"
          });
        }

        return;
      }

      // Only two users
      if (connectedClients().length >= MAX_USERS) {
        send(ws, {
          type: "room-full"
        });

        return;
      }

      clientId = newId();

      const role =
        connectedClients().length === 0
          ? "host"
          : "guest";

      clients.set(clientId, {
        id: clientId,
        ws,
        role,
        disconnectedAt: null
      });

      ws.clientId = clientId;

      const peer = getPeer(clientId);

      send(ws, {
        type: "joined",
        room: ROOM,
        clientId,
        role,
        peerPresent: !!peer
      });

      if (peer) {
        send(peer.ws, {
          type: "peer-joined"
        });
      }

      return;
    }

    // ============================
    // WEBRTC SIGNALING
    // ============================

    if (
      msg.type === "offer" ||
      msg.type === "answer" ||
      msg.type === "candidate"
    ) {
      if (!clientId) return;

      const peer = getPeer(clientId);

      if (!peer) return;

      send(peer.ws, {
        type: msg.type,
        ...(msg.type === "offer"
          ? { offer: msg.offer }
          : {}),
        ...(msg.type === "answer"
          ? { answer: msg.answer }
          : {}),
        ...(msg.type === "candidate"
          ? { candidate: msg.candidate }
          : {})
      });
    }
  });

  ws.on("close", () => {
    if (!clientId) return;

    const client = clients.get(clientId);

    if (!client || client.ws !== ws) return;

    client.ws = null;
    client.disconnectedAt = Date.now();

    const peer = getPeer(clientId);

    if (peer) {
      send(peer.ws, {
        type: "peer-disconnected"
      });
    }

    setTimeout(cleanup, RECONNECT_GRACE + 100);
  });
});

setInterval(cleanup, 5000);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Room: ${ROOM}`);
  console.log(`Maximum users: ${MAX_USERS}`);
  console.log(
    `TURN enabled: ${
      !!(
        process.env.TURN_URLS &&
        process.env.TURN_USERNAME &&
        process.env.TURN_CREDENTIAL
      )
    }`
  );
});
