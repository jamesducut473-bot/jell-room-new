const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const ROOM_ID = "main";
const MAX_USERS = 2;

const clients = new Map();

function createClientId() {
  return crypto.randomUUID();
}

function send(ws, message) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function getConnectedClients() {
  return [...clients.values()].filter(
    client => client.ws && client.ws.readyState === WebSocket.OPEN
  );
}

function getOtherClient(clientId) {
  return getConnectedClients().find(
    client => client.id !== clientId
  );
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(JSON.stringify({
      ok: true,
      room: ROOM_ID,
      users: getConnectedClients().length,
      maxUsers: MAX_USERS
    }));

    return;
  }

  if (req.url === "/ice-config") {
    const iceServers = [
      {
        urls: [
          "stun:stun.l.google.com:19302",
          "stun:stun1.l.google.com:19302"
        ]
      }
    ];

    // Optional TURN server.
    if (
      process.env.TURN_URL &&
      process.env.TURN_USERNAME &&
      process.env.TURN_PASSWORD
    ) {
      iceServers.push({
        urls: process.env.TURN_URL,
        username: process.env.TURN_USERNAME,
        credential: process.env.TURN_PASSWORD
      });
    }

    res.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    });

    res.end(JSON.stringify({ iceServers }));
    return;
  }

  const filePath = path.join(__dirname, "index.html");

  fs.readFile(filePath, (error, data) => {
    if (error) {
      res.writeHead(500, {
        "Content-Type": "text/plain"
      });

      res.end("index.html could not be loaded.");
      return;
    }

    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8"
    });

    res.end(data);
  });
});

const wss = new WebSocket.Server({
  server
});

wss.on("connection", ws => {
  let clientId = null;

  ws.on("message", raw => {
    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      return;
    }

    /*
     * ONE ROOM ONLY
     */
    if (message.type === "join") {
      const requestedId =
        typeof message.clientId === "string"
          ? message.clientId
          : null;

      /*
       * Reconnect existing client.
       */
      if (requestedId && clients.has(requestedId)) {
        const existing = clients.get(requestedId);

        existing.ws = ws;
        existing.disconnected = false;

        clientId = requestedId;
        ws.clientId = clientId;

        const peer = getOtherClient(clientId);

        send(ws, {
          type: "joined",
          room: ROOM_ID,
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

      /*
       * Only two people are allowed.
       */
      if (getConnectedClients().length >= MAX_USERS) {
        send(ws, {
          type: "room-full"
        });

        return;
      }

      clientId = createClientId();

      const role =
        getConnectedClients().length === 0
          ? "host"
          : "guest";

      clients.set(clientId, {
        id: clientId,
        ws,
        role,
        disconnected: false
      });

      ws.clientId = clientId;

      const peer = getOtherClient(clientId);

      send(ws, {
        type: "joined",
        room: ROOM_ID,
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

    /*
     * WebRTC signaling.
     */
    if (
      message.type === "offer" ||
      message.type === "answer" ||
      message.type === "candidate"
    ) {
      if (!clientId) return;

      const peer = getOtherClient(clientId);

      if (!peer) return;

      const outgoing = {
        type: message.type
      };

      if (message.type === "offer") {
        outgoing.offer = message.offer;
      }

      if (message.type === "answer") {
        outgoing.answer = message.answer;
      }

      if (message.type === "candidate") {
        outgoing.candidate = message.candidate;
      }

      send(peer.ws, outgoing);
    }
  });

  ws.on("close", () => {
    if (!clientId) return;

    const client = clients.get(clientId);

    if (!client) return;

    /*
     * Keep the client record for reconnect.
     */
    client.ws = null;
    client.disconnected = true;

    const peer = getOtherClient(clientId);

    if (peer) {
      send(peer.ws, {
        type: "peer-disconnected"
      });
    }

    /*
     * Remove stale disconnected clients later.
     */
    setTimeout(() => {
      const current = clients.get(clientId);

      if (
        current &&
        current.disconnected
      ) {
        clients.delete(clientId);
      }
    }, 30000);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Room: ${ROOM_ID}`);
  console.log(`Maximum users: ${MAX_USERS}`);
});
