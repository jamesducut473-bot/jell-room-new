const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const ROOM_ID = "main";
const MAX_USERS = 2;
const RECONNECT_GRACE_MS = 30000;

const clients = new Map();

const TURN_URLS = process.env.TURN_URLS
  ? process.env.TURN_URLS
      .split(",")
      .map(x => x.trim())
      .filter(Boolean)
  : [];

const TURN_USERNAME =
  process.env.TURN_USERNAME || "";

const TURN_CREDENTIAL =
  process.env.TURN_CREDENTIAL || "";

function createClientId() {
  return crypto.randomUUID();
}

function send(ws, data) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}

function activeClients() {
  return [...clients.values()].filter(
    client => client.ws
  );
}

function findPeer(clientId) {
  return activeClients().find(
    client => client.id !== clientId
  );
}

function cleanupExpiredClients() {
  const now = Date.now();

  for (const [id, client] of clients) {
    if (
      !client.ws &&
      client.disconnectedAt &&
      now - client.disconnectedAt >
        RECONNECT_GRACE_MS
    ) {
      clients.delete(id);
      console.log("Expired client removed:", id);
    }
  }
}

function getIceServers() {
  const iceServers = [
    {
      urls: [
        "stun:stun.l.google.com:19302",
        "stun:stun1.l.google.com:19302"
      ]
    }
  ];

  if (
    TURN_URLS.length &&
    TURN_USERNAME &&
    TURN_CREDENTIAL
  ) {
    iceServers.push({
      urls: TURN_URLS,
      username: TURN_USERNAME,
      credential: TURN_CREDENTIAL
    });
  }

  return iceServers;
}

const server = http.createServer(
  (req, res) => {
    if (req.url === "/health") {
      cleanupExpiredClients();

      res.writeHead(200, {
        "Content-Type":
          "application/json; charset=utf-8"
      });

      res.end(
        JSON.stringify({
          ok: true,
          room: ROOM_ID,
          users: activeClients().length,
          maxUsers: MAX_USERS
        })
      );

      return;
    }

    if (req.url === "/ice-config") {
      res.writeHead(200, {
        "Content-Type":
          "application/json; charset=utf-8",
        "Cache-Control": "no-store"
      });

      res.end(
        JSON.stringify({
          iceServers: getIceServers()
        })
      );

      return;
    }

    const indexPath = path.join(
      __dirname,
      "index.html"
    );

    fs.readFile(
      indexPath,
      (error, data) => {
        if (error) {
          res.writeHead(500, {
            "Content-Type":
              "text/plain; charset=utf-8"
          });

          res.end(
            "index.html could not be loaded."
          );

          return;
        }

        res.writeHead(200, {
          "Content-Type":
            "text/html; charset=utf-8"
        });

        res.end(data);
      }
    );
  }
);

const wss =
  new WebSocketServer({ server });

wss.on("connection", ws => {
  let clientId = null;

  console.log("WebSocket connected.");

  ws.on("message", raw => {
    let message;

    try {
      message = JSON.parse(
        raw.toString()
      );
    } catch {
      send(ws, {
        type: "error",
        message: "Invalid message."
      });

      return;
    }

    // ==========================================
    // JOIN
    // ==========================================

    if (message.type === "join") {
      cleanupExpiredClients();

      const requestedId =
        typeof message.clientId === "string" &&
        message.clientId.trim()
          ? message.clientId.trim()
          : null;

      // ------------------------------------------
      // RECONNECT
      // ------------------------------------------

      if (
        requestedId &&
        clients.has(requestedId)
      ) {
        const existing =
          clients.get(requestedId);

        if (
          existing.ws &&
          existing.ws !== ws
        ) {
          send(ws, {
            type: "error",
            message:
              "This client is already connected."
          });

          return;
        }

        clientId = requestedId;

        existing.ws = ws;
        existing.disconnectedAt = null;

        ws.clientId = clientId;

        const peer =
          findPeer(clientId);

        send(ws, {
          type: "joined",
          room: ROOM_ID,
          clientId,
          role: existing.role,
          peerPresent: Boolean(peer)
        });

        if (peer) {
          send(peer.ws, {
            type: "peer-reconnected"
          });
        }

        console.log(
          "Client reconnected:",
          clientId
        );

        return;
      }

      // ------------------------------------------
      // ROOM LIMIT
      // ------------------------------------------

      if (
        activeClients().length >=
        MAX_USERS
      ) {
        send(ws, {
          type: "room-full",
          message:
            "The room already has two users."
        });

        return;
      }

      // ------------------------------------------
      // NEW CLIENT
      // ------------------------------------------

      clientId =
        createClientId();

      const role =
        activeClients().length === 0
          ? "host"
          : "guest";

      clients.set(clientId, {
        id: clientId,
        ws,
        role,
        disconnectedAt: null
      });

      ws.clientId = clientId;

      const peer =
        findPeer(clientId);

      send(ws, {
        type: "joined",
        room: ROOM_ID,
        clientId,
        role,
        peerPresent: Boolean(peer)
      });

      if (peer) {
        send(peer.ws, {
          type: "peer-joined"
        });
      }

      console.log(
        `${role} joined ${ROOM_ID}: ${clientId}`
      );

      return;
    }

    // ==========================================
    // WEBRTC SIGNALING
    // ==========================================

    if (
      message.type === "offer" ||
      message.type === "answer" ||
      message.type === "candidate"
    ) {
      if (!clientId) {
        return;
      }

      const peer =
        findPeer(clientId);

      if (!peer) {
        return;
      }

      if (message.type === "offer") {
        send(peer.ws, {
          type: "offer",
          offer: message.offer
        });

        return;
      }

      if (message.type === "answer") {
        send(peer.ws, {
          type: "answer",
          answer: message.answer
        });

        return;
      }

      if (message.type === "candidate") {
        send(peer.ws, {
          type: "candidate",
          candidate: message.candidate
        });

        return;
      }
    }
  });

  ws.on("close", () => {
    if (!clientId) {
      return;
    }

    const client =
      clients.get(clientId);

    if (
      !client ||
      client.ws !== ws
    ) {
      return;
    }

    client.ws = null;
    client.disconnectedAt =
      Date.now();

    const peer =
      findPeer(clientId);

    if (peer) {
      send(peer.ws, {
        type: "peer-disconnected"
      });
    }

    console.log(
      "Client disconnected:",
      clientId
    );

    setTimeout(
      cleanupExpiredClients,
      RECONNECT_GRACE_MS + 100
    );
  });

  ws.on("error", error => {
    console.error(
      "WebSocket error:",
      error.message
    );
  });
});

setInterval(
  cleanupExpiredClients,
  5000
);

server.listen(
  PORT,
  HOST,
  () => {
    console.log(
      `Server running on ${HOST}:${PORT}`
    );

    console.log(
      `Room: ${ROOM_ID}`
    );

    console.log(
      `Maximum users: ${MAX_USERS}`
    );

    console.log(
      `TURN configured: ${
        TURN_URLS.length > 0 &&
        TURN_USERNAME &&
        TURN_CREDENTIAL
          ? "yes"
          : "no"
      }`
    );
  }
);
