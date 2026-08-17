const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const ROOM_ID = "main";
const MAX_USERS = 2;
const RECONNECT_GRACE_MS = 30000;

const users = new Map();

function createClientId() {
  return crypto.randomUUID();
}

function send(ws, data) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}

function connectedUsers() {
  return [...users.values()].filter(user => user.ws);
}

function getPeer(clientId) {
  for (const [id, user] of users) {
    if (id !== clientId && user.ws) {
      return user;
    }
  }

  return null;
}

function cleanupExpiredUsers() {
  const now = Date.now();

  for (const [id, user] of users) {
    if (
      !user.ws &&
      user.disconnectedAt &&
      now - user.disconnectedAt > RECONNECT_GRACE_MS
    ) {
      users.delete(id);
      console.log("Removed expired client:", id);
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    cleanupExpiredUsers();

    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(
      JSON.stringify({
        ok: true,
        room: ROOM_ID,
        users: connectedUsers().length,
        maxUsers: MAX_USERS
      })
    );

    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/plain"
  });

  res.end("Video call server is running.");
});

const wss = new WebSocketServer({ server });

wss.on("connection", ws => {
  let clientId = null;

  console.log("WebSocket connected");

  ws.on("message", raw => {
    let message;

    try {
      message = JSON.parse(raw.toString());
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
      cleanupExpiredUsers();

      const requestedClientId =
        typeof message.clientId === "string" &&
        message.clientId.trim()
          ? message.clientId.trim()
          : null;

      // ========================================
      // RECONNECT EXISTING CLIENT
      // ========================================

      if (
        requestedClientId &&
        users.has(requestedClientId)
      ) {
        const user = users.get(requestedClientId);

        if (user.ws && user.ws !== ws) {
          send(ws, {
            type: "error",
            message: "Client is already connected."
          });

          return;
        }

        clientId = requestedClientId;

        user.ws = ws;
        user.disconnectedAt = null;

        ws.clientId = clientId;

        const peer = getPeer(clientId);

        send(ws, {
          type: "joined",
          room: ROOM_ID,
          clientId,
          role: user.role,
          peerPresent: Boolean(peer)
        });

        if (peer) {
          send(peer.ws, {
            type: "peer-reconnected"
          });
        }

        console.log("Client reconnected:", clientId);

        return;
      }

      // ========================================
      // MAXIMUM TWO CONNECTED USERS
      // ========================================

      if (connectedUsers().length >= MAX_USERS) {
        send(ws, {
          type: "room-full",
          message: "This room already has two users."
        });

        console.log("Rejected: room full");

        return;
      }

      // ========================================
      // CREATE NEW CLIENT
      // ========================================

      clientId = createClientId();

      const role =
        connectedUsers().length === 0
          ? "host"
          : "guest";

      users.set(clientId, {
        ws,
        role,
        disconnectedAt: null
      });

      ws.clientId = clientId;

      const peer = getPeer(clientId);

      send(ws, {
        type: "joined",
        room: ROOM_ID,
        clientId,
        role,
        peerPresent: Boolean(peer)
      });

      // Tell the existing user that the second
      // user has arrived.
      if (peer) {
        send(peer.ws, {
          type: "peer-joined"
        });
      }

      console.log(
        `${role} joined room "${ROOM_ID}": ${clientId}`
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

      const peer = getPeer(clientId);

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

  // ==========================================
  // DISCONNECT
  // ==========================================

  ws.on("close", () => {
    if (!clientId) {
      return;
    }

    const user = users.get(clientId);

    if (!user || user.ws !== ws) {
      return;
    }

    user.ws = null;
    user.disconnectedAt = Date.now();

    const peer = getPeer(clientId);

    if (peer) {
      send(peer.ws, {
        type: "peer-disconnected"
      });
    }

    console.log("Client disconnected:", clientId);

    setTimeout(
      cleanupExpiredUsers,
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

setInterval(cleanupExpiredUsers, 5000);

server.listen(PORT, HOST, () => {
  console.log(`Server running on ${HOST}:${PORT}`);
  console.log(`Room: ${ROOM_ID}`);
  console.log(`Maximum users: ${MAX_USERS}`);
});
