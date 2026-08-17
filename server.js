const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const ROOM_ID = "main";
const MAX_USERS = 2;
const RECONNECT_GRACE_MS = 30000;

const users = new Map();

function makeClientId() {
  return crypto.randomUUID();
}

function send(ws, message) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(message));
  }
}

function activeUsers() {
  return [...users.values()].filter(user => user.ws);
}

function findOther(clientId) {
  for (const [id, user] of users) {
    if (id !== clientId && user.ws) {
      return user;
    }
  }

  return null;
}

function cleanupUsers() {
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
    cleanupUsers();

    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(
      JSON.stringify({
        ok: true,
        room: ROOM_ID,
        users: activeUsers().length
      })
    );

    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/plain"
  });

  res.end("Video call server is running.");
});

const wss = new WebSocketServer({
  server
});

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

    if (message.type === "join") {
      cleanupUsers();

      const requestedId =
        typeof message.clientId === "string" &&
        message.clientId.trim()
          ? message.clientId.trim()
          : null;

      /*
       * RECONNECT
       */
      if (requestedId && users.has(requestedId)) {
        const user = users.get(requestedId);

        clientId = requestedId;

        user.ws = ws;
        user.disconnectedAt = null;

        ws.clientId = clientId;

        const other = findOther(clientId);

        send(ws, {
          type: "joined",
          clientId,
          room: ROOM_ID,
          role: user.role,
          peerPresent: Boolean(other)
        });

        if (other) {
          send(other.ws, {
            type: "peer-reconnected"
          });
        }

        console.log("Client reconnected:", clientId);

        return;
      }

      /*
       * MAXIMUM 2 USERS
       */
      if (activeUsers().length >= MAX_USERS) {
        send(ws, {
          type: "room-full"
        });

        console.log("Rejected: room full");

        return;
      }

      /*
       * NEW CLIENT
       */
      clientId = requestedId || makeClientId();

      const role =
        activeUsers().length === 0
          ? "host"
          : "guest";

      users.set(clientId, {
        ws,
        role,
        disconnectedAt: null
      });

      ws.clientId = clientId;

      const other = findOther(clientId);

      send(ws, {
        type: "joined",
        clientId,
        room: ROOM_ID,
        role,
        peerPresent: Boolean(other)
      });

      if (other) {
        send(other.ws, {
          type: "peer-joined"
        });
      }

      console.log(
        `${role} joined: ${clientId}`
      );

      return;
    }

    /*
     * WEBRTC SIGNALING
     */
    if (
      message.type === "offer" ||
      message.type === "answer" ||
      message.type === "candidate"
    ) {
      if (!clientId) {
        return;
      }

      const other = findOther(clientId);

      if (!other) {
        return;
      }

      send(other.ws, {
        type: message.type,

        ...(message.type === "offer"
          ? { offer: message.offer }
          : {}),

        ...(message.type === "answer"
          ? { answer: message.answer }
          : {}),

        ...(message.type === "candidate"
          ? { candidate: message.candidate }
          : {})
      });

      return;
    }
  });

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

    const other = findOther(clientId);

    if (other) {
      send(other.ws, {
        type: "peer-disconnected"
      });
    }

    console.log("Client disconnected:", clientId);
  });

  ws.on("error", error => {
    console.error(
      "WebSocket error:",
      error.message
    );
  });
});

setInterval(cleanupUsers, 5000);

server.listen(PORT, HOST, () => {
  console.log(
    `Server running on ${HOST}:${PORT}`
  );

  console.log(
    `Room: ${ROOM_ID}`
  );

  console.log(
    `Maximum users: ${MAX_USERS}`
  );
});
