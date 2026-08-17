const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const ROOM_ID = "main";
const MAX_USERS = 2;
const RECONNECT_GRACE_MS = 30000;

const room = {
  users: new Map()
};

function createClientId() {
  return crypto.randomUUID();
}

function send(ws, data) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}

function cleanupRoom() {
  const now = Date.now();

  for (const [clientId, user] of room.users) {
    if (
      !user.ws &&
      user.disconnectedAt &&
      now - user.disconnectedAt > RECONNECT_GRACE_MS
    ) {
      room.users.delete(clientId);
      console.log("Removed expired user:", clientId);
    }
  }
}

function getOtherUser(clientId) {
  for (const [id, user] of room.users) {
    if (id !== clientId && user.ws) {
      return user;
    }
  }

  return null;
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    cleanupRoom();

    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(
      JSON.stringify({
        ok: true,
        room: ROOM_ID,
        users: room.users.size
      })
    );

    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/plain"
  });

  res.end("One-room signaling server is running.");
});

const wss = new WebSocketServer({
  server
});

wss.on("connection", (ws) => {
  let clientId = null;

  console.log("WebSocket connected");

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(ws, {
        type: "error",
        message: "Invalid message."
      });
      return;
    }

    /*
    ==========================================
    JOIN
    ==========================================
    */

    if (data.type === "join") {
      cleanupRoom();

      /*
       * CLIENT ID IS OPTIONAL.
       * If missing, the server creates one.
       */

      const requestedId =
        typeof data.clientId === "string" &&
        data.clientId.trim()
          ? data.clientId.trim()
          : null;

      /*
       * Reconnect existing client
       */

      if (
        requestedId &&
        room.users.has(requestedId)
      ) {
        const existing =
          room.users.get(requestedId);

        clientId = requestedId;

        existing.ws = ws;
        existing.disconnectedAt = null;

        ws.clientId = clientId;

        const other =
          getOtherUser(clientId);

        send(ws, {
          type: "joined",
          clientId,
          room: ROOM_ID,
          role: existing.role,
          peerPresent: !!other
        });

        if (other) {
          send(other.ws, {
            type: "peer-rejoined"
          });
        }

        console.log(
          "User reconnected:",
          clientId
        );

        return;
      }

      /*
       * Brand-new client
       */

      if (room.users.size >= MAX_USERS) {
        send(ws, {
          type: "room-full"
        });

        console.log(
          "Rejected user: room full"
        );

        return;
      }

      clientId =
        requestedId ||
        createClientId();

      const role =
        room.users.size === 0
          ? "host"
          : "guest";

      room.users.set(clientId, {
        ws,
        role,
        disconnectedAt: null
      });

      ws.clientId = clientId;

      const other =
        getOtherUser(clientId);

      /*
       * Tell client its automatically
       * assigned ID.
       */

      send(ws, {
        type: "joined",
        clientId,
        room: ROOM_ID,
        role,
        peerPresent: !!other
      });

      if (other) {
        send(other.ws, {
          type: "peer-joined"
        });
      }

      console.log(
        `${role} joined. Users: ${room.users.size}`
      );

      return;
    }

    /*
    ==========================================
    WEBRTC SIGNALING
    ==========================================
    */

    if (
      data.type === "offer" ||
      data.type === "answer" ||
      data.type === "candidate"
    ) {
      if (!clientId) {
        return;
      }

      const other =
        getOtherUser(clientId);

      if (!other) {
        return;
      }

      send(other.ws, {
        type: data.type,

        ...(data.type === "offer"
          ? { offer: data.offer }
          : {}),

        ...(data.type === "answer"
          ? { answer: data.answer }
          : {}),

        ...(data.type === "candidate"
          ? { candidate: data.candidate }
          : {})
      });

      return;
    }

    /*
    ==========================================
    PING
    ==========================================
    */

    if (data.type === "ping") {
      send(ws, {
        type: "pong"
      });
    }
  });

  ws.on("close", () => {
    if (!clientId) {
      return;
    }

    const user =
      room.users.get(clientId);

    /*
     * Ignore old websocket connections.
     * This is important during reconnects.
     */

    if (!user || user.ws !== ws) {
      return;
    }

    user.ws = null;
    user.disconnectedAt = Date.now();

    const other =
      getOtherUser(clientId);

    if (other) {
      send(other.ws, {
        type: "peer-left"
      });
    }

    console.log(
      "User disconnected:",
      clientId
    );

    /*
     * Do NOT immediately delete the user.
     * They have 30 seconds to reconnect.
     */

    setTimeout(() => {
      cleanupRoom();
    }, RECONNECT_GRACE_MS + 100);
  });

  ws.on("error", (error) => {
    console.error(
      "WebSocket error:",
      error.message
    );
  });
});

setInterval(
  cleanupRoom,
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
      "Room:",
      ROOM_ID
    );
    console.log(
      "Maximum users:",
      MAX_USERS
    );
  }
);
