const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const ROOM_ID = "main";
const RECONNECT_GRACE_MS = 30000;

// ONE ROOM ONLY
const room = {
  host: null,
  guest: null,
  movie: null
};

const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(
      JSON.stringify({
        ok: true,
        service: "jell-room-server",
        room: ROOM_ID,
        hostConnected: !!room.host,
        guestConnected: !!room.guest
      })
    );

    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/plain"
  });

  res.end("Jell one-room signaling server is running.");
});

const wss = new WebSocketServer({
  server: httpServer
});

function send(ws, data) {
  if (ws && ws.readyState === 1) {
    try {
      ws.send(JSON.stringify(data));
    } catch (err) {
      console.error("Send error:", err.message);
    }
  }
}

function getPeer(ws) {
  if (room.host?.ws === ws) {
    return room.guest?.ws || null;
  }

  if (room.guest?.ws === ws) {
    return room.host?.ws || null;
  }

  return null;
}

function sendToPeer(ws, data) {
  const peer = getPeer(ws);

  if (peer) {
    send(peer, data);
  }
}

function slotFor(ws) {
  if (room.host?.ws === ws) return "host";
  if (room.guest?.ws === ws) return "guest";
  return null;
}

function cleanupExpiredSlots() {
  const now = Date.now();

  if (
    room.host &&
    !room.host.ws &&
    room.host.disconnectedAt &&
    now - room.host.disconnectedAt > RECONNECT_GRACE_MS
  ) {
    console.log("Host reconnect window expired.");
    room.host = null;
  }

  if (
    room.guest &&
    !room.guest.ws &&
    room.guest.disconnectedAt &&
    now - room.guest.disconnectedAt > RECONNECT_GRACE_MS
  ) {
    console.log("Guest reconnect window expired.");
    room.guest = null;
  }
}

function notifyPeerLeft(ws) {
  const peer = getPeer(ws);

  if (peer) {
    send(peer, {
      type: "peer-left",
      room: ROOM_ID
    });
  }
}

function disconnectSlot(ws) {
  const slot = slotFor(ws);

  if (!slot) return;

  const record = room[slot];

  if (!record) return;

  /*
   * Keep the identity for a short time so a refresh/reconnect
   * does not immediately create a false "room full".
   */
  record.ws = null;
  record.disconnectedAt = Date.now();

  console.log(
    `${slot} disconnected. Reconnect grace started.`
  );
}

wss.on("connection", (ws) => {
  ws.clientId = null;

  console.log("WebSocket connected");

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(ws, {
        type: "error",
        message: "Invalid JSON"
      });
      return;
    }

    /*
    =========================================
    JOIN THE ONLY ROOM
    =========================================
    */

    if (data.type === "join") {
      cleanupExpiredSlots();

      const clientId = String(
        data.clientId || ""
      ).trim();

      if (!clientId) {
        send(ws, {
          type: "error",
          message: "Missing client ID"
        });

        return;
      }

      ws.clientId = clientId;

      /*
       * RECONNECT EXISTING HOST
       */

      if (
        room.host &&
        room.host.clientId === clientId
      ) {
        room.host.ws = ws;
        room.host.disconnectedAt = null;

        send(ws, {
          type: "joined",
          role: "host",
          room: ROOM_ID,
          peerPresent: !!room.guest?.ws,
          movie: room.movie
        });

        if (room.guest?.ws) {
          send(room.guest.ws, {
            type: "peer-rejoined",
            room: ROOM_ID
          });
        }

        console.log("Host reconnected.");

        return;
      }

      /*
       * RECONNECT EXISTING GUEST
       */

      if (
        room.guest &&
        room.guest.clientId === clientId
      ) {
        room.guest.ws = ws;
        room.guest.disconnectedAt = null;

        send(ws, {
          type: "joined",
          role: "guest",
          room: ROOM_ID,
          peerPresent: !!room.host?.ws,
          movie: room.movie
        });

        if (room.host?.ws) {
          send(room.host.ws, {
            type: "peer-rejoined",
            room: ROOM_ID
          });
        }

        console.log("Guest reconnected.");

        return;
      }

      /*
       * FIRST USER
       */

      if (!room.host) {
        room.host = {
          clientId,
          ws,
          disconnectedAt: null
        };

        send(ws, {
          type: "joined",
          role: "host",
          room: ROOM_ID,
          peerPresent: !!room.guest?.ws,
          movie: room.movie
        });

        console.log("Host joined the only room.");

        if (room.guest?.ws) {
          send(room.guest.ws, {
            type: "peer-rejoined",
            room: ROOM_ID
          });
        }

        return;
      }

      /*
       * SECOND USER
       */

      if (!room.guest) {
        room.guest = {
          clientId,
          ws,
          disconnectedAt: null
        };

        send(ws, {
          type: "joined",
          role: "guest",
          room: ROOM_ID,
          peerPresent: !!room.host?.ws,
          movie: room.movie
        });

        if (room.host?.ws) {
          send(room.host.ws, {
            type: "peer-joined",
            room: ROOM_ID
          });
        }

        console.log("Guest joined the only room.");

        return;
      }

      /*
       * BOTH SLOTS ARE USED
       */

      send(ws, {
        type: "room-full",
        room: ROOM_ID
      });

      console.log(
        "Rejected third user: room full."
      );

      return;
    }

    /*
    =========================================
    WEBRTC SIGNALING
    =========================================
    */

    if (
      data.type === "offer" ||
      data.type === "answer" ||
      data.type === "candidate"
    ) {
      const peer = getPeer(ws);

      if (!peer) {
        return;
      }

      send(peer, {
        type: data.type,
        room: ROOM_ID,

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
    =========================================
    MOVIE URL
    =========================================
    */

    if (data.type === "movie") {
      room.movie = {
        url: String(data.url || ""),
        state: data.state || {
          currentTime: 0,
          paused: true,
          playbackRate: 1,
          volume: 1
        },
        updatedAt: Date.now()
      };

      sendToPeer(ws, {
        type: "movie",
        room: ROOM_ID,
        url: room.movie.url,
        state: room.movie.state,
        updatedAt: room.movie.updatedAt
      });

      return;
    }

    /*
    =========================================
    MOVIE STATE
    =========================================
    */

    if (data.type === "movie-state") {
      if (!room.movie) {
        room.movie = {
          url: String(data.url || ""),
          state: data.state || {},
          updatedAt:
            Number(data.updatedAt) || Date.now()
        };
      } else {
        if (data.url) {
          room.movie.url =
            String(data.url);
        }

        if (data.state) {
          room.movie.state =
            data.state;
        }

        room.movie.updatedAt =
          Number(data.updatedAt) ||
          Date.now();
      }

      sendToPeer(ws, {
        type: "movie-state",
        room: ROOM_ID,
        url: room.movie.url,
        state: room.movie.state,
        updatedAt: room.movie.updatedAt
      });

      return;
    }

    /*
    =========================================
    PING
    =========================================
    */

    if (data.type === "ping") {
      send(ws, {
        type: "pong"
      });

      return;
    }
  });

  ws.on("close", () => {
    /*
     * Only mark the slot disconnected if this
     * websocket is still the active websocket.
     *
     * This prevents an old connection from
     * kicking out a newly reconnected user.
     */

    if (
      room.host &&
      room.host.ws === ws
    ) {
      notifyPeerLeft(ws);
      disconnectSlot(ws);
    }

    if (
      room.guest &&
      room.guest.ws === ws
    ) {
      notifyPeerLeft(ws);
      disconnectSlot(ws);
    }

    cleanupExpiredSlots();

    console.log("WebSocket closed.");
  });

  ws.on("error", (err) => {
    console.error(
      "WebSocket error:",
      err.message
    );
  });
});

setInterval(
  cleanupExpiredSlots,
  5000
);

httpServer.listen(
  PORT,
  HOST,
  () => {
    console.log(
      `Jell one-room server listening on ${HOST}:${PORT}`
    );
  }
);
