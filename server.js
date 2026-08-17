const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const rooms = new Map();

const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      service: "jell-room-server",
      rooms: rooms.size
    }));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Jell room signaling server is running.");
});

const wss = new WebSocketServer({
  server: httpServer
});

function send(ws, data) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}

function broadcastToRoom(room, data, except = null) {
  if (!room) return;

  if (room.host && room.host !== except) {
    send(room.host, data);
  }

  if (room.guest && room.guest !== except) {
    send(room.guest, data);
  }
}

function getMovieState(room) {
  if (!room || !room.movie) return null;

  return {
    url: room.movie.url,
    state: {
      currentTime: Number(room.movie.state?.currentTime || 0),
      paused: room.movie.state?.paused !== false,
      playbackRate: Number(room.movie.state?.playbackRate || 1),
      volume: typeof room.movie.state?.volume === "number"
        ? room.movie.state.volume
        : 1
    },
    updatedAt: Number(room.movie.updatedAt || Date.now())
  };
}

function leaveRoom(ws) {
  const roomId = ws.roomId;

  if (!roomId) return;

  const room = rooms.get(roomId);

  if (!room) {
    ws.roomId = null;
    ws.role = null;
    return;
  }

  if (room.host === ws) {
    if (room.guest) {
      send(room.guest, {
        type: "peer-left",
        room: roomId
      });

      room.guest.roomId = null;
      room.guest.role = null;
    }

    rooms.delete(roomId);

  } else if (room.guest === ws) {
    room.guest = null;

    send(room.host, {
      type: "peer-left",
      room: roomId
    });
  }

  ws.roomId = null;
  ws.role = null;

  console.log(`Left room: ${roomId}`);
}

wss.on("connection", (ws) => {
  ws.roomId = null;
  ws.role = null;

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

    /* =========================
       JOIN ROOM
    ========================= */

    if (data.type === "join") {
      const roomId = String(data.room || "").trim();

      if (!roomId) {
        send(ws, {
          type: "error",
          message: "Missing room ID"
        });
        return;
      }

      if (ws.roomId) {
        leaveRoom(ws);
      }

      let room = rooms.get(roomId);

      /* FIRST PERSON = HOST */

      if (!room) {
        room = {
          host: ws,
          guest: null,
          movie: null
        };

        rooms.set(roomId, room);

        ws.roomId = roomId;
        ws.role = "host";

        send(ws, {
          type: "joined",
          role: "host",
          room: roomId,
          peerPresent: false,
          movie: null
        });

        console.log(`Room created: ${roomId}`);
        return;
      }

      /* THIRD PERSON = FULL */

      if (room.guest) {
        send(ws, {
          type: "room-full",
          room: roomId
        });

        return;
      }

      /* SECOND PERSON = GUEST */

      room.guest = ws;

      ws.roomId = roomId;
      ws.role = "guest";

      send(ws, {
        type: "joined",
        role: "guest",
        room: roomId,
        peerPresent: true,
        movie: getMovieState(room)
      });

      send(room.host, {
        type: "peer-joined",
        room: roomId,
        movie: getMovieState(room)
      });

      console.log(`Guest joined: ${roomId}`);
      return;
    }

    /* =========================
       WEBRTC SIGNALING
    ========================= */

    if (
      data.type === "offer" ||
      data.type === "answer" ||
      data.type === "candidate"
    ) {
      const roomId = ws.roomId;
      const room = rooms.get(roomId);

      if (!room) return;

      const target =
        ws === room.host
          ? room.guest
          : ws === room.guest
          ? room.host
          : null;

      if (!target) return;

      if (data.type === "offer") {
        send(target, {
          type: "offer",
          room: roomId,
          offer: data.offer
        });
      }

      if (data.type === "answer") {
        send(target, {
          type: "answer",
          room: roomId,
          answer: data.answer
        });
      }

      if (data.type === "candidate") {
        send(target, {
          type: "candidate",
          room: roomId,
          candidate: data.candidate
        });
      }

      return;
    }

    /* =========================
       NEW MOVIE
       ONLY HOST MAY CHANGE IT
    ========================= */

    if (data.type === "movie") {
      const roomId = ws.roomId;
      const room = rooms.get(roomId);

      if (!room || ws !== room.host) return;

      const url = String(data.url || "").trim();

      if (!url) return;

      room.movie = {
        url,
        state: {
          currentTime: 0,
          paused: true,
          playbackRate: 1,
          volume: 1
        },
        updatedAt: Date.now()
      };

      broadcastToRoom(
        room,
        {
          type: "movie",
          room: roomId,
          url,
          state: room.movie.state,
          updatedAt: room.movie.updatedAt
        },
        ws
      );

      return;
    }

    /* =========================
       MOVIE PLAYBACK STATE
       ONLY HOST MAY SEND
    ========================= */

    if (data.type === "movie-state") {
      const roomId = ws.roomId;
      const room = rooms.get(roomId);

      if (!room || ws !== room.host) return;
      if (!room.movie) return;

      const state = data.state || {};

      room.movie.state = {
        currentTime: Number(state.currentTime || 0),
        paused: state.paused !== false,
        playbackRate: Number(state.playbackRate || 1),
        volume:
          typeof state.volume === "number"
            ? Math.max(0, Math.min(1, state.volume))
            : 1
      };

      room.movie.updatedAt = Number(
        data.updatedAt || Date.now()
      );

      if (data.url) {
        room.movie.url = String(data.url);
      }

      /* Send ONLY to guest.
         This prevents an infinite sync loop. */

      if (room.guest) {
        send(room.guest, {
          type: "movie-state",
          room: roomId,
          url: room.movie.url,
          state: room.movie.state,
          updatedAt: room.movie.updatedAt
        });
      }

      return;
    }
  });

  ws.on("close", () => {
    leaveRoom(ws);
  });

  ws.on("error", (err) => {
    console.error(
      "WebSocket error:",
      err.message
    );
  });
});

httpServer.listen(PORT, HOST, () => {
  console.log(
    `Jell room server listening on ${HOST}:${PORT}`
  );
});
