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

const wss = new WebSocketServer({ server: httpServer });

function send(ws, data) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}

function getOtherPeer(room, ws) {
  if (!room) return null;
  if (ws === room.host) return room.guest;
  if (ws === room.guest) return room.host;
  return null;
}

function leaveRoom(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;

  const room = rooms.get(roomId);
  if (!room) return;

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

      /* CREATE ROOM */

      if (!room) {
        room = {
          host: ws,
          guest: null,
          movie: null,
          movieState: null
        };

        rooms.set(roomId, room);

        ws.roomId = roomId;
        ws.role = "host";

        send(ws, {
          type: "joined",
          role: "host",
          room: roomId,
          peerPresent: false
        });

        console.log(`Room created: ${roomId}`);

        return;
      }

      /* ROOM FULL */

      if (room.guest) {
        send(ws, {
          type: "room-full",
          room: roomId
        });

        return;
      }

      /* JOIN AS GUEST */

      room.guest = ws;

      ws.roomId = roomId;
      ws.role = "guest";

      send(ws, {
        type: "joined",
        role: "guest",
        room: roomId,
        peerPresent: true,
        movie: room.movie,
        movieState: room.movieState
      });

      send(room.host, {
        type: "peer-joined",
        room: roomId
      });

      /* SEND CURRENT MOVIE */

      if (room.movie) {
        send(ws, {
          type: "movie",
          room: roomId,
          url: room.movie
        });
      }

      /* SEND CURRENT PLAYBACK STATE */

      if (room.movieState) {
        send(ws, {
          type: "movie-state",
          room: roomId,
          url: room.movie || "",
          state: room.movieState,
          updatedAt:
            room.movieState.updatedAt ||
            Date.now()
        });
      }

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

      const target = getOtherPeer(room, ws);

      if (!target) return;

      send(target, {
        type: data.type,
        room: roomId,

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

    /* =========================
       MOVIE URL
    ========================= */

    if (data.type === "movie") {
      const roomId = ws.roomId;
      const room = rooms.get(roomId);

      if (!room) return;

      const url =
        String(data.url || "").trim();

      if (!url) return;

      room.movie = url;

      room.movieState = {
        currentTime: 0,
        paused: true,
        playbackRate: 1,
        volume: 1,
        updatedAt: Date.now()
      };

      const target =
        getOtherPeer(room, ws);

      if (target) {

        send(target, {
          type: "movie",
          room: roomId,
          url: url
        });

        send(target, {
          type: "movie-state",
          room: roomId,
          url: url,
          state: room.movieState,
          updatedAt:
            room.movieState.updatedAt
        });

      }

      console.log(
        `Movie updated in room ${roomId}: ${url}`
      );

      return;
    }

    /* =========================
       MOVIE PLAY / PAUSE / SEEK
    ========================= */

    if (data.type === "movie-state") {

      const roomId = ws.roomId;
      const room = rooms.get(roomId);

      if (!room) return;

      const target =
        getOtherPeer(room, ws);

      /* Save movie URL */

      if (data.url) {
        room.movie =
          String(data.url);
      }

      /* Save playback state */

      if (
        data.state &&
        typeof data.state === "object"
      ) {

        room.movieState = {

          currentTime:
            Number(
              data.state.currentTime || 0
            ),

          paused:
            Boolean(
              data.state.paused
            ),

          playbackRate:
            Number(
              data.state.playbackRate || 1
            ),

          volume:
            Number(
              typeof data.state.volume === "number"
                ? data.state.volume
                : 1
            ),

          updatedAt:
            Number(
              data.updatedAt ||
              data.state.updatedAt ||
              Date.now()
            )
        };
      }

      if (
        !target ||
        !room.movieState
      ) {
        return;
      }

      /* Send state to the other person */

      send(target, {

        type: "movie-state",

        room: roomId,

        url:
          room.movie || "",

        state:
          room.movieState,

        updatedAt:
          room.movieState.updatedAt
      });

      return;
    }

    console.log(
      "Unknown message type:",
      data.type
    );
  });

  /* =========================
     DISCONNECT
  ========================= */

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

/* =========================
   START SERVER
========================= */

httpServer.listen(
  PORT,
  HOST,
  () => {
    console.log(
      `Jell room server listening on ${HOST}:${PORT}`
    );
  }
);
