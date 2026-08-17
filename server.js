const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

const rooms = new Map();

const server = http.createServer((req, res) => {
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

const wss = new WebSocketServer({ server });

function send(ws, data) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}

function sendToPeer(ws, data) {
  if (!ws || !ws.roomId) return;

  const room = rooms.get(ws.roomId);
  if (!room) return;

  const peer =
    ws === room.host
      ? room.guest
      : ws === room.guest
      ? room.host
      : null;

  if (peer) send(peer, data);
}

function broadcastRoom(room, data, except = null) {
  if (room.host && room.host !== except) send(room.host, data);
  if (room.guest && room.guest !== except) send(room.guest, data);
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

    if (room.host) {
      send(room.host, {
        type: "peer-left",
        room: roomId
      });
    }
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

    /*
    =========================================
    JOIN ROOM
    =========================================
    */

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

      /*
      FIRST USER = HOST
      */

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
          movie: room.movie
        });

        console.log(`Room created: ${roomId}`);
        return;
      }

      /*
      SECOND USER = GUEST
      */

      if (room.guest) {
        send(ws, {
          type: "room-full",
          room: roomId
        });
        return;
      }

      room.guest = ws;

      ws.roomId = roomId;
      ws.role = "guest";

      send(ws, {
        type: "joined",
        role: "guest",
        room: roomId,
        peerPresent: true,
        movie: room.movie
      });

      /*
      Tell host that a new guest has arrived.
      Host is responsible for starting WebRTC.
      */

      send(room.host, {
        type: "peer-joined",
        room: roomId
      });

      console.log(`Guest joined: ${roomId}`);
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
      sendToPeer(ws, {
        type: data.type,
        room: ws.roomId,
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
    MOVIE
    =========================================
    */

    if (data.type === "movie") {
      const roomId = ws.roomId;
      const room = rooms.get(roomId);

      if (!room) return;

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

      broadcastRoom(
        room,
        {
          type: "movie",
          room: roomId,
          url: room.movie.url,
          state: room.movie.state,
          updatedAt: room.movie.updatedAt
        },
        ws
      );

      return;
    }

    /*
    =========================================
    MOVIE STATE
    =========================================
    */

    if (data.type === "movie-state") {
      const roomId = ws.roomId;
      const room = rooms.get(roomId);

      if (!room) return;

      if (!room.movie) {
        room.movie = {
          url: String(data.url || ""),
          state: data.state || {},
          updatedAt: Date.now()
        };
      } else {
        if (data.url) {
          room.movie.url = String(data.url);
        }

        if (data.state) {
          room.movie.state = data.state;
        }

        room.movie.updatedAt =
          Number(data.updatedAt) || Date.now();
      }

      broadcastRoom(
        room,
        {
          type: "movie-state",
          room: roomId,
          url: room.movie.url,
          state: room.movie.state,
          updatedAt: room.movie.updatedAt
        },
        ws
      );

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
    leaveRoom(ws);
  });

  ws.on("error", (error) => {
    console.error(
      "WebSocket error:",
      error.message
    );
  });
});

server.listen(PORT, HOST, () => {
  console.log(
    `Jell room server listening on ${HOST}:${PORT}`
  );
});
