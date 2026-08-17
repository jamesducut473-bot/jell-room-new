const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const HOST = "0.0.0.0";

// ============================================
// SINGLE ROOM CONFIGURATION
// ============================================

const ROOM_ID = "main";
const MAX_USERS = 2;

// Kapag nawalan ng connection ang user,
// hawakan muna ang slot niya para makapag-reconnect.
const RECONNECT_GRACE_MS = 30000;

const users = new Map();


// ============================================
// HELPERS
// ============================================

function createClientId() {
  return crypto.randomUUID();
}


function send(ws, data) {
  if (!ws) return;

  if (ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}


function getConnectedUsers() {
  return [...users.values()].filter(
    user => user.ws
  );
}


function getPeer(clientId) {
  for (const [id, user] of users) {
    if (
      id !== clientId &&
      user.ws
    ) {
      return user;
    }
  }

  return null;
}


function cleanupExpiredUsers() {
  const now = Date.now();

  for (const [clientId, user] of users) {

    if (
      !user.ws &&
      user.disconnectedAt &&
      now - user.disconnectedAt >
        RECONNECT_GRACE_MS
    ) {

      users.delete(clientId);

      console.log(
        "Expired user slot:",
        clientId
      );
    }
  }
}


// ============================================
// HTTP SERVER
// ============================================

const server = http.createServer(
  (req, res) => {

    if (req.url === "/health") {

      cleanupExpiredUsers();

      res.writeHead(200, {
        "Content-Type":
          "application/json"
      });

      res.end(
        JSON.stringify({
          ok: true,
          room: ROOM_ID,
          users:
            getConnectedUsers().length,
          maxUsers: MAX_USERS
        })
      );

      return;
    }


    res.writeHead(200, {
      "Content-Type":
        "text/plain"
    });

    res.end(
      "Single-room WebRTC server is running."
    );
  }
);


// ============================================
// WEBSOCKET SERVER
// ============================================

const wss =
  new WebSocketServer({
    server
  });


wss.on(
  "connection",
  ws => {

    let clientId = null;


    console.log(
      "WebSocket connection opened"
    );


    // ========================================
    // MESSAGE
    // ========================================

    ws.on(
      "message",
      raw => {

        let message;

        try {

          message =
            JSON.parse(
              raw.toString()
            );

        } catch {

          send(ws, {
            type: "error",
            message:
              "Invalid JSON message."
          });

          return;
        }


        // ====================================
        // JOIN
        // ====================================

        if (
          message.type === "join"
        ) {

          cleanupExpiredUsers();


          const requestedId =
            typeof message.clientId ===
              "string" &&
            message.clientId.trim()
              ? message.clientId.trim()
              : null;


          // ==================================
          // RECONNECT
          // ==================================

          if (
            requestedId &&
            users.has(requestedId)
          ) {

            const existingUser =
              users.get(requestedId);


            // Huwag hayaang dalawang
            // WebSocket ang gumamit ng
            // parehong client ID.

            if (
              existingUser.ws &&
              existingUser.ws !== ws
            ) {

              send(ws, {
                type: "error",
                message:
                  "Client is already connected."
              });

              return;
            }


            clientId =
              requestedId;


            existingUser.ws =
              ws;

            existingUser.disconnectedAt =
              null;


            ws.clientId =
              clientId;


            const peer =
              getPeer(clientId);


            send(ws, {
              type: "joined",
              clientId,
              room: ROOM_ID,
              role:
                existingUser.role,
              peerPresent:
                Boolean(peer)
            });


            if (peer) {

              send(peer.ws, {
                type:
                  "peer-reconnected"
              });

            }


            console.log(
              "User reconnected:",
              clientId
            );


            return;
          }


          // ==================================
          // MAXIMUM 2 USERS
          // ==================================

          if (
            getConnectedUsers().length >=
            MAX_USERS
          ) {

            send(ws, {
              type: "room-full",
              message:
                "Only two users are allowed."
            });

            console.log(
              "Rejected connection: room full"
            );

            return;
          }


          // ==================================
          // NEW USER
          // ==================================

          clientId =
            createClientId();


          const role =
            getConnectedUsers().length ===
            0
              ? "host"
              : "guest";


          users.set(
            clientId,
            {
              ws,
              role,
              disconnectedAt:
                null
            }
          );


          ws.clientId =
            clientId;


          const peer =
            getPeer(clientId);


          send(ws, {
            type: "joined",
            clientId,
            room: ROOM_ID,
            role,
            peerPresent:
              Boolean(peer)
          });


          if (peer) {

            send(peer.ws, {
              type:
                "peer-joined"
            });

          }


          console.log(
            `${role} joined the main room: ${clientId}`
          );


          return;
        }


        // ====================================
        // WEBRTC SIGNALING
        // ====================================

        if (
          message.type === "offer" ||
          message.type === "answer" ||
          message.type === "candidate"
        ) {

          if (!clientId) {
            return;
          }


          const peer =
            getPeer(clientId);


          if (!peer) {
            return;
          }


          if (
            message.type ===
            "offer"
          ) {

            send(peer.ws, {
              type: "offer",
              offer:
                message.offer
            });

            return;
          }


          if (
            message.type ===
            "answer"
          ) {

            send(peer.ws, {
              type: "answer",
              answer:
                message.answer
            });

            return;
          }


          if (
            message.type ===
            "candidate"
          ) {

            send(peer.ws, {
              type:
                "candidate",
              candidate:
                message.candidate
            });

            return;
          }
        }
      }
    );


    // ========================================
    // DISCONNECT
    // ========================================

    ws.on(
      "close",
      () => {

        if (!clientId) {
          return;
        }


        const user =
          users.get(clientId);


        // Ignore old socket connections.
        if (
          !user ||
          user.ws !== ws
        ) {
          return;
        }


        user.ws = null;

        user.disconnectedAt =
          Date.now();


        const peer =
          getPeer(clientId);


        if (peer) {

          send(peer.ws, {
            type:
              "peer-disconnected"
          });

        }


        console.log(
          "User disconnected:",
          clientId
        );


        setTimeout(
          cleanupExpiredUsers,
          RECONNECT_GRACE_MS + 100
        );
      }
    );


    // ========================================
    // ERROR
    // ========================================

    ws.on(
      "error",
      error => {

        console.error(
          "WebSocket error:",
          error.message
        );

      }
    );
  }
);


// Periodic cleanup.
setInterval(
  cleanupExpiredUsers,
  5000
);


// ============================================
// START SERVER
// ============================================

server.listen(
  PORT,
  HOST,
  () => {

    console.log(
      `Server running on ${HOST}:${PORT}`
    );

    console.log(
      `Single room: ${ROOM_ID}`
    );

    console.log(
      `Maximum users: ${MAX_USERS}`
    );
  }
);
