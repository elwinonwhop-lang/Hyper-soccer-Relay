import asyncio
from http import HTTPStatus
import json
import os
import random
import websockets

ROOMS = {}
MAX_PLAYERS = 4


def make_code():
  while True:
    code = "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=5))
    if code not in ROOMS:
      return code


def health_check(connection, request):
  if request.headers.get("Upgrade", "").lower() != "websocket":
    return connection.respond(HTTPStatus.OK, "Relay OK\n")


async def handler(ws):
  room_code = None
  slot = None
  try:
    async for raw in ws:
      msg = json.loads(raw)
      action = msg.get("relay")

      if action == "create":
        room_code = make_code()
        ROOMS[room_code] = [ws, None, None, None]
        slot = 0
        await ws.send(
            json.dumps({"relay": "created", "code": room_code, "slot": 0})
        )

      elif action == "join":
        code = str(msg.get("code", "")).upper().strip()
        room = ROOMS.get(code)
        if not room:
          await ws.send(json.dumps({"relay": "error", "msg": "Room not found"}))
        elif ws in room:
          room_code = code
          slot = room.index(ws)
          await ws.send(
              json.dumps({"relay": "joined", "code": code, "slot": slot})
          )
        else:
          open_slot = next(
              (i for i in range(1, MAX_PLAYERS) if room[i] is None), None
          )
          if open_slot is None:
            await ws.send(
                json.dumps({"relay": "error", "msg": "Room is full (4/4)"})
            )
          else:
            room_code = code
            room[open_slot] = ws
            slot = open_slot
            await ws.send(
                json.dumps({"relay": "joined", "code": code, "slot": slot})
            )

      elif room_code and room_code in ROOMS and slot is not None:
        room = ROOMS[room_code]
        if slot == 0:
          # Host broadcasts snapshots/lobby updates to all connected guests
          for i in range(1, MAX_PLAYERS):
            if room[i] is not None:
              try:
                await room[i].send(raw)
              except Exception:
                pass
        else:
          # Guests forward their inputs to the Host, tagged with their slot ID (1, 2, or 3)
          msg["slot"] = slot
          tagged = json.dumps(msg, separators=(",", ":"))
          if room[0] is not None:
            try:
              await room[0].send(tagged)
            except Exception:
              pass

  except websockets.ConnectionClosed:
    pass
  finally:
    if room_code and room_code in ROOMS and slot is not None:
      room = ROOMS[room_code]
      if slot == 0:
        # Host disconnected: notify all guests and close the room
        for i in range(1, MAX_PLAYERS):
          if room[i] is not None:
            try:
              await room[i].send(json.dumps({"g": "HSOC", "t": "closed"}))
            except Exception:
              pass
        del ROOMS[room_code]
      else:
        # A guest disconnected: free their slot and tell the Host which slot left
        if room[slot] == ws:
          room[slot] = None
        if room[0] is not None:
          try:
            await room[0].send(
                json.dumps({"g": "HSOC", "t": "leave", "slot": slot})
            )
          except Exception:
            pass


async def main():
  port = int(os.environ.get("PORT", 8765))
  async with websockets.serve(
      handler, "0.0.0.0", port, process_request=health_check
  ):
    await asyncio.Future()


if __name__ == "__main__":
  asyncio.run(main())
