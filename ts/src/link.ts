// L2, browser side: one duplex link per line, always a WebSocket.
//
// A browser cannot open a raw TCP or TLS socket, so every link is a WebSocket — wss:// in
// production (the origin, or a CDN edge in front of it, terminates TLS), ws:// for a local testbed.
// The link sends its HELLO the moment it opens and thereafter carries redundant frames as binary
// messages; it reassembles frames across message boundaries with a FrameReader and hands whole
// frames up. It knows nothing of offsets or windows — that is the redundant stream's job.

import { Frame, FrameReader, encodeHello } from './frames.js';

export interface LinkHandlers {
  onOpen(): void;
  onFrame(frame: Frame): void;
  onClose(): void;
}

export interface Link {
  send(frame: Uint8Array): void;
  close(): void;
}

// openWebSocketLink dials url, announces itself with HELLO(connID, linkIdx), and drives handlers.
// It is deliberately dumb: any error becomes onClose, and the redundant stream above decides whether
// to reconnect. wsCtor is injected so tests can supply a WebSocket that runs over an in-memory pipe.
export function openWebSocketLink(
  url: string,
  connID: Uint8Array,
  linkIdx: number,
  handlers: LinkHandlers,
  wsCtor: typeof WebSocket = WebSocket,
): Link {
  const ws = new wsCtor(url);
  ws.binaryType = 'arraybuffer';
  const reader = new FrameReader();
  let closed = false;

  const fail = () => {
    if (closed) return;
    closed = true;
    handlers.onClose();
  };

  ws.onopen = () => {
    ws.send(encodeHello(connID, linkIdx));
    handlers.onOpen();
  };
  ws.onmessage = (event: MessageEvent) => {
    reader.push(new Uint8Array(event.data as ArrayBuffer));
    try {
      let frame: Frame | null;
      while ((frame = reader.next()) !== null) handlers.onFrame(frame);
    } catch {
      // A corrupt frame means this link is unreliable; drop it and let the stream recover the bytes
      // from another link or a replay.
      try {
        ws.close();
      } catch {
        /* already closing */
      }
      fail();
    }
  };
  ws.onerror = fail;
  ws.onclose = fail;

  return {
    send(frame: Uint8Array) {
      // WebSocket.send throws if the socket is not open; a link that cannot send is a dead link, and
      // the stream will notice through the missing ACKs and reconnect it.
      try {
        ws.send(frame);
      } catch {
        /* dead link; reconnect handles it */
      }
    },
    close() {
      closed = true;
      try {
        ws.close();
      } catch {
        /* already closing */
      }
    },
  };
}
