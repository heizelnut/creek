// Dependency-free helpers for the Creek browser load/perf pages.
// No CDNs, no build step: plain ES modules.

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// Encode a command as a RESP2 array of bulk strings (same bytes as TCP).
export function encodeCommand(name, args = []) {
  const parts = [name, ...args.map(String)];
  const chunks = [encoder.encode(`*${parts.length}\r\n`)];
  for (const part of parts) {
    const bytes = encoder.encode(part);
    chunks.push(encoder.encode(`$${bytes.length}\r\n`), bytes, encoder.encode("\r\n"));
  }
  let size = 0;
  for (const chunk of chunks) size += chunk.length;
  const out = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

// Decode one RESP2 reply. Throws on an error reply.
export function decodeReply(data) {
  const view = data instanceof Uint8Array ? data : new Uint8Array(data);
  let pos = 0;

  function readLine() {
    let end = pos;
    while (end + 1 < view.length && !(view[end] === 13 && view[end + 1] === 10)) end++;
    const line = decoder.decode(view.subarray(pos, end));
    pos = end + 2;
    return line;
  }

  function parse() {
    const line = readLine();
    const kind = line[0];
    const rest = line.slice(1);
    if (kind === "+") return rest;
    if (kind === "-") throw new Error(rest);
    if (kind === ":") return Number(rest);
    if (kind === "$") {
      const length = Number(rest);
      if (length < 0) return null;
      const value = decoder.decode(view.subarray(pos, pos + length));
      pos += length + 2;
      return value;
    }
    if (kind === "*") {
      const count = Number(rest);
      const items = [];
      for (let i = 0; i < count; i++) items.push(parse());
      return items;
    }
    throw new Error("malformed reply");
  }

  return parse();
}

// A WebSocket client with a FIFO of pending replies (one reply per frame).
export class CreekClient {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this.waiters = [];
  }

  open() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);
      this.ws.binaryType = "arraybuffer";
      this.ws.onopen = () => resolve(this);
      this.ws.onerror = () => reject(new Error(`cannot connect to ${this.url}`));
      this.ws.onmessage = (event) => {
        const waiter = this.waiters.shift();
        if (!waiter) return;
        try {
          waiter.resolve(decodeReply(event.data));
        } catch (error) {
          waiter.reject(error);
        }
      };
    });
  }

  request(name, args = []) {
    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
      this.ws.send(encodeCommand(name, args));
    });
  }

  close() {
    if (this.ws) this.ws.close();
  }
}

export function percentile(sorted, fraction) {
  if (sorted.length === 0) return 0;
  const index = (sorted.length - 1) * fraction;
  const lower = Math.floor(index);
  const upper = Math.min(lower + 1, sorted.length - 1);
  return sorted[lower] * (1 - (index - lower)) + sorted[upper] * (index - lower);
}

// Draw a simple bar chart onto a canvas. `values` are in the unit of `suffix`.
export function drawBars(canvas, labels, values, color = "#4ea1ff") {
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  const max = Math.max(1, ...values);
  const slot = width / values.length;
  ctx.font = "12px system-ui, sans-serif";
  ctx.textAlign = "center";
  values.forEach((value, index) => {
    const barHeight = (value / max) * (height - 34);
    const x = index * slot + slot * 0.2;
    ctx.fillStyle = color;
    ctx.fillRect(x, height - 22 - barHeight, slot * 0.6, barHeight);
    ctx.fillStyle = "#9fb3c8";
    ctx.fillText(labels[index], index * slot + slot / 2, height - 6);
    ctx.fillStyle = "#e6edf3";
    ctx.fillText(String(Math.round(value)), index * slot + slot / 2, height - 28 - barHeight);
  });
}

// Latency histogram buckets (milliseconds).
export function histogram(latencies, buckets = 10) {
  const max = Math.max(0.001, ...latencies);
  const counts = new Array(buckets).fill(0);
  for (const value of latencies) {
    const index = Math.min(buckets - 1, Math.floor((value / max) * buckets));
    counts[index] += 1;
  }
  return counts;
}

export function defaultUrl() {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${location.host}/ws`;
}
