import { describe, expect, it, vi } from "vitest";
import { hedgedRead, writeWithFailover } from "../src/strategy.js";
import type { Line } from "../src/registry.js";

const lines: Line[] = [
  { id: "a", url: "https://a.example" },
  { id: "b", url: "https://b.example" },
  { id: "c", url: "https://c.example" },
];

const READ = { hedgeAfterMs: 30, readTimeoutMs: 2000 };
const WRITE = { writeTimeoutMs: 100, maxWriteAttempts: 3 };

/** An attemptor whose per-line behaviour a test dictates, recording who was asked. */
function attemptor(behaviour: Record<string, (signal: AbortSignal) => Promise<Response>>) {
  const asked: string[] = [];
  const aborted: string[] = [];
  const attempt = (line: Line, signal: AbortSignal) => {
    asked.push(line.id);
    signal.addEventListener("abort", () => aborted.push(line.id), { once: true });
    return behaviour[line.id]!(signal);
  };
  return { asked, aborted, attempt };
}

const answers =
  (id: string, afterMs = 0) =>
  () =>
    new Promise<Response>((resolve) =>
      setTimeout(() => resolve(new Response(id, { status: 200 })), afterMs),
    );
const never = () => new Promise<Response>(() => {});
const failsWith = (message: string) => () => Promise.reject(new Error(message));

describe("hedgedRead", () => {
  it("asks only the best line when it answers promptly", async () => {
    const { asked, attempt } = attemptor({ a: answers("a"), b: never, c: never });
    const response = await hedgedRead(lines, attempt, READ);

    expect(await response.text()).toBe("a");
    // The whole reason for hedging rather than fanning out: on a healthy line the extra requests
    // are never sent at all.
    expect(asked).toEqual(["a"]);
  });

  it("brings in the others once the best line has had its head start", async () => {
    const { asked, attempt } = attemptor({ a: answers("a", 500), b: answers("b"), c: never });
    const response = await hedgedRead(lines, attempt, READ);

    expect(await response.text()).toBe("b");
    expect(asked).toEqual(["a", "b", "c"]);
  });

  it("aborts the losers as soon as somebody wins", async () => {
    const { aborted, attempt } = attemptor({ a: answers("a", 500), b: answers("b"), c: never });
    await hedgedRead(lines, attempt, READ);

    // Otherwise a hedge costs N requests to completion rather than one extra for a moment.
    expect(aborted).toContain("a");
    expect(aborted).toContain("c");
  });

  /**
   * The winner must survive the cleanup. Aborting it cancels the very response being returned:
   * headers have arrived but the body is still streaming, so the caller gets an AbortError the
   * moment it reads. Losing the race is what makes a request disposable; winning it is not.
   */
  it("does not abort the line that won", async () => {
    const { aborted, attempt } = attemptor({ a: answers("a"), b: never, c: never });
    await hedgedRead(lines, attempt, READ);
    expect(aborted).not.toContain("a");
  });

  it("does not wait out the hedge delay when the first line fails immediately", async () => {
    const { asked, attempt } = attemptor({
      a: failsWith("refused"),
      b: answers("b"),
      c: never,
    });
    const started = Date.now();
    const response = await hedgedRead(lines, attempt, READ);

    expect(await response.text()).toBe("b");
    expect(asked).toContain("b");
    // Waiting for company that will never come is pure delay.
    expect(Date.now() - started).toBeLessThan(READ.hedgeAfterMs);
  });

  /**
   * An error status is an answer, not a routing failure.
   *
   * Treating a 404 as "try the next line" would ask every line for something that does not exist,
   * on every such request, and still end in a 404.
   */
  it("accepts an error status as the answer", async () => {
    const { asked, attempt } = attemptor({
      a: () => Promise.resolve(new Response("gone", { status: 404 })),
      b: answers("b"),
      c: never,
    });
    const response = await hedgedRead(lines, attempt, READ);

    expect(response.status).toBe(404);
    expect(asked).toEqual(["a"]);
  });

  it("fails only when every line has failed", async () => {
    const { attempt } = attemptor({
      a: failsWith("first"),
      b: failsWith("second"),
      c: failsWith("third"),
    });
    await expect(hedgedRead(lines, attempt, READ)).rejects.toThrow("first");
  });

  it("survives when only the last line works", async () => {
    const { attempt } = attemptor({
      a: failsWith("no"),
      b: failsWith("no"),
      c: answers("c"),
    });
    expect(await (await hedgedRead(lines, attempt, READ)).text()).toBe("c");
  });

  it("gives up rather than hanging when nothing ever answers", async () => {
    const { attempt } = attemptor({ a: never, b: never, c: never });
    await expect(
      hedgedRead(lines, attempt, { hedgeAfterMs: 5, readTimeoutMs: 40 }),
    ).rejects.toThrow(/timed out/);
  });

  it("honours a caller who cancels", async () => {
    const controller = new AbortController();
    const { aborted, attempt } = attemptor({ a: never, b: never, c: never });
    const promise = hedgedRead(lines, attempt, READ, controller.signal);
    setTimeout(() => controller.abort(), 10);

    await expect(promise).rejects.toThrow();
    expect(aborted).toContain("a");
  });

  it("refuses an empty line list rather than resolving to nothing", async () => {
    const { attempt } = attemptor({});
    await expect(hedgedRead([], attempt, READ)).rejects.toThrow(/no line/);
  });
});

describe("writeWithFailover", () => {
  it("uses one line when that line works", async () => {
    const { asked, attempt } = attemptor({ a: answers("a"), b: never, c: never });
    expect(await (await writeWithFailover(lines, attempt, WRITE)).text()).toBe("a");
    // Never raced: two writes are two writes, however good the de-duplication is.
    expect(asked).toEqual(["a"]);
  });

  it("moves to the next line when a line cannot be reached", async () => {
    const { asked, attempt } = attemptor({
      a: failsWith("connection refused"),
      b: answers("b"),
      c: never,
    });
    expect(await (await writeWithFailover(lines, attempt, WRITE)).text()).toBe("b");
    expect(asked).toEqual(["a", "b"]);
  });

  it("moves on when a line accepts the request and then goes quiet", async () => {
    const { asked, attempt } = attemptor({ a: never, b: answers("b"), c: never });
    expect(await (await writeWithFailover(lines, attempt, WRITE)).text()).toBe("b");
    expect(asked).toEqual(["a", "b"]);
  });

  /**
   * A response ends the matter, whatever it says.
   *
   * Only a transport failure leaves it unknown whether the write happened. A 500 is an answer: the
   * request arrived and the server decided. Re-sending it over another route asks the same server
   * the same question, and whether to retry is the caller's business.
   */
  it("does not fail over on a server error", async () => {
    const { asked, attempt } = attemptor({
      a: () => Promise.resolve(new Response("boom", { status: 500 })),
      b: answers("b"),
      c: never,
    });
    const response = await writeWithFailover(lines, attempt, WRITE);

    expect(response.status).toBe(500);
    expect(asked).toEqual(["a"]);
  });

  it("stops after the configured number of attempts", async () => {
    const { asked, attempt } = attemptor({
      a: failsWith("no"),
      b: failsWith("no"),
      c: answers("c"),
    });
    await expect(
      writeWithFailover(lines, attempt, { ...WRITE, maxWriteAttempts: 2 }),
    ).rejects.toThrow("no");
    // c is never asked: a client that keeps failing over forever is a client that hammers a
    // struggling origin from every direction at once.
    expect(asked).toEqual(["a", "b"]);
  });

  it("reports the failure when no line works", async () => {
    const { attempt } = attemptor({
      a: failsWith("one"),
      b: failsWith("two"),
      c: failsWith("three"),
    });
    await expect(writeWithFailover(lines, attempt, WRITE)).rejects.toThrow("three");
  });

  it("stops immediately when the caller cancels, rather than trying the next line", async () => {
    const controller = new AbortController();
    const { asked, attempt } = attemptor({ a: never, b: answers("b"), c: never });
    const promise = writeWithFailover(lines, attempt, WRITE, controller.signal);
    setTimeout(() => controller.abort(), 10);

    await expect(promise).rejects.toThrow();
    // Failing over after a cancellation would be sending a write the caller asked us not to send.
    expect(asked).toEqual(["a"]);
  });

  it("refuses an empty line list", async () => {
    const { attempt } = attemptor({});
    await expect(writeWithFailover([], attempt, WRITE)).rejects.toThrow(/no line/);
  });
});

vi.setConfig({ testTimeout: 10_000 });
