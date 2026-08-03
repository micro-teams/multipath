/*
 *  Description: The line proxies' control surface, shared by every spec that stages an impairment.
 *
 *               Impairments are process state: a line told to black-hole stays black-holed until
 *               something says otherwise, across specs and across files. That makes them the one
 *               piece of shared mutable state in a suite that is otherwise self-contained, so
 *               reviving belongs in an afterEach — which runs whether or not the test reached the
 *               end — and a spec that depends on a line being healthy says so itself rather than
 *               trusting the previous file's clean-up.
 *
 *  Author(s):
 *      agent3
 */

import type { APIRequestContext } from "@playwright/test";

/** Every line the testbed starts. 9004 is the one that is meant to be dead. */
export const LINE_PORTS = [9001, 9002, 9003, 9004] as const;
export const PERMANENTLY_STALLED = 9004;

/** Black-hole or revive one line while the browser keeps running. */
export async function setStalling(
  request: APIRequestContext,
  port: number,
  stalling: boolean,
): Promise<void> {
  await request.get(`http://localhost:${port}/__line/${stalling ? "stall" : "revive"}`);
}

/** Put every line back to the topology the specs are written against. */
export async function reviveAll(request: APIRequestContext): Promise<void> {
  for (const port of LINE_PORTS) {
    await setStalling(request, port, port === PERMANENTLY_STALLED);
  }
}
