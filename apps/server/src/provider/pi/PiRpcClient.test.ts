import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import {
  makePiRpcTransport,
  PiRpcCommandError,
  PiRpcProtocolError,
  PiRpcRequestTimeoutError,
} from "./PiRpcClient.ts";

const bytes = (text: string) => new TextEncoder().encode(text);

const makeIo = Effect.fn("PiRpcClient.test.makeIo")(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const writes = yield* Queue.unbounded<string>();
  const decoder = new TextDecoder();
  return {
    stdout,
    writes,
    io: {
      stdout: Stream.fromQueue(stdout).pipe(
        Stream.mapError((cause) => new PiRpcProtocolError({ detail: "test stdout failed", cause })),
      ),
      stdin: Sink.forEach((chunk: Uint8Array) =>
        Queue.offer(writes, decoder.decode(chunk)).pipe(Effect.asVoid),
      ),
    },
  } as const;
});

describe("PiRpcClient transport", () => {
  it.effect("frames chunks and preserves unicode line separators", () =>
    Effect.gen(function* () {
      const test = yield* makeIo();
      const client = yield* makePiRpcTransport(test.io);
      const eventFiber = yield* Stream.runCollect(client.events.pipe(Stream.take(1))).pipe(
        Effect.forkScoped,
      );
      yield* Queue.offer(test.stdout, bytes('{"type":"message","text":"a'));
      yield* Queue.offer(test.stdout, bytes("\u2028b\u2029c" + '"}\r\n'));
      const events = yield* Fiber.join(eventFiber);
      expect(Array.from(events)).toEqual([{ type: "message", text: "a\u2028b\u2029c" }]);
    }).pipe(Effect.scoped),
  );

  it.effect("surfaces malformed JSON without blocking a correlated response", () =>
    Effect.gen(function* () {
      const test = yield* makeIo();
      const client = yield* makePiRpcTransport(test.io);
      const eventsFiber = yield* Stream.runCollect(client.events.pipe(Stream.take(1))).pipe(
        Effect.forkScoped,
      );
      const stateFiber = yield* client.getState().pipe(Effect.forkScoped);
      const request = yield* Queue.take(test.writes);
      expect(request).toContain('"type":"get_state"');
      yield* Queue.offer(test.stdout, bytes("not json\n"));
      yield* Queue.offer(
        test.stdout,
        bytes(
          '{"type":"response","command":"get_state","success":true,"id":"t3-pi-1","data":{"sessionId":"s1"}}\n',
        ),
      );
      const state = yield* Fiber.join(stateFiber);
      expect(state.sessionId).toBe("s1");
      expect(Array.from(yield* Fiber.join(eventsFiber))[0]).toMatchObject({
        _tag: "PiRpcProtocolFailureEvent",
        reason: "MalformedJson",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("bounds oversized remainders and resumes at the next line", () =>
    Effect.gen(function* () {
      const test = yield* makeIo();
      const client = yield* makePiRpcTransport(test.io, { maxLineLength: 8 });
      const eventsFiber = yield* Stream.runCollect(client.events.pipe(Stream.take(2))).pipe(
        Effect.forkScoped,
      );
      yield* Queue.offer(test.stdout, bytes("123456789"));
      yield* Queue.offer(test.stdout, bytes('\n{"x":1}\n'));
      expect(Array.from(yield* Fiber.join(eventsFiber))).toEqual([
        expect.objectContaining({ reason: "LineTooLong" }),
        { x: 1 },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("reports command failures without poisoning later requests", () =>
    Effect.gen(function* () {
      const test = yield* makeIo();
      const client = yield* makePiRpcTransport(test.io);
      const failedFiber = yield* client.getState().pipe(Effect.flip, Effect.forkScoped);
      yield* Queue.take(test.writes);
      yield* Queue.offer(
        test.stdout,
        bytes(
          '{"type":"response","command":"get_state","success":false,"id":"t3-pi-1","error":"no state"}\n',
        ),
      );
      expect(yield* Fiber.join(failedFiber)).toBeInstanceOf(PiRpcCommandError);

      const nextFiber = yield* client.getState().pipe(Effect.forkScoped);
      yield* Queue.take(test.writes);
      yield* Queue.offer(
        test.stdout,
        bytes(
          '{"type":"response","command":"get_state","success":true,"id":"t3-pi-2","data":{"sessionId":"s2"}}\n',
        ),
      );
      expect((yield* Fiber.join(nextFiber)).sessionId).toBe("s2");
    }).pipe(Effect.scoped),
  );

  it.effect("times out requests and ignores their late responses", () =>
    Effect.gen(function* () {
      const test = yield* makeIo();
      const client = yield* makePiRpcTransport(test.io, { requestTimeoutMs: 1 });
      const timedOutFiber = yield* client.getState().pipe(Effect.flip, Effect.forkScoped);
      yield* Queue.take(test.writes);
      yield* TestClock.adjust("2 millis");
      expect(yield* Fiber.join(timedOutFiber)).toBeInstanceOf(PiRpcRequestTimeoutError);

      yield* Queue.offer(
        test.stdout,
        bytes(
          '{"type":"response","command":"get_state","success":true,"id":"t3-pi-1","data":{"sessionId":"late"}}\n',
        ),
      );
      const nextFiber = yield* client.getState().pipe(Effect.forkScoped);
      yield* Queue.take(test.writes);
      yield* Queue.offer(
        test.stdout,
        bytes(
          '{"type":"response","command":"get_state","success":true,"id":"t3-pi-2","data":{"sessionId":"current"}}\n',
        ),
      );
      expect((yield* Fiber.join(nextFiber)).sessionId).toBe("current");
    }).pipe(Effect.scoped),
  );
});
