import type {
  PiNativeCommandReceipt,
  PiNativeRuntimeState,
  PiNativeStreamItem,
} from "@t3tools/contracts";

export const SUPERVISOR_PROTOCOL = "t3-control-v1";
export type SupervisorRequest =
  | { readonly type: "request"; readonly requestId: string; readonly method: "list" }
  | {
      readonly type: "request";
      readonly requestId: string;
      readonly method: "dispatch";
      readonly command: Record<string, unknown>;
    }
  | {
      readonly type: "request";
      readonly requestId: string;
      readonly method: "subscribe";
      readonly runtimeId: string;
      readonly cursor?: number;
    };
export type BridgeFrame =
  | {
      readonly type: "register";
      readonly protocol: typeof SUPERVISOR_PROTOCOL;
      readonly sessionId: string;
      readonly sessionFile: string | null;
      readonly cwd: string;
      readonly pid: number;
      readonly isStreaming?: boolean;
    }
  | {
      readonly type: "event";
      readonly protocol: typeof SUPERVISOR_PROTOCOL;
      readonly sessionId: string;
      readonly eventId: number;
      readonly event: string;
      readonly data: unknown;
    }
  | {
      readonly type: "receipt";
      readonly protocol: typeof SUPERVISOR_PROTOCOL;
      readonly commandId: string;
      readonly status: "accepted" | "submitted" | "duplicate" | "error";
      readonly error?: string;
    }
  | {
      readonly type: "unregister";
      readonly protocol: typeof SUPERVISOR_PROTOCOL;
      readonly sessionId: string;
    };
export type SupervisorResponse =
  | {
      readonly type: "response";
      readonly requestId: string;
      readonly ok: true;
      readonly result: ReadonlyArray<PiNativeRuntimeState> | PiNativeCommandReceipt;
    }
  | {
      readonly type: "response";
      readonly requestId: string;
      readonly ok: false;
      readonly error: string;
    }
  | { readonly type: "stream"; readonly requestId: string; readonly item: PiNativeStreamItem };
export const encodeLine = (value: unknown): string => `${JSON.stringify(value)}\n`;
export class JsonLineDecoder {
  #remainder = "";
  push(chunk: string): unknown[] {
    const lines = (this.#remainder + chunk).split("\n");
    this.#remainder = lines.pop() ?? "";
    return lines
      .filter((line) => line.trim() !== "")
      .flatMap((line) => {
        try {
          return [JSON.parse(line) as unknown];
        } catch {
          return [];
        }
      });
  }
}
export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
