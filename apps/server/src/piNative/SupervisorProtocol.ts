import type {
  CommandId,
  PiNativeEventId,
  PiNativeJsonlEntry,
  PiNativeRuntimeId,
  PiNativeSessionKey,
  PiThreadLifecycleData,
} from "@t3tools/contracts";
import { MANAGED_TURN_ADMISSION_PROTOCOL } from "@t3tools/contracts";

export const SUPERVISOR_PROTOCOL = "t3-control-v2";
export const MANAGED_ADMISSION_PROTOCOL = MANAGED_TURN_ADMISSION_PROTOCOL;
export const GUARDED_RESUME_CAPABILITY = "guarded-resume-v1";
export const SUPERVISOR_MAX_LINE_BYTES = 112 * 1024 * 1024;
export const SUPERVISOR_MAX_STREAM_ITEM_BYTES = 32 * 1024 * 1024;

export interface SupervisorCapabilities {
  readonly managedAdmission: typeof MANAGED_ADMISSION_PROTOCOL;
  readonly guardedResume: typeof GUARDED_RESUME_CAPABILITY;
}
export type SupervisorCapabilityProbe = Partial<SupervisorCapabilities>;

export interface ManagedPiTurnStartPayload {
  readonly type: "managed-pi.turn-start";
  readonly providerInstanceId: string;
  readonly threadId: string;
  readonly session: {
    readonly schemaVersion: 1;
    readonly sessionFile: string;
    readonly sessionId: string;
  };
  readonly message: string;
  readonly attachments: ReadonlyArray<{
    readonly type: "image";
    readonly id: string;
    readonly name: string;
    readonly mimeType: string;
    readonly sizeBytes: number;
  }>;
  readonly model: {
    readonly provider: string;
    readonly modelId: string;
  };
  // null is the stable accepted instruction to preserve Pi's configured level.
  readonly thinkingLevel: "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | null;
  readonly interactionMode: "default" | "plan";
}

export interface ManagedClaimRequest {
  readonly protocol: typeof MANAGED_ADMISSION_PROTOCOL;
  readonly intent: "execute" | "recover-existing";
  readonly operationKey: string;
  readonly payload: ManagedPiTurnStartPayload;
}

export interface ManagedCompletedReceipt {
  readonly turnId: string;
}

export type ManagedOperationState =
  | { readonly status: "absent" }
  | { readonly status: "delivering" }
  | { readonly status: "completed"; readonly receipt: ManagedCompletedReceipt }
  | { readonly status: "rejected"; readonly error: string }
  | { readonly status: "indeterminate"; readonly error: string };

export type ManagedClaimResponse =
  | {
      readonly status: "granted";
      readonly operationKey: string;
      readonly leaseToken: string;
    }
  | ManagedOperationState
  | { readonly status: "conflict"; readonly error: string };

export type ManagedFinalization =
  | { readonly status: "completed"; readonly receipt: ManagedCompletedReceipt }
  | { readonly status: "rejected"; readonly error: string }
  | { readonly status: "indeterminate"; readonly error: string };

export interface ManagedFinalizeRequest {
  readonly protocol: typeof MANAGED_ADMISSION_PROTOCOL;
  readonly operationKey: string;
  readonly leaseToken: string;
  readonly finalization: ManagedFinalization;
}

export type ManagedFinalizeResponse =
  | {
      readonly status: "finalized";
      readonly operation: Exclude<ManagedOperationState, { readonly status: "absent" }>;
    }
  | { readonly status: "staleLease"; readonly operation: ManagedOperationState };

export type SupervisorCommand =
  | {
      readonly type: "start";
      readonly commandId: CommandId;
      readonly cwd: string;
      readonly sessionFile?: string;
    }
  | {
      readonly type: "resumeAndSend";
      readonly commandId: CommandId;
      readonly sessionKey: PiNativeSessionKey;
      readonly sessionFile: string;
      readonly cwd: string;
      readonly message: string;
      readonly streamingBehavior: "steer" | "followUp";
      readonly images?: ReadonlyArray<{
        readonly type: "image";
        readonly data: string;
        readonly mimeType: string;
      }>;
    }
  | {
      readonly type: "send" | "steer" | "followUp";
      readonly commandId: CommandId;
      readonly runtimeId: PiNativeRuntimeId;
      readonly message: string;
      readonly images?: ReadonlyArray<{
        readonly type: "image";
        readonly data: string;
        readonly mimeType: string;
      }>;
    }
  | {
      readonly type: "abort" | "shutdown";
      readonly commandId: CommandId;
      readonly runtimeId: PiNativeRuntimeId;
    }
  | {
      readonly type: "setLifecycle";
      readonly commandId: CommandId;
      readonly runtimeId: PiNativeRuntimeId;
      readonly lifecycle: PiThreadLifecycleData;
    };
export interface SupervisorRuntimeState {
  readonly runtimeId: PiNativeRuntimeId;
  readonly sessionKey?: PiNativeSessionKey;
  readonly sessionFile?: string;
  readonly cwd?: string;
  readonly writerKind: "rpc" | "tuiBridge";
  readonly status: "starting" | "idle" | "streaming" | "exited";
  readonly sequence: number;
  readonly state?: unknown;
  readonly overlay?: {
    readonly isStreaming: boolean;
    readonly pendingMessageCount: number;
    readonly lastEventType?: string;
  };
}
export interface SupervisorCommandReceipt {
  readonly commandId: CommandId;
  readonly status: "started" | "completed" | "rejected" | "indeterminate";
  readonly runtimeId?: PiNativeRuntimeId;
  readonly result?: unknown;
  readonly error?: string;
}
export interface SupervisorStreamEvent {
  readonly type: "event";
  readonly runtimeId: PiNativeRuntimeId;
  readonly sequence: number;
  readonly eventId: PiNativeEventId;
  readonly event: unknown;
}
export type SupervisorStreamItem =
  | {
      readonly type: "snapshot";
      readonly runtime: SupervisorRuntimeState;
      readonly entries: ReadonlyArray<PiNativeJsonlEntry>;
      readonly events: ReadonlyArray<SupervisorStreamEvent>;
      readonly omittedOverlayEventCount: number;
    }
  | SupervisorStreamEvent
  | {
      readonly type: "entries";
      readonly runtimeId: PiNativeRuntimeId;
      readonly sequence: number;
      readonly entries: ReadonlyArray<PiNativeJsonlEntry>;
    }
  | {
      readonly type: "synchronized";
      readonly runtimeId: PiNativeRuntimeId;
      readonly sequence: number;
    }
  | {
      readonly type: "exited";
      readonly runtimeId: PiNativeRuntimeId;
      readonly sequence: number;
      readonly exitCode?: number;
    };
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
    }
  | {
      readonly type: "request";
      readonly requestId: string;
      readonly method: "claimManaged";
      readonly claim: ManagedClaimRequest;
    }
  | {
      readonly type: "request";
      readonly requestId: string;
      readonly method: "finalizeManaged";
      readonly finalization: ManagedFinalizeRequest;
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
      readonly steering?: ReadonlyArray<string>;
      readonly followUp?: ReadonlyArray<string>;
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
      readonly result:
        | ReadonlyArray<SupervisorRuntimeState>
        | SupervisorCommandReceipt
        | ManagedClaimResponse
        | ManagedFinalizeResponse;
      readonly capabilities?: SupervisorCapabilities;
    }
  | {
      readonly type: "response";
      readonly requestId: string;
      readonly ok: false;
      readonly error: string;
    }
  | { readonly type: "stream"; readonly requestId: string; readonly item: SupervisorStreamItem };
export function decodeSupervisorCapabilityProbe(value: unknown): SupervisorCapabilityProbe {
  if (!isRecord(value)) return {};
  return {
    ...(value.managedAdmission === MANAGED_ADMISSION_PROTOCOL
      ? { managedAdmission: MANAGED_ADMISSION_PROTOCOL }
      : {}),
    ...(value.guardedResume === GUARDED_RESUME_CAPABILITY
      ? { guardedResume: GUARDED_RESUME_CAPABILITY }
      : {}),
  };
}

export const supportsGuardedResume = (capabilities: SupervisorCapabilityProbe): boolean =>
  capabilities.guardedResume === GUARDED_RESUME_CAPABILITY;

export const encodeLine = (value: unknown): string => `${JSON.stringify(value)}\n`;
export class SupervisorStreamBuffer {
  readonly #maxBytes: number;
  readonly #items: Array<{ readonly item: SupervisorStreamItem; readonly bytes: number }> = [];
  #bytes = 0;

  constructor(maxBytes = SUPERVISOR_MAX_STREAM_ITEM_BYTES) {
    this.#maxBytes = maxBytes;
  }

  get length(): number {
    return this.#items.length;
  }

  push(item: SupervisorStreamItem): boolean {
    const bytes = Buffer.byteLength(JSON.stringify(item), "utf8");
    if (this.#bytes + bytes > this.#maxBytes) return false;
    this.#items.push({ item, bytes });
    this.#bytes += bytes;
    return true;
  }

  shift(): SupervisorStreamItem | undefined {
    const entry = this.#items.shift();
    if (!entry) return undefined;
    this.#bytes -= entry.bytes;
    return entry.item;
  }
}
export class JsonLineDecoder {
  #remainder = "";
  readonly #maxLineBytes: number;
  constructor(maxLineBytes = SUPERVISOR_MAX_LINE_BYTES) {
    this.#maxLineBytes = maxLineBytes;
  }
  push(chunk: string): unknown[] {
    const buffered = this.#remainder + chunk;
    if (Buffer.byteLength(buffered, "utf8") > this.#maxLineBytes) {
      this.#remainder = "";
      throw new Error("pi supervisor frame exceeds the serialized-byte ceiling");
    }
    const lines = buffered.split("\n");
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
