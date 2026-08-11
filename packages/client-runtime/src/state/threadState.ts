import type { OrchestrationActivityPageInfo, OrchestrationThread } from "@t3tools/contracts";
import * as Option from "effect/Option";

export type EnvironmentThreadStatus = "empty" | "cached" | "synchronizing" | "live" | "deleted";

export interface EnvironmentThreadState {
  readonly data: Option.Option<OrchestrationThread>;
  readonly activityPageInfo?: OrchestrationActivityPageInfo | null;
  readonly activityHistoryVersion?: number;
  readonly status: EnvironmentThreadStatus;
  readonly error: Option.Option<string>;
}

export const EMPTY_ENVIRONMENT_THREAD_STATE: EnvironmentThreadState = {
  data: Option.none(),
  activityPageInfo: null,
  activityHistoryVersion: 0,
  status: "empty",
  error: Option.none(),
};
