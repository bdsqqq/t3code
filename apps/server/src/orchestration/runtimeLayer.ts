import * as Layer from "effect/Layer";

import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { PiExternalThreadSource } from "../piNative/PiExternalThreadSource.ts";
import { SessionCatalog } from "../piNative/SessionCatalog.ts";
import { SupervisorClient } from "../piNative/SupervisorClient.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import { ProviderSessionDirectoryLive } from "../provider/Layers/ProviderSessionDirectory.ts";
import { PiExternalLifecycleOverrideRepositoryLive } from "../persistence/Layers/PiExternalLifecycleOverrides.ts";

export const OrchestrationEventInfrastructureLayerLive = Layer.mergeAll(
  OrchestrationEventStoreLive,
  OrchestrationCommandReceiptRepositoryLive,
);

export const OrchestrationProjectionPipelineLayerLive = OrchestrationProjectionPipelineLive.pipe(
  Layer.provide(OrchestrationEventStoreLive),
);

export const OrchestrationInfrastructureLayerLive = Layer.mergeAll(
  OrchestrationProjectionSnapshotQueryLive,
  OrchestrationEventInfrastructureLayerLive,
  OrchestrationProjectionPipelineLayerLive,
);

const PiExternalThreadSourceLive = PiExternalThreadSource.layer.pipe(
  Layer.provide(SessionCatalog.layer()),
  Layer.provide(SupervisorClient.layer),
  Layer.provide(OrchestrationProjectionSnapshotQueryLive),
  Layer.provide(ProviderSessionDirectoryLive.pipe(Layer.provide(ProviderSessionRuntime.layer))),
  Layer.provide(PiExternalLifecycleOverrideRepositoryLive),
);

export const OrchestrationLayerLive = Layer.mergeAll(
  OrchestrationInfrastructureLayerLive,
  OrchestrationEngineLive.pipe(Layer.provide(OrchestrationInfrastructureLayerLive)),
  PiExternalThreadSourceLive,
);
