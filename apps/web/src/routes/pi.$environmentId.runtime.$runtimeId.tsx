import { createFileRoute } from "@tanstack/react-router";

import { NativePiRuntimeDetail } from "../components/NativePi";

export const Route = createFileRoute("/pi/$environmentId/runtime/$runtimeId")({
  component: NativePiRuntimeDetailRoute,
});

function NativePiRuntimeDetailRoute() {
  const params = Route.useParams();
  return (
    <NativePiRuntimeDetail environmentId={params.environmentId} runtimeId={params.runtimeId} />
  );
}
