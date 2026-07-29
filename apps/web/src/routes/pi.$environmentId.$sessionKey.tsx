import { createFileRoute } from "@tanstack/react-router";

import { NativePiDetail } from "../components/NativePi";

export const Route = createFileRoute("/pi/$environmentId/$sessionKey")({
  component: NativePiDetailRoute,
});

function NativePiDetailRoute() {
  const params = Route.useParams();
  return (
    <NativePiDetail
      key={`${params.environmentId}:${params.sessionKey}`}
      environmentId={params.environmentId}
      sessionKey={params.sessionKey}
    />
  );
}
