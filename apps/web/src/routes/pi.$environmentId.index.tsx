import { createFileRoute } from "@tanstack/react-router";
import { NativePiList } from "../components/NativePi";
export const Route = createFileRoute("/pi/$environmentId/")({
  component: () => <NativePiList environmentId={Route.useParams().environmentId} />,
});
