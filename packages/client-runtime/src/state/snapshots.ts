import type {
  EnvironmentId,
  OrchestrationShellSnapshot,
  PiExternalCatalogSnapshot,
  ProjectId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import type { EnvironmentShellState } from "./shell.ts";
import type { PiExternalCatalogState } from "./piNative.ts";

export function mergeExternalCatalogShells(
  internal: OrchestrationShellSnapshot | null,
  external: PiExternalCatalogSnapshot | null,
): OrchestrationShellSnapshot | null {
  if (external === null) return internal;
  const base: OrchestrationShellSnapshot = internal ?? {
    snapshotSequence: 0,
    projects: [],
    threads: [],
    updatedAt: external.updatedAt,
  };
  const internalProjectByRoot = new Map(
    base.projects.map((project) => [project.workspaceRoot, project] as const),
  );
  const projectRemap = new Map<ProjectId, ProjectId>();
  const externalProjects = external.projects.filter((project) => {
    const internalProject = internalProjectByRoot.get(project.workspaceRoot);
    if (!internalProject) return true;
    projectRemap.set(project.id, internalProject.id);
    return false;
  });
  const internalThreadIds = new Set(base.threads.map((thread) => thread.id));
  const availableProjectIds = new Set([
    ...base.projects.map((project) => project.id),
    ...externalProjects.map((project) => project.id),
  ]);
  const externalThreads = external.threads
    .filter((thread) => !internalThreadIds.has(thread.id))
    .map((thread) => {
      const projectId = projectRemap.get(thread.projectId);
      return projectId === undefined ? thread : { ...thread, projectId };
    })
    .filter((thread) => availableProjectIds.has(thread.projectId));
  return {
    ...base,
    projects: [...base.projects, ...externalProjects],
    threads: [...base.threads, ...externalThreads],
    externalOmittedProjectCount: external.omittedProjectCount,
    externalOmittedThreadCount: external.omittedThreadCount,
    updatedAt:
      base.updatedAt.localeCompare(external.updatedAt) >= 0 ? base.updatedAt : external.updatedAt,
  };
}

export function createEnvironmentSnapshotAtom<E>(
  shellStateAtom: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<AsyncResult.AsyncResult<EnvironmentShellState, E>>,
  externalCatalogStateAtom?: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<AsyncResult.AsyncResult<PiExternalCatalogState, E>>,
) {
  return Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get): OrchestrationShellSnapshot | null => {
      const internal = Option.match(AsyncResult.value(get(shellStateAtom(environmentId))), {
        onNone: () => null,
        onSome: (state) => Option.getOrNull(state.snapshot),
      });
      const external =
        externalCatalogStateAtom === undefined
          ? null
          : Option.match(AsyncResult.value(get(externalCatalogStateAtom(environmentId))), {
              onNone: () => null,
              onSome: (state) => state.snapshot,
            });
      return mergeExternalCatalogShells(internal, external);
    }).pipe(Atom.withLabel(`environment-snapshot:${environmentId}`)),
  );
}
