import { createPiNativeEnvironmentAtoms } from "@t3tools/client-runtime/state/pi-native";

import { connectionAtomRuntime } from "../connection/runtime";

export const piNativeEnvironment = createPiNativeEnvironmentAtoms(connectionAtomRuntime);
