import { describe, expect, it } from "@effect/vitest";

import { decodeSupervisorListEnvelope } from "./SupervisorClient.ts";
import { GUARDED_RESUME_CAPABILITY, MANAGED_ADMISSION_PROTOCOL } from "./SupervisorProtocol.ts";

describe("SupervisorClient capability probe", () => {
  it("preserves guarded-resume capability while old daemon envelopes remain unsupported", () => {
    expect(
      decodeSupervisorListEnvelope({
        result: [],
        capabilities: { managedAdmission: MANAGED_ADMISSION_PROTOCOL },
      }).capabilities,
    ).toEqual({ managedAdmission: MANAGED_ADMISSION_PROTOCOL });

    expect(
      decodeSupervisorListEnvelope({
        result: [],
        capabilities: {
          managedAdmission: MANAGED_ADMISSION_PROTOCOL,
          guardedResume: "guarded-resume-v0",
        },
      }).capabilities,
    ).toEqual({ managedAdmission: MANAGED_ADMISSION_PROTOCOL });

    expect(
      decodeSupervisorListEnvelope({
        result: [],
        capabilities: {
          managedAdmission: MANAGED_ADMISSION_PROTOCOL,
          guardedResume: GUARDED_RESUME_CAPABILITY,
        },
      }).capabilities,
    ).toEqual({
      managedAdmission: MANAGED_ADMISSION_PROTOCOL,
      guardedResume: GUARDED_RESUME_CAPABILITY,
    });
  });
});
