// SPDX-FileCopyrightText: 2020 Serokell <https://serokell.io/>
//
// SPDX-License-Identifier: MPL-2.0

use serde::Serialize;

/// Identifies a phase of the deployment pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DeployStep {
    /// Checking if the Nix installation supports flakes.
    FlakeSupport,
    /// Running `nix flake check`.
    CheckDeployment,
    /// Evaluating the flake's `deploy` attribute with `nix eval`.
    EvaluateData,
    /// Building a profile derivation.
    Build,
    /// Copying a profile closure to a remote node.
    Push,
    /// Activating a profile on a node over SSH.
    Activate,
    /// Confirming a magic-rollback activation.
    Confirm,
    /// Rolling back / revoking a profile.
    Revoke,
}

/// An event emitted during a deployment, suitable for forwarding
/// over an IPC channel or serialising to JSON.
#[derive(Debug, Clone, Serialize)]
pub struct DeployEvent {
    pub step: DeployStep,
    pub node: Option<String>,
    pub profile: Option<String>,
    #[serde(flatten)]
    pub outcome: DeployOutcome,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DeployOutcome {
    Started,
    Succeeded,
    Failed {
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

impl DeployEvent {
    pub fn new(
        step: DeployStep,
        node: Option<String>,
        profile: Option<String>,
        outcome: DeployOutcome,
    ) -> Self {
        Self {
            step,
            node,
            profile,
            outcome,
        }
    }
}
