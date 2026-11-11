/**
 * Scenario registry, in run order. Add a scenario by creating a module in
 * this directory and listing it here (see README.md, "Adding a scenario").
 */
import thinSdkParity from './thin-sdk-parity.mjs';
import reducerSecondClient from './reducer-second-client.mjs';
import replicaResume from './replica-resume.mjs';
import reducerErrors from './reducer-errors.mjs';
import presence from './presence.mjs';
import liveQueryWindow from './live-query-window.mjs';
import controlAuth from './control-auth.mjs';
import controlTenants from './control-tenants.mjs';
import controlInvitations from './control-invitations.mjs';
import controlSupport from './control-support.mjs';
import controlExternalAuth from './control-external-auth.mjs';
import controlNativeAuth from './control-native-auth.mjs';
import controlSessions from './control-sessions.mjs';
import receiptLostReply from './receipt-lost-reply.mjs';
import receiptOwnerSwitch from './receipt-owner-switch.mjs';
import receiptUpgrade from './receipt-upgrade.mjs';
import receiptCrash from './receipt-crash.mjs';
import storageSql from './storage-sql.mjs';
import moduleHost from './module-host.mjs';
import sessionHandshake from './session-handshake.mjs';
import sessionFrames from './session-frames.mjs';
import changeCapture from './change-capture.mjs';
import visibilityMasks from './visibility-masks.mjs';
import receiptExpiry from './receipt-expiry.mjs';
import scheduler from './scheduler.mjs';
import liveQueryTaskGrid from './live-query-task-grid.mjs';
import operatorRoutes from './operator-routes.mjs';
import presenceEdges from './presence-edges.mjs';
import actions from './actions.mjs';

export const SCENARIOS = [
  thinSdkParity,
  reducerSecondClient,
  replicaResume,
  reducerErrors,
  presence,
  liveQueryWindow,
  controlAuth,
  controlTenants,
  controlInvitations,
  controlSupport,
  controlExternalAuth,
  controlNativeAuth,
  controlSessions,
  receiptLostReply,
  receiptOwnerSwitch,
  receiptUpgrade,
  storageSql,
  moduleHost,
  sessionHandshake,
  sessionFrames,
  changeCapture,
  visibilityMasks,
  receiptExpiry,
  scheduler,
  liveQueryTaskGrid,
  // K19: deploy and operator routes over HTTP (no seeded tenants).
  operatorRoutes,
  presenceEdges,
  actions,
  // Restarts the backend: keep last so a failure cannot disturb the others.
  receiptCrash,
];
