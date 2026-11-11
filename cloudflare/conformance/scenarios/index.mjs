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
import receiptLostReply from './receipt-lost-reply.mjs';
import receiptOwnerSwitch from './receipt-owner-switch.mjs';
import receiptUpgrade from './receipt-upgrade.mjs';
import receiptCrash from './receipt-crash.mjs';
import storageSql from './storage-sql.mjs';

export const SCENARIOS = [
  thinSdkParity,
  reducerSecondClient,
  replicaResume,
  reducerErrors,
  presence,
  liveQueryWindow,
  controlAuth,
  receiptLostReply,
  receiptOwnerSwitch,
  receiptUpgrade,
  storageSql,
  // Restarts the backend: keep last so a failure cannot disturb the others.
  receiptCrash,
];
