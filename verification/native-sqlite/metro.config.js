const { getDefaultConfig } = require('expo/metro-config');
const config = getDefaultConfig(__dirname);
// Native Expo SQLite is resolved by Metro's Android platform selection. No web
// SQLite engine, OPFS adapter or .wasm asset configuration belongs in this app.
module.exports = config;
