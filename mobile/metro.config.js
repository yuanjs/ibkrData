const { getDefaultConfig } = require('@expo/metro-config');

const config = getDefaultConfig(__dirname);

// Allow .js files as assets so lightweight-charts can be loaded
// locally in the WebView instead of from a remote CDN.
config.resolver.assetExts.push('js');

module.exports = config;
