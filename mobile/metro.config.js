const { getDefaultConfig } = require('@expo/metro-config');

const config = getDefaultConfig(__dirname);

// chart.html must travel inside the JS bundle. Treating it as a normal Expo
// asset makes iOS fetch it from Metro at runtime, which fails when the device
// cannot reach Metro's asset endpoint even though the JS bundle is connected.
config.transformer.babelTransformerPath = require.resolve('./html-transformer');
config.resolver.assetExts = config.resolver.assetExts.filter(ext => ext !== 'html');
config.resolver.sourceExts.push('html');

module.exports = config;
