const babelTransformer = require('@expo/metro-config/babel-transformer');

module.exports.transform = function transform({ src, filename, options }) {
  if (filename.endsWith('.html')) {
    return babelTransformer.transform({
      src: `module.exports = ${JSON.stringify(src)};`,
      filename: `${filename}.js`,
      options,
    });
  }
  return babelTransformer.transform({ src, filename, options });
};
