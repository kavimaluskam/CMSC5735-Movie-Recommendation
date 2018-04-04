module.exports = {
    entry: "./index.js",
    output: {
        filename: "./dist/bundle.js"
    },
    module: {
        loaders: [
            { test: /\.js$/, exclude: /node_modules/, loader: "babel-loader" },
            { test: /\.css$/, loader: [ 'style-loader', 'css-loader' ]},
        ]
    },
    resolve: {
        alias: {
            "vue$": "vue/dist/vue.common.js"
        }
    } 
};
