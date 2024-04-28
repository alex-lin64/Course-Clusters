#!/usr/bin/env node

const http = require('node:http');

const args = require('yargs').argv;

const util = require('./distribution/util/util');
const local = require('./distribution/local');
const {putInDistribution} = require('./distribution/all');

function optional(key, value) {
  return value ? {[key]: value} : {};
}

const argsConfig = args.config ? util.deserialize(args.config) : {};

global.nodeConfig = {
  ip: '0.0.0.0',
  port: 8080,
  onStart: (server, node, callback) => console.log('Node started!'),
  ...optional('ip', args.ip),
  ...optional('port', parseInt(args.port)),
  ...optional('ip', argsConfig.ip),
  ...optional('port', argsConfig.port),
  ...optional('onStart', argsConfig.onStart),
};

const hostOn = argsConfig.hostOn;
const known = argsConfig.known || [];

async function handleConnection(req, server) {
  await local.async.status.incrementCount();
  let message = [];
  req.on('data', (chunk) => message.push(chunk));
  await new Promise((resolve) => req.on('end', resolve));
  message = Buffer.concat(message).toString();
  message = util.deserialize(message, (expr) => eval(expr));
  let [, service, method] = req.url.match(/^\/(.*)\/(.*)$/);
  service = await local.async.routes.get(service);
  const [e, v] = await new Promise((callback) =>
    service[method].call(service, ...message, (...ev) => callback(ev)));
  if (e instanceof Error && e.message === 'handleClose') {
    const [closeToken, node] = message;
    const remote = {node, service: 'handleClose', method: 'handleClose'};
    server.close(() => {
      // no await
      local.async.comm.send([closeToken], remote);
    });
    return [null, global.nodeConfig];
  }
  return [e, v];
}

function start(started, hostOn) {
    let {ip, port} = hostOn ? hostOn : global.nodeConfig;
  
  const server = http.createServer((req, res) => {
    handleConnection(req, server)
        .then((ev) => {
          res.writeHead(400, {'Content-Type': 'application/json'});
          res.end(util.serialize(ev));
        })
        .catch((e) => {
          console.trace(e);
          res.writeHead(500, {'Content-Type': 'application/json'});
          res.end(util.serialize([e, null]));
        });
  });

  server.listen(port, ip, () => {
    started(server, global.nodeConfig, () => {});
  });
}

module.exports = global.distribution = {
  util: require('./distribution/util/util'),
  local: require('./distribution/local'),
  node: {start},
};

// no await
local.async.groups.registerKnownNode(global.nodeConfig);
putInDistribution({gid: 'all'});

for (const {gid, node} of known) {
    // dirty no await
    local.async.groups.registerKnownNode(node);
    // dirty no await
    local.async.groups.add(gid, node);
    console.log('groups add', gid, node);
}

if (require.main === module) {
  start(global.nodeConfig.onStart, hostOn);
}

