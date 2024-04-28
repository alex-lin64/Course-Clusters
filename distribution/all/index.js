const { randomUUID } = require("node:crypto");
const util = require("../util/util");

function ev(func) {
  return async function (...args) {
    try {
      return [null, await func(...args)];
    } catch (e) {
      return [e, null];
    }
  };
}

function Comm() {
  this.send = async function (message, { service, method }) {
    return await util.sendToAll({
      message,
      service,
      method,
      gid: this.gid,
      exclude: null,
      subset: null,
    });
  };
}

function Gossip() {
  this.sendMID = async function (mid, message, finalRemote) {
    return await util.sendToAll({
      message: [mid, finalRemote, message, this.gid],
      service: "gossip",
      method: "recv",
      gid: this.gid,
      exclude: null,
      subset: this.subset,
    });
  };
  this.send = async function (message, finalRemote) {
    return await distribution[this.gid].async.gossip.sendMID(
      randomUUID(),
      message,
      finalRemote,
    );
  };
  this.at = async function (periodMillis, rpc) {
    return [null, setInterval(rpc, periodMillis)];
  };
  this.del = async function (intervalID) {
    return [null, clearInterval(intervalID)];
  };
}

function augmentGIDConfig(gidConfig, gid) {
  return gid.gid === undefined
    ? { ...gidConfig, gid }
    : { ...gidConfig, ...gid };
}

function Groups() {
  this.get = async function (gid) {
    return await util.sendToAll({
      message: [gid],
      service: "groups",
      method: "get",
      gid: "all",
      exclude: null,
      subset: null,
    });
  };
  this.put = async function (gid, group) {
    return await util.sendToAll({
      message: [augmentGIDConfig(this, gid), group],
      service: "groups",
      method: "put",
      gid: "all",
      exclude: null,
      subset: null,
    });
  };
  this.add = async function (gid, node) {
    return await util.sendToAll({
      message: [augmentGIDConfig(this, gid), node],
      service: "groups",
      method: "add",
      gid: "all",
      exclude: null,
      subset: null,
    });
  };
  this.rem = async function (gid, sid) {
    return await util.sendToAll({
      message: [gid, sid],
      service: "groups",
      method: "rem",
      gid: "all",
      exclude: null,
      subset: null,
    });
  };
  this.del = async function (gid) {
    return await util.sendToAll({
      message: [gid],
      service: "groups",
      method: "del",
      gid: "all",
      exclude: null,
      subset: null,
    });
  };
}

function augmentGIDKey(gidConfig, gidKey) {
  return {
    key: !gidKey || gidKey.key === undefined ? gidKey : gidKey.key,
    gid: !gidKey || gidKey.gid === undefined ? gidConfig.gid : gidKey.gid,
  };
}

function MemStore(service) {
  this.get = async function (key) {
    // if key is null, then run it for all node in the group
    // pass in the context from gidConfig
    if (key === null) {
      const [es, vs] = await util.sendToAll({
        message: [augmentGIDKey(this, null)],
        service,
        method: "get",
        gid: this.gid,
        exclude: null,
        subset: null,
      });
      return [es, Object.values(vs).flat()];
    } else {
      return await ev(util.callOnHolder)({
        key: key === null || key.key === undefined ? key : key.key,
        value: null,
        gid: this.gid,
        hash: this.hash,
        message: [augmentGIDKey(this, key)],
        service,
        method: "get",
      });
    }
  };
  this.put = async function (value, key) {
    return await ev(util.callOnHolder)({
      key: key === null || key.key === undefined ? key : key.key,
      value,
      gid: this.gid,
      hash: this.hash,
      message: [value, augmentGIDKey(this, key)],
      service,
      method: "put",
    });
  };
  this.del = async function (key) {
    return await ev(util.callOnHolder)({
      key: key === null || key.key === undefined ? key : key.key,
      value: null,
      gid: this.gid,
      hash: this.hash,
      message: [augmentGIDKey(this, key)],
      service,
      method: "del",
    });
  };
}

function MapReduce() {
  this.exec = async function ({ keys: key1s, map, reduce, memory }) {
    // XXX
    const job = `${util.id.getNID(global.nodeConfig)}-${randomUUID()}`;
    const memOrStore =
      memory === null ? this.memOrStore : memory ? "mem" : "store";
    const { gid, hash } = this;
    await Promise.all(
      key1s.map((key1) =>
        util.callOnHolder({
          key: key1,
          value: null,
          gid,
          hash,
          message: [map, job, gid, hash, key1, memOrStore],
          service: "mapReduceMapper",
          method: "map",
        }),
      ),
    );
    const [es, vs] = await util.sendToAll({
      message: [job, reduce],
      service: "mapReduceReducer",
      method: "reduce",
      gid,
      exclude: null,
      subset: null,
    });
    return [es, Object.values(vs).flat()];
  };
}

function Routes() {
  this.put = async function (service, name) {
    return await util.sendToAll({
      message: [service, name],
      service: "routes",
      method: "put",
      gid: this.gid,
      exclude: null,
      subset: null,
    });
  };
}

function Status() {
  this.get = async function (installation) {
    let [es, vs] = await util.sendToAll({
      message: [installation],
      service: "status",
      method: "get",
      gid: this.gid,
      exclude: null,
      subset: null,
    });
    const shouldAggregate = ["counts", "heapTotal", "heapUsed"].includes(
      installation,
    );
    if (shouldAggregate) {
      vs = Object.values(vs).reduce((acc, elem) => acc + elem, 0);
    }
    return [es, vs];
  };
  this.stop = async function () {
    return await util.sendToAll({
      message: [],
      service: "status",
      method: "stop",
      gid: this.gid,
      exclude: util.id.getSID(global.nodeConfig),
      subset: null,
    });
  };
  this.spawn = async function (config) {
    const node = await distribution.local.async.status.spawn(config);
    const [es] = await distribution[this.gid].async.groups.add(this, node);
    if (Object.keys(es).length > 0) {
      return [es, null];
    }
    return [null, node];
  };
}

const routes = {
  comm: new Comm(),
  gossip: new Gossip(),
  groups: new Groups(),
  mem: new MemStore("mem"),
  mr: new MapReduce(),
  routes: new Routes(),
  status: new Status(),
  store: new MemStore("store"),
};

function mapValues(x, func) {
  return Object.fromEntries(Object.entries(x).map(([k, v]) => [k, func(v)]));
}

function defaultGIDConfig(gidConfig) {
  if (!gidConfig.gid) {
    gidConfig = { gid: gidConfig };
  }
  return {
    //    gid: 'all',
    subset: (lst) => 3,
    hash: util.id.naiveHash,
    memOrStore: "store",
    ...(gidConfig || {}),
  };
}

function serviceToCallbackService(gidConfig, service) {
  return mapValues(service, (method) => (...args) => {
    if (args.length !== method.length + 1) {
      throw new Error(`wrong number of arguments for 
          ${method.toString()}: found ${args.length}, 
          expected ${method.length + 1}: ${args}`);
    }
    const callback = args.pop();
    method.call(gidConfig, ...args).then(([e, v]) => callback(e, v));
  });
}

function serviceToAsyncService(gidConfig, service) {
  return mapValues(service, (method) => method.bind(gidConfig));
}

function putInDistribution(gidConfig) {
  gidConfig = defaultGIDConfig(gidConfig);

  global.distribution[gidConfig.gid] = {
    ...mapValues(routes, (service) =>
      serviceToCallbackService(gidConfig, service),
    ),
    async: mapValues(routes, (service) =>
      serviceToAsyncService(gidConfig, service),
    ),
  };
}

function groupsTemplate(gidConfig) {
  gidConfig = defaultGIDConfig(gidConfig);
  return serviceToCallbackService(gidConfig, routes.groups);
}

async function createGroup(gidConfig, group) {
  gidConfig = defaultGIDConfig(gidConfig);
  group = Object.fromEntries(group.map((node) => [util.id.getSID(node), node]));
  return await routes.groups.put.call(gidConfig, gidConfig, group);
}

module.exports = { putInDistribution, groupsTemplate, createGroup };
