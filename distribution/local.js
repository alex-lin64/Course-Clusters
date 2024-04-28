const http = require("node:http");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const process = require("node:process");
const {
  unlink,
  readdir,
  mkdir,
  writeFile,
  readFile,
} = require("node:fs/promises");
const childProcess = require("node:child_process");
const fs = require("fs");

const { putInDistribution } = require("./all");
const util = require("./util/util");

function Status() {
  this.counts = 0;
  this.server = null;
  this.incrementCount = async () => {
    this.counts += 1;
  };
  this.get = async (installation) => {
    const getter = {
      nid: () => util.id.getNID(global.nodeConfig),
      sid: () => util.id.getSID(global.nodeConfig),
      ip: () => global.nodeConfig.ip,
      port: () => global.nodeConfig.port,
      counts: () => this.counts,
      heapTotal: () => process.memoryUsage().heapTotal,
      heapUsed: () => process.memoryUsage().heapUsed,
    }[installation];
    if (getter === undefined) {
      throw new Error(`could not identify ${installation}`);
    }
    return getter.call(this);
  };
  this.stop = async (closeToken, node) => {
    throw new Error("handleClose");
  };
  this.spawn = async (config) => {
    config = config || {};
    let resolve;
    const promise = new Promise((res) => {
      resolve = res;
    });
    const localOnStart = config.onStart || (() => {});
    config.onStart = util.wire.asyncRPC((server, node) => {
      localOnStart();
      resolve(node);
    });
    config = util.serialize(config);
    const correctPath = path.join(__dirname, "../distribution.js");
    childProcess
      .spawn("node", [correctPath, "--config", config], {
        stdio: "inherit",
      })
      .on("error", (e) => console.error("spawn error", e));
    const node = await promise;
    await util.sendToAll({
      message: [node],
      service: "groups",
      method: "registerKnownNode",
      gid: "all",
      exclude: null,
      subset: null,
    });
    return node;
  };
}

function Groups() {
  this.gidToGroup = new Map();
  this.all = {};
  this.registerKnownNode = async (node) => {
    this.all[util.id.getSID(node)] = node;
  };
  this.get = async (gid) => {
    if (gid === "local") {
      return { [util.id.getSID(global.nodeConfig)]: global.nodeConfig };
    }
    if (gid === "all") {
      return this.all;
    }
    let group = this.gidToGroup.get(gid);
    if (group === undefined) {
      throw new Error(`get: could not find: ${gid}`);
    }
    return group;
  };
  this.put = async (gidConfig, group) => {
    const gid = gidConfig.gid || gidConfig;
    this.gidToGroup.set(gid, group);
    putInDistribution(gidConfig);
    return group;
  };
  this.add = async (gidConfig, node) => {
    const gid = gidConfig.gid || gidConfig;
    if (!this.gidToGroup.has(gid)) {
      this.gidToGroup.set(gid, {});
      putInDistribution(gidConfig);
    }
    const group = this.gidToGroup.get(gid);
    group[util.id.getSID(node)] = node;
    return group;
  };
  this.rem = async (gid, sid) => {
    const removeFrom = this.gidToGroup.get(gid);
    if (removeFrom === undefined) {
      throw new Error(`could not find: ${gid}`);
    }
    delete removeFrom[sid];
    return removeFrom;
  };
  this.del = async (gid) => {
    const group = this.gidToGroup.get(gid);
    if (group === undefined) {
      throw new Error(`group ${gid} does not exist`);
    }
    this.gidToGroup.delete(gid);
    return group;
  };
}

function Gossip() {
  this.received = new Set();
  this.recv = async (mid, { service, method }, message, gid) => {
    if (this.received.has(mid)) {
      return;
    }
    this.received.add(mid);
    // no await
    distribution[gid].async.gossip.sendMID(mid, message, { service, method });
    service = await distribution.local.async.routes.get(service);
    const [e, v] = await new Promise((resolve) =>
      service[method].call(service, ...message, resolve),
    );
    if (isError(e)) {
      throw e;
    }
    return v;
  };
}

function Routes() {
  this.customRoutes = new Map();
  this.put = async (service, name) => {
    this.customRoutes.set(name, service);
    return service;
  };
  this.get = async (name) => {
    if (distribution.local[name] !== undefined) {
      return distribution.local[name];
    }
    if (this.customRoutes.has(name)) {
      return this.customRoutes.get(name);
    }
    throw new Error(`could not identify route ${name}`);
  };
}

function isError(e) {
  if (!e) {
    return false;
  }
  const isEmptyObject =
    typeof e === "object" &&
    Object.keys(e).length === 0 &&
    !(e instanceof Error);
  if (isEmptyObject) {
    return false;
  }
  return true;
}

function HandleClose() {
  this.installed = new Map();
  this.promise = () => {
    const closeToken = randomUUID();
    const donePromise = new Promise((res) =>
      this.installed.set(closeToken, res),
    );
    return { message: [closeToken, global.nodeConfig], donePromise };
  };
  this.handleClose = async (closeToken) => {
    this.installed.get(closeToken)();
  };
}

// A message communication interface
function Comm() {
  this.send = async (message, { node, service, method }) => {
    const options = {
      host: node.ip,
      port: node.port,
      path: `/${service}/${method}`,
      method: "POST",
      headers: { "Content-type": "application/json", Connection: "close" },
    };
    let donePromise = Promise.resolve();
    if (service === "status" && method === "stop") {
      // we have to wait till the server tells us it stopped
      ({ message, donePromise } =
        distribution.local.async.handleClose.promise());
    }
    message = util.serialize(message);
    let body = [];
    await new Promise((resolve, reject) => {
      const req = http.request(options, (res) => {
        res.on("data", (chunk) => body.push(chunk));
        res.on("end", resolve);
      });
      req.on("error", (e) => reject(new Error("request send", { cause: e })));
      req.write(message);
      req.end();
    });
    body = Buffer.concat(body).toString();
    const [e, v] = util.deserialize(body, (expr) => eval(expr));
    if (isError(e)) {
      throw e;
    }
    await donePromise;
    return v;
  };
}

function RPC() {
  this.installed = [];
  this.call = async (args, installation) => {
    return await this.installed[installation](...args);
  };
  this.install = (func) => {
    const installation = this.installed.length;
    this.installed.push(func);
    return installation;
  };
}

function MapReduceMapper() {
  this.map = async (map, job, gid, hash, key1, memOrStore) => {
    const value1 = await distribution.local.async[memOrStore].get({
      gid,
      key: key1,
    });
    let results = await Promise.resolve(map(key1, value1));
    results = Array.isArray(results) ? results : [results];
    results = results.map((result) => Object.entries(result).flat());
    await Promise.all(
      results.map(([key2, value2]) =>
        util.callOnHolder({
          key: key2,
          value: null,
          gid,
          hash,
          message: [job, key2, value2, memOrStore],
          service: "mapReduceReducer",
          method: "shuffle",
        }),
      ),
    );
  };
}

function MapReduceReducer() {
  this.jobToKey2ToValue2s = new Map();
  this.shuffle = async (job, key2, value2, memOrStore) => {
    if (!this.jobToKey2ToValue2s.has(job)) {
      this.jobToKey2ToValue2s.set(job, new Map());
    }
    const key2ToValue2s = this.jobToKey2ToValue2s.get(job);
    if (!key2ToValue2s.has(key2)) {
      key2ToValue2s.set(key2, []);
    }
    key2ToValue2s.get(key2).push(value2);
  };
  this.reduce = async (job, reduce) => {
    const key2ToValue2s = this.jobToKey2ToValue2s.get(job) || [];
    return await Promise.all(
      [...key2ToValue2s].map(([key2, value2s]) =>
        Promise.resolve(reduce(key2, value2s)),
      ),
    );
  };
}

function getGidKey(gidKey) {
  const gid = !gidKey || gidKey.gid === undefined ? "all" : gidKey.gid;
  const key = !gidKey || gidKey.key === undefined ? gidKey : gidKey.key;
  return { gid, key };
}

function Mem() {
  this.store = new Map();
  this.get = async (gidKey) => {
    const { gid, key } = getGidKey(gidKey);
    if (!this.store.has(gid)) {
      throw new Error(`could not find gid ${gid}`);
    }
    const gidStore = this.store.get(gid);
    if (key === null) {
      return [...gidStore.keys()];
    }
    if (!gidStore.has(key)) {
      throw new Error(`could not find key ${key}`);
    }
    return gidStore.get(key);
  };
  this.put = async (value, gidKey) => {
    let { gid, key } = getGidKey(gidKey);
    key = util.getActualKey(key, value);
    if (!this.store.has(gid)) {
      this.store.set(gid, new Map());
    }
    this.store.get(gid).set(key, value);
    return value;
  };
  this.del = async (gidKey) => {
    const { gid, key } = getGidKey(gidKey);
    if (!this.store.has(gid)) {
      throw new Error(`could not find gid ${gid}`);
    }
    const gidStore = this.store.get(gid);
    if (!gidStore.has(key)) {
      throw new Error(`could not find key ${key}`);
    }
    const ret = gidStore.get(key);
    gidStore.delete(key);
    return ret;
  };
}

function Store() {
  const getLocationHead = (gid) =>
    path.join(
      __dirname,
      "../store/store",
      util.id.getNID(global.nodeConfig),
      gid,
    );
  const getAll = async (gid) => {
    let paths;
    try {
      paths = await readdir(getLocationHead(gid));
    } catch (e) {
      paths = [];
    }
    return paths.map((key) => decodeURIComponent(key));
  };
  const getLocation = async (key, gid, create) => {
    const head = getLocationHead(gid);
    if (create) {
      await mkdir(head, { recursive: true });
    }
    key = encodeURIComponent(key);
    return path.join(head, key);
  };
  this.get = async (gidKey) => {
    let { gid, key } = getGidKey(gidKey);
    if (key === null) {
      return await getAll(gid);
    }
    let value;
    try {
      value = await readFile(await getLocation(key, gid, false));
    } catch (e) {
      throw new Error(`could not find ${e}`, { cause: e });
    }
    return util.deserialize(value, (expr) => eval(expr));
  };
  this.put = async (value, gidKey) => {
    let { gid, key } = getGidKey(gidKey);
    key = util.getActualKey(key, value);
    await writeFile(await getLocation(key, gid, true), util.serialize(value));
    return value;
  };
  this.del = async (gidKey) => {
    const ret = await this.get(gidKey);
    let { gid, key } = getGidKey(gidKey);
    await unlink(await getLocation(key, gid, false));
    return ret;
  };
}

function AuthoritativeCourses() {
  let map;
  const setup = async () => {
    if (map !== undefined) {
      return;
    }
    let ret = path.join(__dirname, "../data/courses.json");
    ret = await readFile(ret);
    ret = JSON.parse(ret);
    map = new Map(Object.entries(ret));
  };
  this.list = async () => {
    await setup();
    return [...map.keys()];
  };
  this.details = async (codes) => {
      await setup();
      return codes.map((code) => [code, map.get(code)]);
  };
}

function AuthoritativeStudents() {
  let map;
  const setup = async () => {
    if (map !== undefined) {
      return;
    }
    let ret = path.join(__dirname, "../data/students.json");
    ret = await readFile(ret);
    ret = JSON.parse(ret);
    map = new Map(Object.entries(ret));
  };
  this.list = async () => {
    await setup();
    return [...map.keys()];
  };
  this.details = async (tokens) => {
    await setup();
    return tokens.map((token) => [token, map.get(token)]);
  };
}

async function esvs(promise) {
  const [es, vs] = await promise;
  if (Object.keys(es).length > 0) {
    throw new Error("some nodes responded with an error", { cause: [es, vs] });
  }
  return vs;
}

function Client() {
  /*
  Sends request to all courses nodes to search for the request.  Only 1 search
  (query, course, or department) can be done at a time

  params:
    - query: string, full text search
    - course: string, course subject + code in the format "CSCI 1380"
    - department: string, ex "CSCI"

  return:
    arr, list of [courseCode, details] "tuples"
  */
  this.search = async (query, course, department) => {
    let queryRes = await util.sendToAll({
      message: [query, course, department],
      service: "courses",
      method: "search",
      gid: "courses",
      exclude: null,
      subset: null,
    });

    queryRes = Object.values(queryRes[1]).flatMap((arr) => arr);
    if (query == null || query == "") {
      // sort by course code names
      queryRes = queryRes.sort((a, b) => a[0].localeCompare(b[0]));
    } else {
      // sort by ranking
      queryRes = queryRes.sort((a, b) => b[1].rank - a[1].rank);
    }
    return queryRes;
  };
  this.studentsTaking = async (token) => {
    if (typeof token !== "string" && !(token instanceof String)) {
      throw new Error(`expected string, found ${token}`);
    }
    return await util.callOnHolder({
      key: token,
      value: null,
      gid: "students",
      hash: util.id.consistentHash,
      message: [token],
      service: "students",
      method: "listRegister",
    });
  };
  this.coursesTaking = async (code) => {
    if (typeof code !== "string" && !(code instanceof String)) {
      throw new Error(`expected string, found ${code}`);
    }
    return await util.callOnHolder({
      key: code,
      value: null,
      gid: "courses",
      hash: util.id.consistentHash,
      message: [code],
      service: "courses",
      method: "listRegister",
    });
  };
  this.register = async (code, token) => {
    if (typeof code !== "string" && !(code instanceof String)) {
      throw new Error(`expected string, found ${code}`);
    }
    if (typeof token !== "string" && !(token instanceof String)) {
      throw new Error(`expected string, found ${token}`);
    }
    const record = await util.callOnHolder({
      key: token,
      value: null,
      gid: "students",
      hash: util.id.consistentHash,
      message: [token],
      service: "students",
      method: "getRecord",
    });
    let studentsLock = util.callOnHolder({
      key: token,
      value: null,
      gid: "students",
      hash: util.id.consistentHash,
      message: [code, token],
      service: "students",
      method: "lock",
    });
    let coursesLock = util.callOnHolder({
      key: code,
      value: null,
      gid: "courses",
      hash: util.id.consistentHash,
      message: [code, record, token],
      service: "courses",
      method: "lock",
    });
    [studentsLock, coursesLock] = await Promise.allSettled([
      studentsLock,
      coursesLock,
    ]);
    const success =
      studentsLock.status === "fulfilled" && coursesLock.status === "fulfilled";
    const studentsSubmit = util.callOnHolder({
      key: token,
      value: null,
      gid: "students",
      hash: util.id.consistentHash,
      message: [code, studentsLock.value, token],
      service: "students",
      method: success ? "submit" : "unlock",
    });
    const coursesSubmit = util.callOnHolder({
      key: code,
      value: null,
      gid: "courses",
      hash: util.id.consistentHash,
      message: [code, coursesLock.value, token],
      service: "courses",
      method: success ? "submit" : "unlock",
    });
    await Promise.all([studentsSubmit, coursesSubmit]);
      if (studentsLock.reason) {
          throw new Error('student node rejected registration', {cause: studentsLock.reason});
      }
      if (coursesLock.reason) {
          throw new Error('course node rejected registration', {cause: coursesLock.reason});
      }
  };
}

function Students() {
  let map;
  let locks;
  let registered;
  let indexed = false;
  this.beginIndex = async () => {
    if (indexed) {
      return map.size;
    }
    const auth = "authoritativeStudents";
    const remote = { service: auth, method: "list" };
    let res = await esvs(distribution[auth].async.comm.send([], remote));
    res = Object.values(res)[0];
    res = await util.whichHashTo(res, "students", util.id.consistentHash);
    const tokens = res.get(util.id.getNID(global.nodeConfig));

    const details = { service: auth, method: "details" };
    res = await esvs(distribution[auth].async.comm.send([tokens], details));
    map = new Map(Object.values(res)[0]);

    locks = new Map(
      tokens.map((token) => [token, { locks: new Set(), codes: new Set() }]),
    );
    registered = new Map(tokens.map((token) => [token, new Set()]));
    indexed = true;
      console.log(`partition has ${map.size}`);
    return map.size;
  };
  this.getRecord = async (token) => {
    await this.beginIndex();
    if (!map.has(token)) {
      throw new Error(`unknown student: "${token}"`);
    }
    return map.get(token);
  };
  this.listTokens = async () => {
    await this.beginIndex();
    return [...map.keys()];
  };
  this.listRegister = async (token) => {
    await this.beginIndex();
    if (!registered.has(token)) {
      throw new Error(`unknown student: "${token}"`);
    }
    return [...registered.get(token)];
  };
  this.lock = async (code, token) => {
    await this.beginIndex();
    if (!registered.has(token)) {
      throw new Error(`unknown student: "${token}"`);
    }
    const alreadyRegistered =
      locks.get(token).codes.size + registered.get(token).size;
    if (alreadyRegistered >= 5) {
      throw new Error(
        `student has already locked ${alreadyRegistered} courses`,
      );
    }
    if (registered.get(token).has(code)) {
      throw new Error("student is already registered for this course");
    }
    if (locks.get(token).codes.has(code)) {
      throw new Error("student is already locked for this course");
    }
    const lock = `stud_lock_${randomUUID()}`;
    locks.get(token).locks.add(lock);
    locks.get(token).codes.add(code);
    return lock;
  };
  this.unlock = async (code, lock, token) => {
    await this.beginIndex();
    if (!locks.has(token)) {
      return;
    }
    locks.get(token).locks.delete(lock);
    locks.get(token).codes.delete(code);
  };
  this.submit = async (code, lock, token) => {
    await this.beginIndex();
    if (!locks.has(token)) {
      return;
    }
    locks.get(token).locks.delete(lock);
    locks.get(token).codes.delete(code);

    registered.get(token).add(code);
  };
}

function prerequisiteQualifications(taken, prerequisites) {
  if (prerequisites === null) {
    return true;
  }
  const [tag, value] = Object.entries(prerequisites).flat();
  if (tag === "any") {
    return value.some((qual) => prerequisiteQualifications(taken, qual));
  }
  if (tag === "all") {
    return value.every((qual) => prerequisiteQualifications(taken, qual));
  }
  if (tag === "exam") {
    return false;
  }
  if (tag === "course") {
    const { subject, number } = value;
    return taken.includes(`${subject} ${number}`);
  }
  throw new Error(`unknown prerequisite "${tag}"`);
}

// Handles course registration states (list of students, capacity), course search
function Courses() {
  let coursesMap; // map of course index
  let registered; // map of students registered for each course
  let locks; // map of lock for course registration
  let initialized = false;
  let tfidf; // tfidf map, courseCode -> map(term -> tf-idf)
  let idf; // idf: map, term -> idf
  const regex = /[!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~]/g;

  // initializes courses index for current node, have to be called first!
  this.beginIndex = async () => {
    // don't index course node is already initialized
    if (initialized) {
      return coursesMap.size;
    }
    const auth = "authoritativeCourses";
    const remote = { service: auth, method: "list" };
    let res = await esvs(distribution[auth].async.comm.send([], remote));

    res = Object.values(res)[0];
    res = await util.whichHashTo(res, "courses", util.id.consistentHash);
    const foo = JSON.stringify(Object.fromEntries(res));
    const ours = res.get(util.id.getNID(global.nodeConfig));
    const details = { service: auth, method: "details" };
      res = await esvs(distribution[auth].async.comm.send([ours], details));
    coursesMap = new Map(Object.values(res)[0]);
    locks = new Map(
      ours.map((code) => [code, { locks: new Set(), tokens: new Set() }]),
    );
    registered = new Map(ours.map((code) => [code, new Set()]));

    // indices for search
    [tfidf, idf] = util.calculateTfidf(coursesMap);

    // set state of course node to initialized
    initialized = true;
      console.log(`partition has ${coursesMap.size}`);
    return coursesMap.size;
  };

  /* 
  Searches for the course using the node's internal indexes.  if all parameters
  are null, return all courses.  Else, only 1 parameter can be non-null

  params:
    - query: string, full text search
    - course: string, course subject + code in the format "CSCI 1380"
    - department: string, ex "CSCI"

  returns:
    - arr, list of courseCodes mapped to their rank and course details 
        [[course code, {...description, rank: rankVal} ** this is a map], ...]
  */
  this.search = async (query, course, department) => {
    // make sure index is ready
    await this.beginIndex();

    // if all null or empty, return all courses
    if (
      (query === null || query === "") &&
      course === null &&
      department === null
    ) {
      return coursesMap;
    }
    // if searching for specific course
    if (course) {
      if (!coursesMap.has(course)) {
        return [];
      }
      return [[course, coursesMap.get(course)]];
    }
    // if searching for department
    if (department) {
      let res = [];
      coursesMap.forEach((details, courseCode) => {
        let courseDep = courseCode.split(" ")[0].toLowerCase();
        if (courseDep.includes(department.toLowerCase())) {
          res.push([courseCode, details]);
        }
      });
      return res;
    }
    // preprocess query - lower case, remove punctuation, split
    query = query.toLowerCase().replace(regex, "").split(" ");
    // stem and remove stop words
    let processedQuery = util.stemAndRemoveStopWords(query);

    // calculate query tf
    let tf = util.calculateTf(processedQuery, null, null);
    // calculate query tfidf
    let [queryVec, docVecs] = util.calculateQueryTfidf(tf, idf, tfidf);

    // calcualte query-document similarity
    let results = [];
    docVecs.forEach((docVec, doc) => {
      let rank = util.cosinesim(docVec, queryVec);

      // cutoff for docs not returned
      if (rank < 0.6) {
        return;
      }

      let details = { ...coursesMap.get(doc) };
      details["rank"] = rank;
      results.push([doc, details]);
    });

    return results;
  };

  // lists all students that are registered for this course
  this.listRegister = async (code) => {
    await this.beginIndex();
    if (!registered.has(code)) {
      throw new Error(`cannot find course: ${code}`);
    }
    return [...registered.get(code)];
  };

  // Attempts to lock this student registration. May fail if the student
  // does not qualify for the course.
  this.lock = async (code, record, token) => {
    await this.beginIndex();
      if (!coursesMap.has(code)) {
      throw new Error(`unknown course: "${code}"`);
      }
    const courseRecord = coursesMap.get(code);
    if (!prerequisiteQualifications(record.taken, courseRecord.prerequisites)) {
      throw new Error("you are not qualified to take this course");
    }
    if (!courseRecord.semester_range.includes(record.semester)) {
      throw new Error("you are not in the right semester to take this course");
    }
    if (registered.get(code).has(token)) {
      throw new Error("student is already registered for this course");
    }
    if (locks.get(code).tokens.has(token)) {
      throw new Error("student is already locked for this course");
    }
    const lock = `course_lock_${randomUUID()}`;
    locks.get(code).locks.add(lock);
    locks.get(code).tokens.add(token);
    return lock;
  };

  // removes the registration lock, because one of the checks failed.
  this.unlock = async (code, lock, token) => {
    await this.beginIndex();
    if (!locks.has(code)) {
      return;
    }
    locks.get(code).locks.delete(lock);
    locks.get(code).tokens.delete(token);
  };

  // submits the registration; never fails if you submit the right token.
  this.submit = async (code, lock, token) => {
    await this.beginIndex();
    if (!locks.has(code)) {
      return;
    }
    locks.get(code).locks.delete(lock);
    locks.get(code).tokens.delete(token);

    registered.get(code).add(token);
    console.trace(`submitting lock ${lock} ${code} ${token}`);
  };
}

const routes = {
  status: new Status(),
  groups: new Groups(),
  gossip: new Gossip(),
  routes: new Routes(),
  comm: new Comm(),
  rpc: new RPC(),
  mem: new Mem(),
  store: new Store(),
  mapReduceMapper: new MapReduceMapper(),
  mapReduceReducer: new MapReduceReducer(),
  handleClose: new HandleClose(),

  authoritativeCourses: new AuthoritativeCourses(),
  authoritativeStudents: new AuthoritativeStudents(),
  client: new Client(),
  students: new Students(),
  courses: new Courses(),
};

function mapValues(x, func) {
  return Object.fromEntries(Object.entries(x).map(([k, v]) => [k, func(v)]));
}

function serviceToCallbackService(service) {
  return mapValues(service, (method) => (...args) => {
    if (args.length === method.length + 1) {
      const callback = args.pop();
      Promise.resolve(method.call(service, ...args))
        .then((v) => callback(null, v))
        .catch((e) => callback(e, null));
    } else if (args.length === method.length) {
      console.trace(`did not provide a callback for ${method.toString()}`);
      // they did not the callback, ignore the promise
      method.call(service, ...args);
    } else {
      throw new Error(`wrong number of arguments for ${method.toString()}: 
          found ${args.length}, expected ${method.length} ${args}`);
    }
  });
}

module.exports = {
  ...mapValues(routes, serviceToCallbackService),
  async: routes,
};
