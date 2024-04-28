global.nodeConfig = { ip: "127.0.0.1", port: 7070 };
const { createGroup } = require("../distribution/all");
global.distribution = require("../distribution");

async function runWorkflows(gid, job) {
  const nodes = [
    { ip: "127.0.0.1", port: 7110 },
    { ip: "127.0.0.1", port: 7111 },
    { ip: "127.0.0.1", port: 7112 },
    { ip: "127.0.0.1", port: 7113 },
  ];

  const server = await new Promise((cb) => distribution.node.start(cb));
  for (const node of nodes) {
    await distribution.local.async.status.spawn(node);
  }

  await createGroup({ gid }, nodes);

  const result = await job();

  for (const node of nodes) {
    await distribution.local.async.comm.send([], {
      service: "status",
      method: "stop",
      node,
    });
  }
  await new Promise((res) => server.close(res));
  return result;
}

async function numberJob(gid) {
  const keys = new Array(100).fill(null).map((_, i) => `hello ${i}`);
  for (let i = 0; i < keys.length; i++) {
    await distribution[gid].async.store.put(i, keys[i]);
  }
  const mapper = async (key, value) => ({ [`${value % 10}`]: value });
  const reducer = async (key, values) => ({ [key]: values });

  return await distribution[gid].async.mr.exec({
    keys,
    map: mapper,
    reduce: reducer,
  });
}

test("(0 pts) testing the distribution of keys", async () => {
  const gid = "number";
  const [, reverseMapping] = await runWorkflows(gid, () => numberJob(gid));
  expect(reverseMapping).toHaveLength(10);
  for (const entry of reverseMapping) {
    const [key, values] = Object.entries(entry).flat();
    expect(values.length).toEqual(10);
    for (const value of values) {
      expect(value % 10).toEqual(+key);
    }
  }
});

async function numberJobSum(gid) {
  const keys = new Array(100).fill(null).map((_, i) => `hello ${i}`);
  for (let i = 0; i < keys.length; i++) {
    await distribution[gid].async.store.put(i, keys[i]);
  }
  const mapper = async (key, value) => ({ [`${value % 10}`]: value });
  const reducer = async (key, values) => ({
    [key]: values.reduce((a, b) => a + b, 0),
  });

  return await distribution[gid].async.mr.exec({
    keys,
    map: mapper,
    reduce: reducer,
  });
}

test("(0 pts) testing the reducer works with sum", async () => {
  const gid = "numberSum";
  const [, reverseMapping] = await runWorkflows(gid, () => numberJobSum(gid));
  expect(reverseMapping).toHaveLength(10);
  for (const entry of reverseMapping) {
    const [, value] = Object.entries(entry).flat();
    expect(value).toBeGreaterThanOrEqual(10);
  }
});

// gets the page contents for all of our urls
async function workflow1(urls, gid) {
  const keys = urls.map((_, i) => `${i}`);
  for (let i = 0; i < urls.length; i++) {
    await distribution[gid].async.store.put(urls[i], keys[i]);
  }
  const mapper = eval(`async (dummy, url) => {
      let body = await distribution.util.getPageContents(url);
      const gid = ${JSON.stringify(gid)};
      await distribution[gid].async.store.put(body, {key: 'content '+url, gid});
      return [];
  }`);
  const reducer = (key, values) => ({ [key]: values }); // never called
  await distribution[gid].async.mr.exec({ keys, map: mapper, reduce: reducer });
}

// gets all of the urls in the pages
async function workflow2(urls, gid) {
  const keys = urls.map((url) => "content " + url);
  const mapper = async (contentUrl, body) => {
    const bareUrl = contentUrl.split(" ")[1];
    const urls = distribution.util.getUrls(bareUrl, body);
    return urls.map((url) => ({ [bareUrl]: url }));
  };
  const reducer = eval(`async (bareUrl, urlsInSource) => {
      const gid = ${JSON.stringify(gid)};
      await distribution[gid].async.store.put(
          urlsInSource, {key: 'urls '+bareUrl, gid});
      return {[bareUrl]: null};
  }`);
  await distribution[gid].async.mr.exec({ keys, map: mapper, reduce: reducer });
}

// computes the reverse mapping
async function workflow3(urls, gid) {
  const keys = urls.map((url) => "urls " + url);
  const mapper = async (urlsUrl, urls) => {
    const bareUrl = urlsUrl.split(" ")[1];
    return urls.map((url) => ({ [url]: bareUrl }));
  };
  const reducer = async (url, bareUrls) => ({ [url]: [...new Set(bareUrls)] });
  return await distribution[gid].async.mr.exec({
    keys,
    map: mapper,
    reduce: reducer,
  });
}

test("(0 pts) testing the three workflows", async () => {
  const urls = [
    "https://en.wikipedia.org/wiki/Hualien_City",
    "https://en.wikipedia.org/wiki/Pacific_Ocean",
    //    'https://en.wikipedia.org/wiki/Antarctica',
    //    'https://en.wikipedia.org/wiki/South_Pole',
    //    'https://en.wikipedia.org/wiki/South_magnetic_pole',
    //    'https://en.wikipedia.org/wiki/Geomagnetic_pole',
    //    'https://en.wikipedia.org/wiki/Solar_wind',
  ];

  const gid = "crawl";

  const job = async () => {
    await workflow1(urls, gid);
    await workflow2(urls, gid);
    return await workflow3(urls, gid);
  };

  const [, reverseMapping] = await runWorkflows(gid, job);
  expect(reverseMapping.length).toBeGreaterThanOrEqual(1500);
  for (const entry of reverseMapping) {
    const [, bys] = Object.entries(entry).flat();
    expect(bys.length).toBeGreaterThan(0);
    expect(urls).toEqual(expect.arrayContaining(bys));
  }
}, 15000);
