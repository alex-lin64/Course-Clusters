global.nodeConfig = { ip: "127.0.0.1", port: 7070 };
const { createGroup } = require("../distribution/all");
const distribution = require("../distribution");
const local = distribution.local.async;
const fs = require("fs");

async function setup(gidCounts, job) {
  jest.resetModules();
  gidCounts = {
    ...gidCounts,
    authoritativeStudents: 1,
    authoritativeCourses: 1,
  };
  const ip = "127.0.0.1";
  const nodes = [];
  const gidNodes = new Map();
  let gidPort = 0;
  for (const [gid, count] of Object.entries(gidCounts)) {
    gidNodes.set(gid, []);
    for (let i = 0; i < count; i++) {
      const node = { ip, port: 7000 + 100 * gidPort + i };
      gidNodes.get(gid).push(node);
      nodes.push(node);
    }
    gidPort += 1;
  }
  const server = await new Promise((cb) => distribution.node.start(cb));
  await Promise.all(nodes.map((node) => local.status.spawn(node)));
  await Promise.all(
    [...gidNodes.entries()].map(([gid, nodes]) => createGroup({ gid }, nodes)),
  );
  const result = await job(Object.fromEntries([...gidNodes.entries()]));
  const stop = { service: "status", method: "stop" };
  await Promise.all(
    nodes.map((node) => local.comm.send([], { ...stop, node })),
  );
  await new Promise((res) => server.close(res));
  return result;
}

test(
  "test search queries",
  async () =>
    setup({ client: 2, students: 3, courses: 3 }, async (gidNodes) => {
      const [client0, client1] = gidNodes.client;

      const search = (node) => ({
        service: "client",
        method: "search",
        node,
      });

      let res = await local.comm.send([null, null, "Csci"], search(client0));

      const filePath = "output.txt";
      fs.writeFile(filePath, JSON.stringify(res), (err) => {
        if (err) {
          console.error("Error writing to file:", err);
          return;
        }
      });

      console.log(res.length);
    }),
  100000,
);
