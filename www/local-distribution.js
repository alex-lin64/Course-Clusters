#!/usr/bin/env node

global.nodeConfig = { ip: "0.0.0.0", port: 8080 };
const { createGroup } = require("../distribution/all");
const distribution = require("../distribution");
const local = distribution.local.async;

async function setup(gidCounts) {
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
}

setup({client: 1, students: 3, courses: 3}).then();
