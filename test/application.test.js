global.nodeConfig = { ip: "127.0.0.1", port: 7070 };
const { createGroup } = require("../distribution/all");
const distribution = require("../distribution");
const local = distribution.local.async;

function shuffle(array) {
  const ret = [];
  while (array.length > 0) {
    const i = Math.floor(Math.random() * array.length);
    ret.push(...array.splice(i, 1));
  }
  return ret;
}

test(
  "stress test",
  () =>
    setup({ client: 1, students: 5, courses: 6 }, async (gidNodes) => {
      const [client] = gidNodes.client;
      const register = (node) => ({
        service: "client",
        method: "register",
        node,
      });
      const listCourses = (node) => ({
        service: "client",
        method: "studentsTaking",
        node,
      });
      const listStudents = (node) => ({
        service: "client",
        method: "coursesTaking",
        node,
      });

      const promises = [];

      let students = await local.authoritativeStudents.list();
      students = shuffle(students).slice(0, 100);
      let courses = await local.authoritativeCourses.list();
      courses = shuffle(courses).slice(0, 25);

      for (let i = 0; i < 10; i++) {
        let code = Math.floor(Math.random() * courses.length);
        code = courses[code];
        let student = Math.floor(Math.random() * students.length);
        student = students[student];

        promises.push(local.comm.send([code, student], register(client)));
      }
      const result = await Promise.allSettled(promises);

      const studentToCourses = new Map();
      const courseToStudents = new Map();

      for (const student of students) {
        const taking = await local.comm.send([student], listCourses(client));
        expect(taking.length).toBeLessThanOrEqual(5);

        studentToCourses.set(student, taking);
      }

      for (const course of courses) {
        const enrolled = await local.comm.send([course], listStudents(client));
        courseToStudents.set(course, enrolled);
      }

      console.trace(studentToCourses);
      console.trace(courseToStudents);

      for (const [c, ss] of courseToStudents) {
        for (const s of ss) {
          expect(studentToCourses.get(s)).toContain(c);
        }
      }

      for (const [s, cs] of studentToCourses) {
        for (const c of cs) {
          expect(courseToStudents.get(c)).toContain(s);
        }
      }

      //            for (const student of enrolled) {
      //                const taking = await local.comm.send([student], listCourses(client));
      //                expect(taking).toContain(course);
      //            }

      //        for (const
      //            for (const t of taking) {
      //                const enrolled = await local.comm.send([t], listStudents(client));
      //                console.trace(t, enrolled, student);
      //                expect(enrolled).toContain(student);
      //            }
    }),
  60 * 1000,
);
test("authoritativeCourses list contains the courses", async () => {
  const list = await local.authoritativeCourses.list();
  expect(list).toContain("CSCI 1380");
  expect(list).toContain("PHP 1100");
});

test("authoritativeCourses has course detail", async () => {
  let detail = await local.authoritativeCourses.details(["CSCI 1380"]);
  detail = detail[0][1];
  expect(detail).toHaveProperty("title");
  expect(detail).toHaveProperty("description");
  expect(detail).toHaveProperty("code");
  expect(detail).toHaveProperty("prerequisites");
  expect(detail).toHaveProperty("semester_range");
  expect(detail).toHaveProperty("offerings");
});

test("authoritativeStudents basic", async () => {
  const list = await local.authoritativeStudents.list();
  const student = list[0];
  expect(student).toBeDefined();
  let detail = await local.authoritativeStudents.details([student]);
  detail = detail[0][1];
  expect(detail).toHaveProperty("name");
  expect(detail).toHaveProperty("semester");
  expect(detail).toHaveProperty("taken");
});

//async function beginIndex(gidNodes, service) {
//  await Promise.all(
//    gidNodes.get(service).map((node) => {
//      return local.comm.send([], { service, method: "beginIndex", node });
//    }),
//  );
//}

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

/*

test(
  "test registration dependents",
  () =>
    setup({ client: 1, students: 2, courses: 3 }, async (gidNodes) => {
      expect(gidNodes).toHaveProperty("client");
      expect(gidNodes).toHaveProperty("students");
      expect(gidNodes).toHaveProperty("courses");
      expect(gidNodes).toHaveProperty("authoritativeStudents");
      expect(gidNodes).toHaveProperty("authoritativeCourses");

      expect(gidNodes.client).toHaveLength(1);
      expect(gidNodes.students).toHaveLength(2);
      expect(gidNodes.courses).toHaveLength(3);

      expect(gidNodes.client[0]).toHaveProperty("ip");
      expect(gidNodes.client[0]).toHaveProperty("port");

      // list all of the student students on both `students` nodes
      let remote;
      remote = {
        service: "students",
        method: "listTokens",
        node: gidNodes.students[0],
      };
      const r0 = await local.comm.send([], remote);
      remote = {
        service: "students",
        method: "listTokens",
        node: gidNodes.students[1],
      };
      const r1 = await local.comm.send([], remote);
      const totalStudents = r0.length + r1.length;
      expect(totalStudents).toBeGreaterThan(4000);
      expect(new Set([...r0, ...r1]).size).toBe(totalStudents);

      // lock a student when the student is not held here
      expect(async () => {
        remote = {
          service: "students",
          method: "lock",
          node: gidNodes.students[1],
        };
        await local.comm.send(["CSCI 1380", r0[0]], remote);
      }).rejects.toThrow();

      // lock a student when they are held heer
      remote = {
        service: "students",
        method: "lock",
        node: gidNodes.students[0],
      };
      const lock1380 = await local.comm.send(["CSCI 1380", r0[0]], remote);

      // attempt to get a lock on the same student twice
      expect(async () => {
        remote = {
          service: "students",
          method: "lock",
          node: gidNodes.students[0],
        };
        await local.comm.send(["CSCI 1380", r0[0]], remote);
      }).rejects.toThrow();

      // get a lock on a new course before the old one expires
      remote = {
        service: "students",
        method: "lock",
        node: gidNodes.students[0],
      };
      const lock1270 = await local.comm.send(["CSCI 1270", r0[0]], remote);

      // drop the lock on 1380
      remote = {
        service: "students",
        method: "unlock",
        node: gidNodes.students[0],
      };
      await local.comm.send(["CSCI 1380", lock1380, r0[0]], remote);

      // submit on 1270
      remote = {
        service: "students",
        method: "submit",
        node: gidNodes.students[0],
      };
      await local.comm.send(["CSCI 1270", lock1270, r0[0]], remote);

      // see what courses the student is registered for
      let taking;
      remote = {
        service: "students",
        method: "listRegister",
        node: gidNodes.students[0],
      };
      taking = await local.comm.send([r0[0]], remote);
      expect(taking).toEqual(["CSCI 1270"]);

      // try to lock a course that the student is already registered for
      expect(async () => {
        remote = {
          service: "students",
          method: "lock",
          node: gidNodes.students[0],
        };
        await local.comm.send(["CSCI 1270", r0[0]], remote);
      }).rejects.toThrow();

      // end to end register for AFRI 0001
      remote = {
        service: "client",
        method: "register",
        node: gidNodes.client[0],
      };
      await local.comm.send(["AFRI 0001", r0[0]], remote);

      // check that these courses appear in the students listRegister
      remote = {
        service: "students",
        method: "listRegister",
        node: gidNodes.students[0],
      };
      taking = await local.comm.send([r0[0]], remote);
      expect(taking).toEqual(
        expect.arrayContaining(["CSCI 1270", "AFRI 0001"]),
      );

      // test that these appear in the client list register
      remote = {
        service: "client",
        method: "coursesTaking",
        node: gidNodes.client[0],
      };
      taking = await local.comm.send(["AFRI 0001"], remote);
      expect(taking).toEqual(expect.arrayContaining([r0[0]]));

      // check client that everything we expect is there
      remote = {
        service: "client",
        method: "studentsTaking",
        node: gidNodes.client[0],
      };
      taking = await local.comm.send([r0[0]], remote);
      expect(taking).toEqual(expect.arrayContaining(["AFRI 0001"]));
    }),
  10000,
);


test(
  "test registration main",
  () =>
    setup({ client: 2, students: 3, courses: 3 }, async (gidNodes) => {
      const [client0, client1] = gidNodes.client;
      const register = (node) => ({
        service: "client",
        method: "register",
        node,
      });
      const listCourses = (node) => ({
        service: "client",
        method: "studentsTaking",
        node,
      });
      const listStudents = (node) => ({
        service: "client",
        method: "coursesTaking",
        node,
      });
      let result;

      await local.comm.send(
        ["CSCI 0170", "student-test-taken-nothing-1"],
        register(client1),
      );

      // submit same registration twice
      expect(async () => {
        await local.comm.send(
          ["CSCI 0170", "student-test-taken-nothing-1"],
          register(client1),
        );
      }).rejects.toThrow();

      // submit same registration, on different node
      expect(async () => {
        await local.comm.send(
          ["CSCI 0170", "student-test-taken-nothing-1"],
          register(client0),
        );
      }).rejects.toThrow();

      await local.comm.send(
        ["CSCI 0150", "student-test-taken-nothing-1"],
        register(client0),
      );

      result = await local.comm.send(
        ["student-test-taken-nothing-1"],
        listCourses(client1),
      );
      expect(result.toSorted()).toEqual(["CSCI 0150", "CSCI 0170"].toSorted());

      result = await local.comm.send(
        ["student-test-taken-nothing-1"],
        listCourses(client0),
      );
      expect(result.toSorted()).toEqual(["CSCI 0150", "CSCI 0170"].toSorted());

      // see if the student is there
      result = await local.comm.send(["CSCI 0150"], listStudents(client1));
      expect(result.toSorted()).toEqual(
        ["student-test-taken-nothing-1"].toSorted(),
      );

      result = await local.comm.send(["CSCI 0170"], listStudents(client0));
      expect(result.toSorted()).toEqual(
        ["student-test-taken-nothing-1"].toSorted(),
      );

      // not in the system yet
      result = await local.comm.send(["CSCI 0200"], listStudents(client0));
      expect(result.toSorted()).toEqual([].toSorted());

      // no prerequisites
      expect(async () => {
        await local.comm.send(
          ["CSCI 0200", "student-test-taken-nothing-1"],
          register(client0),
        );
      }).rejects.toThrow();

      result = await local.comm.send(["CSCI 0200"], listStudents(client0));
      expect(result.toSorted()).toEqual([].toSorted());

      // register a student with the right prereqs
      await local.comm.send(
        ["CSCI 0200", "student-test-taken-csci-0150-1"],
        register(client0),
      );

      result = await local.comm.send(["CSCI 0200"], listStudents(client1));
      expect(result.toSorted()).toEqual(
        ["student-test-taken-csci-0150-1"].toSorted(),
      );

      result = await local.comm.send(["CSCI 0200"], listStudents(client1));
      expect(result.toSorted()).toEqual(
        ["student-test-taken-csci-0150-1"].toSorted(),
      );

      expect(async () => {
        await local.comm.send(
          ["AFRI 0001", "student-test-semester-8"],
          register(client0),
        );
      }).rejects.toThrow();

      await local.comm.send(
        ["AFRI 0001", "student-test-semester-7"],
        register(client0),
      );

      // try to register for more than 5 couress
      await local.comm.send(
        ["AFRI 0005", "student-test-taken-nothing-2"],
        register(client0),
      );
      await local.comm.send(
        ["AFRI 0090", "student-test-taken-nothing-2"],
        register(client0),
      );
      await local.comm.send(
        ["AFRI 0110C", "student-test-taken-nothing-2"],
        register(client0),
      );
      await local.comm.send(
        ["AFRI 0130", "student-test-taken-nothing-2"],
        register(client0),
      );
      await local.comm.send(
        ["AFRI 0160", "student-test-taken-nothing-2"],
        register(client0),
      );

      expect(async () => {
        await local.comm.send(
          ["AFRI 0205", "student-test-taken-nothing-2"],
          register(client0),
        );
      }).rejects.toThrow();
    }),
  10000,
);

*/
