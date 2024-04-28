const util = require("../distribution/util/util");

// sample course data.  certain fields are aritificially removed as they are not
// necessary to create the inverted index
let courseData1 = {
  "AFRI 0001": {
    code: {
      subject: "AFRI",
      number: "0001",
    },
    title: "Octavia N.K",
    description: "analytical lenses",
    aliases: [],
    offerings: [
      {
        instructors: ["Kidane"],
      },
    ],
  },
  "ECON 0001": {
    code: {
      subject: "ECON",
      number: "0001",
    },
    title: "cool",
    description: "Pokemon",
    aliases: [],
    offerings: [
      {
        instructors: ["Joe Rogan"],
      },
    ],
  },
};
let courseData2 = {
  "AFRI 0001": {
    code: {
      subject: "AFRI",
      number: "0001",
    },
    title:
      "From Octavia Butler to N.K Jemisin, Black Feminist and Queer Theories in Science-Fiction and Fantasy",
    description:
      "Through the analytical lenses offered by Black feminist and queer theories, this course engages works of Black speculative fiction as modes of theorizing questions of race, gender, sexuality, class, environmental justice, interpersonal politics and more. Through course readings, seminar discussions, and creative writing projects, students will critically analyze works of speculative fiction written by Black authors and broaden their knowledge of Black speculative literature, with a particular focus on science fiction and fantasy.",
    aliases: [],
    offerings: [
      {
        instructors: ["Senit Kidane"],
      },
    ],
  },
  "PHIL 0412": {
    code: {
      subject: "PHIL",
      number: "0412",
    },
    title: "Climate Change Ethics",
    description:
      "Climate change is commonly said to be one of the defining issues of the 21st century. Calls for, and movement towards, significant action seem to be growing. Yet what ought to be done? This is a moral question as much as, if not more so, than an economic, political, or scientific question. In this course, then, we will consider climate change from the point of view of moral philosophy.",
    aliases: [],
    offerings: [
      {
        instructors: ["Marc Hewitt"],
      },
    ],
  },
  "HIST 1360": {
    code: {
      subject: "HIST",
      number: "1360",
    },
    title: "Amazonia from the Prehuman to the Present",
    description:
      "This course merging lecture and discussions will examine the fascinating and contested history of one of the world’s most complex fluvial ecosystems: Amazonia, in equatorial South America, from its pre-human history to the present day. The course will include readings and discussions on the region’s ecological origins; the social history of its diverse Indigenous and immigrant populations, including African-descended peoples; exploration myths and European colonial projects; and more recent efforts to exploit and protect Amazonia’s extraordinary natural and human resources. The course will use tools and resources from archaeology, anthropology, biology, and social and cultural history, and will also examine popular representations of the Amazon through novels, newspapers, podcasts, and film.",
    aliases: [
      {
        subject: "ARCH",
        number: "1242",
      },
    ],
    offerings: [
      {
        instructors: ["Neil Safier"],
      },
      {
        instructors: ["Neil F Safier"],
      },
    ],
  },
};
let courses1 = new Map(Object.entries(courseData1));
let courses2 = new Map(Object.entries(courseData2));

test("courses1: correctness of idf and tf-idf values", () => {
  // Calculate IDF values
  const res = util.calculateTfidf(courses1);
  const tfidf = res[0];
  const idf = res[1];

  // Expected tf-idf values for each term
  const expectedTfidf = new Map([
    [
      "AFRI 0001",
      new Map([
        ["octavia", 0.14285714285714285],
        ["n.k", 0.14285714285714285],
        ["analyt", 0.14285714285714285],
        ["lens", 0.14285714285714285],
        ["kidan", 0.14285714285714285],
        ["afri", 0.14285714285714285],
        ["0001", 0.08493355598454794],
      ]),
    ],
    [
      "ECON 0001",
      new Map([
        ["cool", 0.16666666666666666],
        ["pokemon", 0.16666666666666666],
        ["joe", 0.16666666666666666],
        ["rogan", 0.16666666666666666],
        ["econ", 0.16666666666666666],
        ["0001", 0.09908914864863927],
      ]),
    ],
  ]);

  // Expected idf values for each term
  const expectedIdf = new Map([
    ["octavia", 1],
    ["n.k", 1],
    ["analyt", 1],
    ["lens", 1],
    ["kidan", 1],
    ["afri", 1],
    ["0001", 0.5945348918918356],
    ["cool", 1],
    ["pokemon", 1],
    ["joe", 1],
    ["rogan", 1],
    ["econ", 1],
  ]);

  // Assert each term's IDF value in each document
  Object.keys(expectedIdf).forEach((term) => {
    Object.keys(expectedIdf[term]).forEach((course) => {
      expect(idf[term][course]).toBeCloseTo(expectedIdf[term][course], 3);
    });
  });

  // Assert each term's tf-idf value in each document
  Object.keys(expectedTfidf).forEach((course) => {
    Object.keys(expectedIdf[course]).forEach((term) => {
      expect(tfidf[course][term]).toBeCloseTo(expectedIdf[course][term], 3);
    });
  });
});

test("empty corpus", () => {
  // Empty corpus
  const emptyCorpus = new Map();

  // Calculate IDF values
  const res = util.calculateTfidf(emptyCorpus);
  const tfidf = res[0];
  const idf = res[1];

  // Expect an empty object
  expect(tfidf.size).toBe(0);
  expect(idf.size).toBe(0);
});

test("documents with no terms", () => {
  // sample corpus with empty documents
  const corpus = new Map(Object.entries({ "AFRI 0001": {} }));

  const res = util.calculateTfidf(corpus);
  const tfidf = res[0];
  const idf = res[1];

  // Expect an empty object
  expect(tfidf.size).toBe(0);
  expect(idf.size).toBe(0);
});

test("stemAndRemoveStopWords, correctness", () => {
  const words = ["pokemon", "the", "i", "lenses"];
  const wordsExpected = ["pokemon", "lens"];

  let res = util.stemAndRemoveStopWords(words);

  expect(res).toEqual(wordsExpected);
});

test("stemAndRemoveStopWords, empty", () => {
  let res = util.stemAndRemoveStopWords([]);

  expect(res.length).toBe(0);
});

test("calculateTf, correctness, idf included", () => {
  const processedTerms = ["crazy", "train", "rice"];
  const idfPartial = new Map();
  const courseCode = "sample";

  const tfExpected = new Map(
    Object.entries({
      crazy: 0.3333333333,
      train: 0.333333333,
      rice: 0.3333333333,
    }),
  );
  const idfPartialExpected = new Map(
    Object.entries({
      crazy: new Set(["sample"]),
      train: new Set(["sample"]),
      rice: new Set(["sample"]),
    }),
  );

  let res = util.calculateTf(processedTerms, idfPartial, courseCode);

  res.forEach((tf, term) => {
    expect(tf).toBeCloseTo(tfExpected.get(term), 3);
  });

  expect(idfPartial.size).toBe(3);

  idfPartial.forEach((val, term) => {
    expect(idfPartial.get(term).size).toBe(1);
    expect(idfPartial.get(term).has("sample")).toBeTruthy();
  });
});

test("calculateTf, correctness, no idf included", () => {
  const processedTerms = ["crazy", "train", "train"];

  const tfExpected = new Map(
    Object.entries({
      crazy: 0.3333333333,
      train: 0.666666666,
    }),
  );

  let res = util.calculateTf(processedTerms, null, null);

  res.forEach((tf, term) => {
    expect(tf).toBeCloseTo(tfExpected.get(term), 3);
  });
});

test("calculateTf, correctness, empty processed terms", () => {
  let res = util.calculateTf([], null, null);

  expect(res.size).toBe(0);
});

test("cosinesim, correctness", () => {
  let array1 = [1, 0, 0, 1];
  let array2 = [1, 0, 0, 0];

  let res = util.cosinesim(array1, array2);

  expect(res).toBeCloseTo(0.7071067811865475, 3);
});

test("cosinesim, correctness, 0 denom", () => {
  let array1 = [1, 0, 0, 1];
  let array2 = [0, 0, 0, 0];

  let res = util.cosinesim(array1, array2);

  expect(res).toBe(0);
});

test("cosinesim, null", () => {
  let array1 = [];
  let array2 = [];

  let res = util.cosinesim(array1, array2);

  expect(res).toBe(0);
});

test("normalizeVector, correctness", () => {
  let vec = [1, 1];
  const expected = [0.7071067811865475, 0.7071067811865475];

  let res = util.normalizeVector(vec);
  expect(res).toEqual(expected);
});

test("normalizeVector, zero vector", () => {
  let vec = [0, 0];
  const expected = [0, 0];

  let res = util.normalizeVector(vec);
  expect(res).toEqual(expected);

  res = util.normalizeVector([]);
  expect(res.length).toBe(0);
});

test("calculateQueryTfidf, correctness", () => {
  const tf = new Map(
    Object.entries({
      machine: 0.5,
      learning: 0.5,
    }),
  );
  const idf = new Map(
    Object.entries({
      machine: 1.4,
      learning: 2.1,
      teaches: 2.1,
      how: 2.1,
      to: 2.1,
      learn: 2.1,
      translation: 2.1,
      is: 1.4,
      my: 2.1,
      favorite: 2.1,
      subject: 2.1,
      term: 2.1,
      frequency: 2.1,
      and: 2.1,
      inverse: 2.1,
      document: 2.1,
      important: 2.1,
    }),
  );
  const tfidf = new Map(
    Object.entries({
      doc1: new Map(
        Object.entries({
          machine: 0.4,
          learning: 0.3,
          teaches: 0.3,
          how: 0.3,
          to: 0.3,
          learn: 0.3,
        }),
      ),
      doc2: new Map(
        Object.entries({
          machine: 0.23,
          translation: 0.35,
          is: 0.23,
          my: 0.35,
          favorite: 0.35,
          subject: 0.35,
        }),
      ),
      doc3: new Map(
        Object.entries({
          term: 0.26,
          frequency: 0.52,
          and: 0.26,
          inverse: 0.26,
          document: 0.26,
          is: 0.18,
          important: 0.26,
        }),
      ),
    }),
  );

  const queryVecExpected = [0.7, 1.05];
  const docVecsExpected = new Map(
    Object.entries({
      doc1: [0.4, 0.3],
      doc2: [0.23, 0],
      doc3: [0.0, 0.0],
    }),
  );

  let [queryVec, docVecs] = util.calculateQueryTfidf(tf, idf, tfidf);

  queryVec.forEach((val, i) => {
    expect(val).toBeCloseTo(queryVecExpected[i], 3);
  });

  // Assert each term's tf-idf value in each document
  Object.keys(docVecs).forEach((doc) => {
    Object.keys(docVecs[doc]).forEach((val, i) => {
      expect(val).toBeCloseTo(docVecsExpected[doc][i], 3);
    });
  });
});

test("calulateQueryTfidf, empty query", () => {
  let [queryVec, docVecs] = util.calculateQueryTfidf([], null, null);

  expect(queryVec.length).toBe(0);
  expect(docVecs.size).toBe(0);
});

test("calulateQueryTfidf, empty tfidf and idf", () => {
  const tf = new Map(
    Object.entries({
      machine: 0.5,
      learning: 0.5,
    }),
  );
  const idf = new Map(
    Object.entries({
      machine: 1.4,
      learning: 2.1,
    }),
  );
  const tfidf = new Map(
    Object.entries({
      doc1: new Map(
        Object.entries({
          machine: 0.4,
        }),
      ),
    }),
  );
  let [queryVec, docVecs] = util.calculateQueryTfidf(tf, idf, new Map());
  expect(queryVec.length).toBe(0);
  expect(docVecs.size).toBe(0);

  [queryVec, docVecs] = util.calculateQueryTfidf(tf, new Map(), tfidf);
  expect(queryVec.length).toBe(0);
  expect(docVecs.size).toBe(0);

  [queryVec, docVecs] = util.calculateQueryTfidf(tf, new Map(), new Map());
  expect(queryVec.length).toBe(0);
  expect(docVecs.size).toBe(0);
});

// --------------------------------------------------------------------------

// dummy testing for debugging purposes
test("debugging, dummy", async () => {
  const res = util.calculateTfidf(courses2);
  const tfidf = res[0];
  const idf = res[1];

  // console.log("tfidf", tfidf);
  // console.log("idf", idf);
});
