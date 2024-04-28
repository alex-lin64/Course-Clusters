const https = require("node:https");
const { promisify } = require("node:util");
const { JSDOM } = require("jsdom");
const serialization = require("./serialization");
const id = require("./id");
const natural = require("natural");
const fs = require("fs");
// stemmer for created the inverted index
const porterStemmer = natural.PorterStemmer;
// stop words
const stopwords = require("./stopwords");

function getActualKey(key, value) {
  return key === null ? id.getID(value) : key;
}

async function whichHashTo(keys, gid, hash) {
  let nodes = await distribution.local.async.groups.get(gid);
  const nids = Object.values(nodes).map((node) => id.getNID(node));
  const result = keys.map((key) => {
    const nid = hash(id.getID(key), nids);
    return { nid, key };
  });
  const ret = new Map(nids.map((node) => [node, []]));
  for (const { nid, key } of result) {
    ret.get(nid).push(key);
  }
  return ret;
}

// sends message to
async function callOnHolder({
  key,
  value,
  gid,
  hash,
  message,
  service,
  method,
}) {
  let nodes = await distribution.local.async.groups.get(gid);

  nodes = Object.values(nodes);
  nodes = nodes.map((node) => [id.getNID(node), node]);
  nodes = Object.fromEntries(nodes);

  let kid = value === null ? key : getActualKey(key, value);
  kid = id.getID(kid);

  const nid = hash(kid, Object.keys(nodes));
  const node = nodes[nid];

  return await distribution.local.async.comm.send(message, {
    node,
    service,
    method,
  });
}

async function sendToAll({ message, service, method, gid, exclude, subset }) {
  let nodes = await distribution.local.async.groups.get(gid);
  nodes = Object.values(nodes).filter((node) => id.getSID(node) !== exclude);
  if (subset) {
    const newNodes = [];
    subset = subset(nodes);
    while (newNodes.length < subset) {
      const index = Math.floor(nodes.length * Math.random());
      newNodes.push(...nodes.splice(index, 1));
    }
    nodes = newNodes;
  }
  let sidToValue = {};
  let sidToError = {};
  const settled = await Promise.allSettled(
    nodes.map(
      async (node) =>
        await distribution.local.async.comm.send(message, {
          node,
          service,
          method,
        }),
    ),
  );
  for (let i = 0; i < nodes.length; i++) {
    const sid = id.getSID(nodes[i]);
    const { status, value, reason } = settled[i];
    if (status === "fulfilled" && value !== null) {
      sidToValue[sid] = value;
    }
    if (status === "rejected" && reason != null) {
      sidToError[sid] = reason;
    }
  }
  return [sidToError, sidToValue];
}

async function getPageContents(url) {
  url = new URL(url);
  let body = [];
  await new Promise((resolve, reject) => {
    https
      .request(url, (res) => {
        res.on("data", (chunk) => body.push(chunk));
        res.on("end", resolve);
      })
      .on("error", reject)
      .end();
  });
  return Buffer.concat(body).toString();
}

function getUrls(url, body) {
  const ret = [];
  const dom = new JSDOM(body);
  for (let link of dom.window.document.querySelectorAll("a[href]")) {
    link = link.getAttribute("href");
    try {
      link = new URL(link, url);
    } catch (e) {
      console.trace("failed to build url from", e, link, url);
      continue;
    }
    link = link.href;
    ret.push(link);
  }
  return ret;
}

function asyncRPC(func) {
  const installation = distribution.local.async.rpc.install(func);
  return eval(`(...args) => {
    const callback = args.pop() || function() {};
    let message = [args, ${JSON.stringify(installation)}];
    const node = ${JSON.stringify(global.nodeConfig)};
    const service = 'rpc';
    const method = 'call';
    global.distribution.local.async.comm.send(message, {node, service, method})
      .then(v => callback(null, v))
      .catch(e => callback(e, null));
  }`);
}

function createRPC(func) {
  return asyncRPC(promisify(func));
}

function toAsync(func) {
  return function (...args) {
    const callback = args.pop() || function () {};
    try {
      const result = func(...args);
      callback(null, result);
    } catch (error) {
      callback(error, null);
    }
  };
}

/* 
calculates tf-idf for all courses stored on a course node

params: 
  - courses: map of courseCode -> course details

return:
  - array, [tfidf, idf], where
    - tfidf: map, courseCode -> map(term -> tf-idf)
    - idf: map, term -> idf
*/
function calculateTfidf(courses) {
  let tfidf = new Map(); // courseCode -> map(term -> tf)
  let idf = new Map(); // term -> idf

  // replace punctuations
  const regex = /[!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~]/g;
  // iterate over each course to create tf-idf calculates
  courses.forEach((details, courseCode) => {
    if (Object.keys(details).length === 0) {
      // empty course
      return [tfidf, idf];
    }

    let subject =
      details.code && details.code.subject
        ? details.code.subject.toLowerCase().replace(regex, "")
        : "";
    let number =
      details.code && details.code.number
        ? details.code.number.toLowerCase().replace(regex, "")
        : "";
    let title = details.title
      ? details.title.toLowerCase().replace(regex, "")
      : "";
    let description = details.description
      ? details.description.toLowerCase().replace(regex, "")
      : "";
    let instructors = details.offerings
      ? details.offerings.flatMap((offering) =>
          offering.instructors
            ? offering.instructors.flatMap((ins) =>
                ins.toLowerCase().split(" "),
              )
            : [],
        )
      : [];

    // process text: merge title and description, split, stem, remove stop words
    let splitTerms = [
      ...title.split(" "),
      ...description.split(" "),
      ...instructors,
      subject,
      number,
      courseCode.toLowerCase(),
    ];
    let processedTerms = stemAndRemoveStopWords(splitTerms);
    // merge repeat words and map to frequency count and course code
    let termToFreq = calculateTf(processedTerms, idf, courseCode);
    // set tf map of courseCode -> map(term -> tf)
    tfidf.set(courseCode, termToFreq);
  });

  // calcualte idf = 1 + log(N / (1 + c_i))
  // N = size of courses stored on this node
  // c_i = number of courses term_i appears in
  const N = courses.size;
  idf.forEach((cntSet, term) => {
    let c_i = cntSet.size;
    let idf_i = 1 + Math.log((N + 1) / (1 + c_i));
    idf.set(term, idf_i);
  });

  // calculate tf-idf,
  tfidf.forEach((terms, course) => {
    terms.forEach((tf, term) => {
      let termTfIdf = tf * idf.get(term);
      tfidf.get(course).set(term, termTfIdf);
    });
  });

  return [tfidf, idf];
}

/*
Calculates the tf of all words in a document

params:
  - processedTerms: arr, list of all words in document, already preprocessed
  - idf: [optional], map, term -> set(docments), used to calculate idf.  Set to 
      null if not needed
  - courseCode: [optional], string, subject + code of a course, used in 
      conjunction w/ idf as optimization in tf-idf calculations.  Set to null if
      not needed.

return:
  - map, term -> tf
*/
function calculateTf(processedTerms, idf, courseCode) {
  let tfAddition = 1 / processedTerms.length;

  // merge repeat words and map to frequency count and course code
  let termToFreq = processedTerms.reduce((count, word) => {
    // update tf for word
    let freqUpdate = count.get(word) || 0;
    freqUpdate += tfAddition;
    count.set(word, freqUpdate);

    // update idf mapping count
    // initialize as term -> set(course1, course2, ...)
    // to calcualte idf, take size of set as c_i, freq of term in doc
    if (idf !== null && courseCode !== null) {
      let idfUpdate = idf.get(word) || new Set();
      idfUpdate.add(courseCode);
      idf.set(word, idfUpdate);
    }

    return count;
  }, new Map());

  return termToFreq;
}

/*
Stems and removes stop words from a list of words

param:
  - words: arr, list of strings

return:
  - arr, list of strings after stemming and removing stop words
*/
function stemAndRemoveStopWords(words) {
  if (!words || !words.length) {
    return [];
  }
  words = words.filter((word) => !stopwords.stopwords.includes(word));
  words = words.map((word) => porterStemmer.stem(word));
  return words;
}

/*
Calculates cos similarity of two arrays
**credit: https://stackoverflow.com/questions/51362252/javascript-cosine-similarity-function

params:
  - A, list of floats of length n
  - B, list of floats of length n

return:
  - float, cos similiarity of the two arrays
*/
function cosinesim(A, B) {
  if (A.length != B.length || A.length == 0 || B.length == 0) {
    return 0;
  }

  // normalize A and B
  A = normalizeVector(A);
  B = normalizeVector(B);

  var dotproduct = 0;
  var mA = 0;
  var mB = 0;

  for (var i = 0; i < A.length; i++) {
    dotproduct += A[i] * B[i];
    mA += A[i] * A[i];
    mB += B[i] * B[i];
  }

  let denom = Math.sqrt(mA) * Math.sqrt(mB);
  // check for 0 denom
  if (denom == 0) {
    return 0;
  }
  var similarity = dotproduct / denom;

  return similarity;
}

/*
Normalize a vector

params:
  - vector to normalize 

return:
  - arr, the normalized vector
*/
function normalizeVector(vector) {
  if (vector.length == 0) {
    return [];
  }

  const magnitude = Math.sqrt(vector.reduce((acc, val) => acc + val * val, 0));
  if (magnitude === 0) {
    return vector.slice(); // Return a copy of the original vector if the magnitude is 0 to avoid division by zero
  }
  return vector.map((val) => val / magnitude);
}

/*
Calculate query tfidf wrt current node's document's tfidf and idf maps

params:
  - tf: map, term -> tf of query
  - idf: map, term -> idf of all docs, should not be empty
  - tfidf: map, courseCode -> map(term -> tf-idf) of all docs, should not be empty

return:
  - arr, length 2, first element is the tfidf of query, second element is 
      map of doc to tf-tdf of each word in the query, in same order as the
      first element
*/
function calculateQueryTfidf(tf, idf, tfidf) {
  const queryVec = []; // list of tf-idf of query
  const docVecs = new Map(); // doc -> arr of tf-idf in same order of queryVec

  if (idf == null || tfidf == null || idf.size == 0 || tfidf.size == 0) {
    return [queryVec, docVecs];
  }

  tf.forEach((val, term) => {
    if (!idf.has(term)) {
      return;
    }
    let curTfidf = val * idf.get(term);
    // create query and docVecs to calculate cos similarity
    queryVec.push(curTfidf);
    tfidf.forEach((termsToTfidf, courseCode) => {
      let vecUpdate = docVecs.get(courseCode) || [];
      let docTfidf = termsToTfidf.get(term) || 0;
      vecUpdate.push(docTfidf);
      docVecs.set(courseCode, vecUpdate);
    });
  });

  return [queryVec, docVecs];
}

module.exports = {
  serialize: serialization.serialize,
  deserialize: serialization.deserialize,
  sendToAll,
  getActualKey,
  whichHashTo,
  callOnHolder,
  getPageContents,
  getUrls,
  id,
  calculateTfidf,
  calculateTf,
  calculateQueryTfidf,
  stemAndRemoveStopWords,
  cosinesim,
  normalizeVector,
  wire: { createRPC, asyncRPC, toAsync },
};
