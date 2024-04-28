#!/usr/bin/env node

import { readFile } from 'node:fs/promises';
import Koa from 'koa';
import Router from '@koa/router';
import { koaBody } from 'koa-body';
import serve from 'koa-static';
import util from '../distribution/util/util.js';
const { serialize, deserialize } = util;
import fetch from 'node-fetch';

const router = new Router()
    .post('/search', koaBody(), async (ctx, next) => {
        let {query, course, department} = ctx.request.body;
        course = course.toUpperCase();
        department = department.toUpperCase();
        ctx.body = await forward(renderSearch, '/client/search', query, course, department);
        ctx.type = 'text/html';
    })
    .post('/register', koaBody(), async (ctx, next) => {
        const {code, token} = ctx.request.body;
        ctx.body = await forward(renderRegister, '/client/register', code, token);
        ctx.type = 'text/html';
    });

new Koa()
    .use(serve('static'))
    .use(router.routes())
    .use(router.allowedMethods())
    .listen(80);

async function forward(render, pathname, ...args) {
    const url = new URL(pathname, 'http://127.0.0.1:8080/');
    const method = 'POST';
    const body = serialize([...args]);
    try {
        let result = await fetch(url, {method, body});
        result = await result.text();
        const [e, v] = deserialize(result);
        if (e) {
            throw e;
        }
        return render(v);
    } catch (error) {
        return renderError(error);
    }
}

function renderError(error) {
    let prefix = '';
    let ret = '';
    while (error) {
        ret += prefix + error.message;
        error = error.cause;
        prefix = ': ';
    }
    return ret;
}

function renderSearch(results) {
    if (results.length === 0) {
        return 'no results';
    }
    return String.prototype.concat(...results.map(([code, detail]) => `
        <article>
            <h3>${code}: ${detail.title}</h3>
            <p>${detail.description}</p>
        </article>
    `));
}

function renderRegister(result) {
    return `registration successful`;
}
