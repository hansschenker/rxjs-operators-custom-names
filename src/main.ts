import './style.css';
import { of, EMPTY, throwError, Observable, fromEvent, interval, timer, Subject } from 'rxjs';
import {
  transformWith, accumulate, withPrevious, replaceWith, collectN, expandRecursively,
  keepIf, limitTo, dropFirst, lastN, stopWhenNot, skipDuplicates, uniqueValues, atIndex,
  runInSequence, waitForAll, pairEmissions, splitBy,
  prependWith, joinSequentially, pairWithLatest,
  handleError, tryAgain,
  foldInto, countValues, maximum, minimum, collectAll,
  orDefault, allMatch, firstMatch, firstMatchIndex, hasNoValues,
  sideEffect,
  afterSilenceOf, limitRateTo, stopWhen, delayBy,
  transformSwitching, transformSequentially, transformConcurrently,
} from './index';

// ── static helpers ─────────────────────────────────────────────────────────────

function run<T>(obs: Observable<T>): T[] {
  const out: T[] = [];
  obs.subscribe({ next: v => out.push(v), error: () => {} });
  return out;
}

function fmt(vals: unknown[]): string {
  if (vals.length === 0) return '(empty)';
  return vals.map(v => JSON.stringify(v)).join(', ');
}

function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ── run static demos ───────────────────────────────────────────────────────────

const r_transformWith      = run(of(1, 2, 3).pipe(transformWith(x => x * 2)));
const r_accumulate         = run(of(1, 2, 3, 4, 5).pipe(accumulate((sum, n) => sum + n, 0)));
const r_withPrevious       = run(of(1, 2, 3, 4).pipe(withPrevious()));
const r_replaceWith        = run(of('a', 'b', 'c').pipe(replaceWith('x')));
const r_collectN           = run(of(1, 2, 3, 4, 5, 6).pipe(collectN(2)));
const r_expandRecursively  = run(of(1).pipe(expandRecursively(n => n >= 8 ? EMPTY : of(n * 2))));
const r_keepIf             = run(of(1, 2, 3, 4, 5).pipe(keepIf(x => x % 2 === 0)));
const r_limitTo            = run(of(1, 2, 3, 4, 5).pipe(limitTo(3)));
const r_dropFirst          = run(of(1, 2, 3, 4, 5).pipe(dropFirst(2)));
const r_lastN              = run(of(1, 2, 3, 4, 5).pipe(lastN(2)));
const r_stopWhenNot        = run(of(1, 2, 3, 4, 5).pipe(stopWhenNot(x => x < 4)));
const r_skipDuplicates     = run(of(1, 1, 2, 2, 3, 1).pipe(skipDuplicates()));
const r_uniqueValues       = run(of(1, 2, 1, 3, 2, 3).pipe(uniqueValues()));
const r_atIndex            = run(of('a', 'b', 'c', 'd').pipe(atIndex(2)));
const r_runInSequence      = run(runInSequence(of(1, 2), of(3, 4)));
const r_waitForAll         = run(waitForAll([of('a'), of('b'), of('c')]));
const r_pairEmissions      = run(pairEmissions([of(1, 2, 3), of('a', 'b', 'c')]));
const [evens$, odds$]      = splitBy(of(1, 2, 3, 4, 5), x => x % 2 === 0);
const r_splitByEvens       = run(evens$);
const r_splitByOdds        = run(odds$);
const r_prependWith        = run(of(3, 4, 5).pipe(prependWith(1, 2)));
const r_joinSequentially   = run(of(of(1, 2), of(3, 4)).pipe(joinSequentially()));
const r_pairWithLatest     = run(of(1, 2, 3).pipe(pairWithLatest(of(10))));
const r_handleError        = run(throwError(() => new Error('boom')).pipe(handleError(() => of('recovered'))));
let retryCount = 0;
const r_tryAgain = run(new Observable<string>(sub => {
  retryCount++;
  if (retryCount < 3) sub.error('fail');
  else { sub.next('succeeded on attempt ' + retryCount); sub.complete(); }
}).pipe(tryAgain(5)));
const r_foldInto    = run(of(1, 2, 3, 4, 5).pipe(foldInto((sum, n) => sum + n, 0)));
const r_countValues = run(of('a', 'b', 'c', 'd').pipe(countValues()));
const r_maximum     = run(of(3, 1, 4, 1, 5, 9, 2, 6).pipe(maximum()));
const r_minimum     = run(of(3, 1, 4, 1, 5, 9, 2, 6).pipe(minimum()));
const r_collectAll  = run(of(1, 2, 3).pipe(collectAll()));
const r_orDefault       = run(EMPTY.pipe(orDefault('nothing here')));
const r_allMatch        = run(of(2, 4, 6, 8).pipe(allMatch(x => x % 2 === 0)));
const r_firstMatch      = run(of(1, 2, 3, 4, 5).pipe(firstMatch(x => x > 3)));
const r_firstMatchIndex = run(of(1, 2, 3, 4, 5).pipe(firstMatchIndex(x => x > 3)));
const r_hasNoValues     = run(EMPTY.pipe(hasNoValues()));
const tapLog: number[] = [];
const r_sideEffect = run(of(1, 2, 3).pipe(sideEffect(x => tapLog.push(x)), transformWith(x => x * 10)));

// ── card builders ──────────────────────────────────────────────────────────────

function card(friendly: string, original: string, code: string, output: unknown[], extra?: string): string {
  return `
<div class="card">
  <div class="card-header">
    <span class="friendly">${friendly}</span>
    <span class="badge">${original}</span>
  </div>
  <pre class="code">${esc(code)}</pre>
  <div class="output"><span class="label">emits →</span>${esc(fmt(output))}</div>
  ${extra ? `<div class="output">${extra}</div>` : ''}
</div>`;
}

function interactiveCard(friendly: string, original: string, controls: string, code: string, logId: string): string {
  return `
<div class="card card--interactive">
  <div class="card-header">
    <span class="friendly">${friendly}</span>
    <span class="badges">
      <span class="badge">${original}</span>
      <span class="badge badge--live">● live</span>
    </span>
  </div>
  <div class="demo-controls">${controls}</div>
  <pre class="code">${esc(code)}</pre>
  <div class="demo-log" id="${logId}"><p class="log-empty">waiting…</p></div>
</div>`;
}

function section(title: string, subtitle: string, cards: string[]): string {
  return `
<section class="section">
  <div class="section-header">
    <h2>${title}</h2>
    <p>${subtitle}</p>
  </div>
  <div class="grid">${cards.join('')}</div>
</section>`;
}

// ── live-log helpers ───────────────────────────────────────────────────────────

function ts(): string {
  return new Date().toLocaleTimeString('en', { hour12: false });
}

function appendLog(id: string, text: string): HTMLElement {
  const log = document.getElementById(id)!;
  log.querySelector('.log-empty')?.remove();
  const row = document.createElement('div');
  row.className = 'log-entry';
  row.innerHTML = `<span class="log-time">${ts()}</span><span class="log-value">${text}</span>`;
  log.appendChild(row);
  log.scrollTop = log.scrollHeight;
  while (log.children.length > 20) log.removeChild(log.firstChild!);
  return row;
}

// ── interactive demo wiring (runs after innerHTML is set) ──────────────────────

function setupInteractiveDemos() {

  // 1. afterSilenceOf — debounce typed input
  const debounceInput = document.getElementById('i-debounce') as HTMLInputElement;
  fromEvent<Event>(debounceInput, 'input').pipe(
    transformWith(e => (e.target as HTMLInputElement).value),
    afterSilenceOf(500)
  ).subscribe(val => appendLog('l-debounce', JSON.stringify(val)));

  // 2. limitRateTo — throttle clicks (500 ms window)
  const throttleBtn = document.getElementById('i-throttle')!;
  let allClicks = 0;
  let passedClicks = 0;
  fromEvent(throttleBtn, 'click').subscribe(() => { allClicks++; });
  fromEvent(throttleBtn, 'click').pipe(limitRateTo(500)).subscribe(() => {
    passedClicks++;
    appendLog('l-throttle', `click passed (${passedClicks} of ${allClicks} total)`);
  });

  // 3. stopWhen — interval controlled by start / stop buttons
  const stopStart = document.getElementById('i-stop-start')!;
  const stopStop  = document.getElementById('i-stop-stop')!;
  let stop$ = new Subject<void>();
  let stopRunning = false;

  fromEvent(stopStart, 'click').subscribe(() => {
    if (stopRunning) return;
    stopRunning = true;
    stop$ = new Subject<void>();
    appendLog('l-stop', 'started');
    interval(1000).pipe(stopWhen(stop$)).subscribe({
      next: n => appendLog('l-stop', `tick ${n + 1}`),
      complete: () => { stopRunning = false; appendLog('l-stop', 'completed ✓'); },
    });
  });
  fromEvent(stopStop, 'click').subscribe(() => { if (stopRunning) stop$.next(); });

  // 4. delayBy — each click arrives 1 s later; entry updates in place
  const delayBtn = document.getElementById('i-delay')!;
  fromEvent(delayBtn, 'click').subscribe(() => {
    const sentAt = ts();
    const row = appendLog('l-delay', `⏳ sending at ${sentAt}…`);
    of(null).pipe(delayBy(1000)).subscribe(() => {
      (row.querySelector('.log-value') as HTMLElement).textContent =
        `✓ arrived at ${ts()} (sent ${sentAt})`;
    });
  });

  // 5. transformSwitching — typeahead: cancelled searches stay in log as strikethrough
  const switchInput = document.getElementById('i-switch') as HTMLInputElement;
  let pendingRow: HTMLElement | null = null;

  fromEvent<Event>(switchInput, 'input').pipe(
    transformWith(e => (e.target as HTMLInputElement).value.trim()),
    keepIf(q => q.length > 0),
    afterSilenceOf(300),
    transformSwitching(query => {
      if (pendingRow) (pendingRow.querySelector('.log-value') as HTMLElement).classList.add('cancelled');
      pendingRow = appendLog('l-switch', `searching "${query}"…`);
      return timer(900).pipe(transformWith(() => query));
    })
  ).subscribe(query => {
    if (pendingRow) {
      (pendingRow.querySelector('.log-value') as HTMLElement).textContent = `✓ results for "${query}"`;
      pendingRow = null;
    }
  });

  // 6. transformSequentially — tasks run one at a time (queue)
  const concatBtn = document.getElementById('i-concat')!;
  let concatN = 0;
  fromEvent(concatBtn, 'click').pipe(
    transformSequentially(() => {
      const n = ++concatN;
      appendLog('l-concat', `▶ task ${n} started`);
      return timer(1500).pipe(transformWith(() => n));
    })
  ).subscribe(n => appendLog('l-concat', `✓ task ${n} done`));

  // 7. transformConcurrently — all tasks start immediately in parallel
  const mergeBtn = document.getElementById('i-merge')!;
  let mergeN = 0;
  fromEvent(mergeBtn, 'click').pipe(
    transformConcurrently(() => {
      const n = ++mergeN;
      appendLog('l-merge', `▶ task ${n} started (parallel)`);
      return timer(1500).pipe(transformWith(() => n));
    })
  ).subscribe(n => appendLog('l-merge', `✓ task ${n} done`));
}

// ── render ─────────────────────────────────────────────────────────────────────

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
<header class="page-header">
  <h1>RxJS — Human-Friendly Names</h1>
  <p>Every operator re-exported with a descriptive alias. Import from <code>src/index.ts</code>.</p>
</header>
<main>

${section('Transformation', 'Apply functions to reshape, accumulate, or expand emitted values.', [
  card('transformWith', 'map',       `of(1, 2, 3).pipe(\n  transformWith(x => x * 2)\n)`,              r_transformWith),
  card('accumulate',   'scan',       `of(1, 2, 3, 4, 5).pipe(\n  accumulate((sum, n) => sum + n, 0)\n)`, r_accumulate),
  card('withPrevious', 'pairwise',   `of(1, 2, 3, 4).pipe(\n  withPrevious()\n)`,                     r_withPrevious),
  card('replaceWith',  'mapTo',      `of('a', 'b', 'c').pipe(\n  replaceWith('x')\n)`,                r_replaceWith),
  card('collectN',     'bufferCount',`of(1, 2, 3, 4, 5, 6).pipe(\n  collectN(2)\n)`,                  r_collectN),
  card('expandRecursively', 'expand',`of(1).pipe(\n  expandRecursively(n => n >= 8 ? EMPTY : of(n * 2))\n)`, r_expandRecursively),
])}

${section('Filtering', 'Decide which values pass through and which are suppressed.', [
  card('keepIf',        'filter',              `of(1, 2, 3, 4, 5).pipe(\n  keepIf(x => x % 2 === 0)\n)`,  r_keepIf),
  card('limitTo',       'take',                `of(1, 2, 3, 4, 5).pipe(\n  limitTo(3)\n)`,                r_limitTo),
  card('dropFirst',     'skip',                `of(1, 2, 3, 4, 5).pipe(\n  dropFirst(2)\n)`,              r_dropFirst),
  card('lastN',         'takeLast',            `of(1, 2, 3, 4, 5).pipe(\n  lastN(2)\n)`,                  r_lastN),
  card('stopWhenNot',   'takeWhile',           `of(1, 2, 3, 4, 5).pipe(\n  stopWhenNot(x => x < 4)\n)`,  r_stopWhenNot),
  card('skipDuplicates','distinctUntilChanged',`of(1, 1, 2, 2, 3, 1).pipe(\n  skipDuplicates()\n)`,      r_skipDuplicates),
  card('uniqueValues',  'distinct',            `of(1, 2, 1, 3, 2, 3).pipe(\n  uniqueValues()\n)`,        r_uniqueValues),
  card('atIndex',       'elementAt',           `of('a', 'b', 'c', 'd').pipe(\n  atIndex(2)\n)`,          r_atIndex),
])}

${section('Join Creation', 'Combine multiple source observables into one stream.', [
  card('runInSequence', 'concat',
    `runInSequence(\n  of(1, 2),\n  of(3, 4)\n)`, r_runInSequence),
  card('waitForAll', 'forkJoin',
    `waitForAll([\n  of('a'), of('b'), of('c')\n])`, r_waitForAll),
  card('pairEmissions', 'zip',
    `pairEmissions([\n  of(1, 2, 3),\n  of('a', 'b', 'c')\n])`, r_pairEmissions),
  card('splitBy', 'partition',
    `const [evens, odds] = splitBy(\n  of(1, 2, 3, 4, 5),\n  x => x % 2 === 0\n)`,
    r_splitByEvens, `<span class="label">odds  →</span>${esc(fmt(r_splitByOdds))}`),
])}

${section('Join (Pipeable)', 'Combine the source stream with other observables mid-pipeline.', [
  card('prependWith',    'startWith',    `of(3, 4, 5).pipe(\n  prependWith(1, 2)\n)`,           r_prependWith),
  card('joinSequentially','concatAll',   `of(of(1, 2), of(3, 4)).pipe(\n  joinSequentially()\n)`, r_joinSequentially),
  card('pairWithLatest', 'withLatestFrom',`of(1, 2, 3).pipe(\n  pairWithLatest(of(10))\n)`,     r_pairWithLatest),
])}

${section('Error Handling', 'Catch errors and decide whether to recover or retry.', [
  card('handleError', 'catchError',
    `throwError(() => new Error('boom')).pipe(\n  handleError(() => of('recovered'))\n)`, r_handleError),
  card('tryAgain', 'retry',
    `new Observable(sub => {\n  if (++n < 3) sub.error('fail');\n  else { sub.next('ok'); sub.complete(); }\n}).pipe(tryAgain(5))`,
    r_tryAgain),
])}

${section('Mathematical & Aggregate', 'Compute a single result over the entire completed sequence.', [
  card('foldInto',    'reduce',  `of(1, 2, 3, 4, 5).pipe(\n  foldInto((sum, n) => sum + n, 0)\n)`, r_foldInto),
  card('countValues', 'count',   `of('a', 'b', 'c', 'd').pipe(\n  countValues()\n)`,               r_countValues),
  card('maximum',     'max',     `of(3, 1, 4, 1, 5, 9, 2, 6).pipe(\n  maximum()\n)`,               r_maximum),
  card('minimum',     'min',     `of(3, 1, 4, 1, 5, 9, 2, 6).pipe(\n  minimum()\n)`,               r_minimum),
  card('collectAll',  'toArray', `of(1, 2, 3).pipe(\n  collectAll()\n)`,                            r_collectAll),
])}

${section('Conditional & Boolean', 'Evaluate a condition against the whole sequence.', [
  card('orDefault',       'defaultIfEmpty', `EMPTY.pipe(\n  orDefault('nothing here')\n)`,               r_orDefault),
  card('allMatch',        'every',          `of(2, 4, 6, 8).pipe(\n  allMatch(x => x % 2 === 0)\n)`,    r_allMatch),
  card('firstMatch',      'find',           `of(1, 2, 3, 4, 5).pipe(\n  firstMatch(x => x > 3)\n)`,     r_firstMatch),
  card('firstMatchIndex', 'findIndex',      `of(1, 2, 3, 4, 5).pipe(\n  firstMatchIndex(x => x > 3)\n)`,r_firstMatchIndex),
  card('hasNoValues',     'isEmpty',        `EMPTY.pipe(\n  hasNoValues()\n)`,                           r_hasNoValues),
])}

${section('Utility', 'Side effects and observable lifecycle helpers.', [
  card('sideEffect', 'tap',
    `of(1, 2, 3).pipe(\n  sideEffect(x => log.push(x)),\n  transformWith(x => x * 10)\n)`,
    r_sideEffect, `<span class="label">logged →</span>${esc(fmt(tapLog))}`),
])}

${section('Time-Based Operators — Interactive', 'Try these live. Each card wires real RxJS observables to the controls below.', [

  interactiveCard('afterSilenceOf', 'debounceTime',
    `<input class="demo-input" id="i-debounce" placeholder="Type anything…" autocomplete="off">`,
    `fromEvent(input, 'input').pipe(\n  transformWith(e => e.target.value),\n  afterSilenceOf(500)\n)`,
    'l-debounce'),

  interactiveCard('limitRateTo', 'throttleTime',
    `<button class="demo-btn" id="i-throttle">Click me!</button>`,
    `fromEvent(btn, 'click').pipe(\n  limitRateTo(500)\n)`,
    'l-throttle'),

  interactiveCard('stopWhen', 'takeUntil',
    `<button class="demo-btn" id="i-stop-start">▶ Start</button>
     <button class="demo-btn" id="i-stop-stop">■ Stop</button>`,
    `interval(1000).pipe(\n  stopWhen(stop$)\n)`,
    'l-stop'),

  interactiveCard('delayBy', 'delay',
    `<button class="demo-btn" id="i-delay">Send</button>`,
    `fromEvent(btn, 'click').pipe(\n  delayBy(1000)\n)`,
    'l-delay'),

  interactiveCard('transformSwitching', 'switchMap',
    `<input class="demo-input" id="i-switch" placeholder="Type to search…" autocomplete="off">`,
    `fromEvent(input, 'input').pipe(\n  afterSilenceOf(300),\n  transformSwitching(q => search$(q))\n)`,
    'l-switch'),

  interactiveCard('transformSequentially', 'concatMap',
    `<button class="demo-btn" id="i-concat">Add Task</button>`,
    `fromEvent(btn, 'click').pipe(\n  transformSequentially(() => task$(1500))\n)`,
    'l-concat'),

  interactiveCard('transformConcurrently', 'mergeMap',
    `<button class="demo-btn" id="i-merge">Add Task</button>`,
    `fromEvent(btn, 'click').pipe(\n  transformConcurrently(() => task$(1500))\n)`,
    'l-merge'),

])}

</main>`;

setupInteractiveDemos();
