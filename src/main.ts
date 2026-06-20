import './style.css';
import { of, EMPTY, throwError, Observable } from 'rxjs';
import {
  transformWith, accumulate, withPrevious, replaceWith, collectN, expandRecursively,
  keepIf, limitTo, dropFirst, lastN, stopWhenNot, skipDuplicates, uniqueValues, atIndex,
  runInSequence, waitForAll, pairEmissions, splitBy,
  prependWith, joinSequentially, pairWithLatest,
  handleError, tryAgain,
  foldInto, countValues, maximum, minimum, collectAll,
  orDefault, allMatch, firstMatch, firstMatchIndex, hasNoValues,
  sideEffect,
} from './index';

// ── helpers ────────────────────────────────────────────────────────────────────

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

// ── run all demos ──────────────────────────────────────────────────────────────

// Transformation
const r_transformWith      = run(of(1, 2, 3).pipe(transformWith(x => x * 2)));
const r_accumulate         = run(of(1, 2, 3, 4, 5).pipe(accumulate((sum, n) => sum + n, 0)));
const r_withPrevious       = run(of(1, 2, 3, 4).pipe(withPrevious()));
const r_replaceWith        = run(of('a', 'b', 'c').pipe(replaceWith('x')));
const r_collectN           = run(of(1, 2, 3, 4, 5, 6).pipe(collectN(2)));
const r_expandRecursively  = run(of(1).pipe(expandRecursively(n => n >= 8 ? EMPTY : of(n * 2))));

// Filtering
const r_keepIf        = run(of(1, 2, 3, 4, 5).pipe(keepIf(x => x % 2 === 0)));
const r_limitTo       = run(of(1, 2, 3, 4, 5).pipe(limitTo(3)));
const r_dropFirst     = run(of(1, 2, 3, 4, 5).pipe(dropFirst(2)));
const r_lastN         = run(of(1, 2, 3, 4, 5).pipe(lastN(2)));
const r_stopWhenNot   = run(of(1, 2, 3, 4, 5).pipe(stopWhenNot(x => x < 4)));
const r_skipDuplicates = run(of(1, 1, 2, 2, 3, 1).pipe(skipDuplicates()));
const r_uniqueValues  = run(of(1, 2, 1, 3, 2, 3).pipe(uniqueValues()));
const r_atIndex       = run(of('a', 'b', 'c', 'd').pipe(atIndex(2)));

// Join Creation
const r_runInSequence  = run(runInSequence(of(1, 2), of(3, 4)));
const r_waitForAll     = run(waitForAll([of('a'), of('b'), of('c')]));
const r_pairEmissions  = run(pairEmissions([of(1, 2, 3), of('a', 'b', 'c')]));
const [evens$, odds$]  = splitBy(of(1, 2, 3, 4, 5), x => x % 2 === 0);
const r_splitByEvens   = run(evens$);
const r_splitByOdds    = run(odds$);

// Join (pipeable)
const r_prependWith      = run(of(3, 4, 5).pipe(prependWith(1, 2)));
const r_joinSequentially = run(of(of(1, 2), of(3, 4)).pipe(joinSequentially()));
const r_pairWithLatest   = run(of(1, 2, 3).pipe(pairWithLatest(of(10))));

// Error Handling
const r_handleError = run(
  throwError(() => new Error('boom')).pipe(handleError(() => of('recovered')))
);
let retryCount = 0;
const r_tryAgain = run(
  new Observable<string>(sub => {
    retryCount++;
    if (retryCount < 3) sub.error('fail');
    else { sub.next('succeeded on attempt ' + retryCount); sub.complete(); }
  }).pipe(tryAgain(5))
);

// Mathematical
const r_foldInto    = run(of(1, 2, 3, 4, 5).pipe(foldInto((sum, n) => sum + n, 0)));
const r_countValues = run(of('a', 'b', 'c', 'd').pipe(countValues()));
const r_maximum     = run(of(3, 1, 4, 1, 5, 9, 2, 6).pipe(maximum()));
const r_minimum     = run(of(3, 1, 4, 1, 5, 9, 2, 6).pipe(minimum()));
const r_collectAll  = run(of(1, 2, 3).pipe(collectAll()));

// Conditional
const r_orDefault       = run(EMPTY.pipe(orDefault('nothing here')));
const r_allMatch        = run(of(2, 4, 6, 8).pipe(allMatch(x => x % 2 === 0)));
const r_firstMatch      = run(of(1, 2, 3, 4, 5).pipe(firstMatch(x => x > 3)));
const r_firstMatchIndex = run(of(1, 2, 3, 4, 5).pipe(firstMatchIndex(x => x > 3)));
const r_hasNoValues     = run(EMPTY.pipe(hasNoValues()));

// Utility
const tapLog: number[] = [];
const r_sideEffect = run(
  of(1, 2, 3).pipe(sideEffect(x => tapLog.push(x)), transformWith(x => x * 10))
);

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

function asyncCard(friendly: string, original: string, code: string, note: string): string {
  return `
<div class="card">
  <div class="card-header">
    <span class="friendly">${friendly}</span>
    <span class="badges">
      <span class="badge">${original}</span>
      <span class="badge badge--async">time-based</span>
    </span>
  </div>
  <pre class="code">${esc(code)}</pre>
  <div class="async-note">${note}</div>
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

// ── render ─────────────────────────────────────────────────────────────────────

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
<header class="page-header">
  <h1>RxJS — Human-Friendly Names</h1>
  <p>Every operator re-exported with a descriptive alias. Import from <code>src/index.ts</code>.</p>
</header>
<main>

${section('Transformation', 'Apply functions to reshape, accumulate, or expand emitted values.', [

  card('transformWith', 'map',
    `of(1, 2, 3).pipe(\n  transformWith(x => x * 2)\n)`,
    r_transformWith),

  card('accumulate', 'scan',
    `of(1, 2, 3, 4, 5).pipe(\n  accumulate((sum, n) => sum + n, 0)\n)`,
    r_accumulate),

  card('withPrevious', 'pairwise',
    `of(1, 2, 3, 4).pipe(\n  withPrevious()\n)`,
    r_withPrevious),

  card('replaceWith', 'mapTo',
    `of('a', 'b', 'c').pipe(\n  replaceWith('x')\n)`,
    r_replaceWith),

  card('collectN', 'bufferCount',
    `of(1, 2, 3, 4, 5, 6).pipe(\n  collectN(2)\n)`,
    r_collectN),

  card('expandRecursively', 'expand',
    `of(1).pipe(\n  expandRecursively(n => n >= 8 ? EMPTY : of(n * 2))\n)`,
    r_expandRecursively),

  asyncCard('transformSequentially', 'concatMap',
    `from(urls).pipe(\n  transformSequentially(url => fetch$(url))\n)`,
    'Maps each value to an inner observable and waits for it to complete before subscribing to the next.'),

  asyncCard('transformSwitching', 'switchMap',
    `searchInput$.pipe(\n  transformSwitching(q => search$(q))\n)`,
    'Cancels the previous inner observable each time a new source value arrives. Perfect for typeahead.'),

  asyncCard('transformConcurrently', 'mergeMap',
    `clicks$.pipe(\n  transformConcurrently(id => loadItem$(id))\n)`,
    'Subscribes to every inner observable at the same time and merges their emissions.'),

])}

${section('Filtering', 'Decide which values pass through and which are suppressed.', [

  card('keepIf', 'filter',
    `of(1, 2, 3, 4, 5).pipe(\n  keepIf(x => x % 2 === 0)\n)`,
    r_keepIf),

  card('limitTo', 'take',
    `of(1, 2, 3, 4, 5).pipe(\n  limitTo(3)\n)`,
    r_limitTo),

  card('dropFirst', 'skip',
    `of(1, 2, 3, 4, 5).pipe(\n  dropFirst(2)\n)`,
    r_dropFirst),

  card('lastN', 'takeLast',
    `of(1, 2, 3, 4, 5).pipe(\n  lastN(2)\n)`,
    r_lastN),

  card('stopWhenNot', 'takeWhile',
    `of(1, 2, 3, 4, 5).pipe(\n  stopWhenNot(x => x < 4)\n)`,
    r_stopWhenNot),

  card('skipDuplicates', 'distinctUntilChanged',
    `of(1, 1, 2, 2, 3, 1).pipe(\n  skipDuplicates()\n)`,
    r_skipDuplicates),

  card('uniqueValues', 'distinct',
    `of(1, 2, 1, 3, 2, 3).pipe(\n  uniqueValues()\n)`,
    r_uniqueValues),

  card('atIndex', 'elementAt',
    `of('a', 'b', 'c', 'd').pipe(\n  atIndex(2)\n)`,
    r_atIndex),

  asyncCard('afterSilenceOf', 'debounceTime',
    `searchInput$.pipe(\n  afterSilenceOf(300)\n)`,
    'Emits only after the source is quiet for 300 ms. Filters out rapid-fire bursts.'),

  asyncCard('limitRateTo', 'throttleTime',
    `scrollEvents$.pipe(\n  limitRateTo(100)\n)`,
    'Allows at most one emission per 100 ms window, silently dropping the rest.'),

  asyncCard('stopWhen', 'takeUntil',
    `interval(1000).pipe(\n  stopWhen(destroy$)\n)`,
    'Passes values through until the signal observable emits, then completes.'),

])}

${section('Join Creation', 'Combine multiple source observables into one stream.', [

  card('runInSequence', 'concat',
    `runInSequence(\n  of(1, 2),\n  of(3, 4)\n)`,
    r_runInSequence),

  card('waitForAll', 'forkJoin',
    `waitForAll([\n  of('a'), of('b'), of('c')\n])`,
    r_waitForAll),

  card('pairEmissions', 'zip',
    `pairEmissions([\n  of(1, 2, 3),\n  of('a', 'b', 'c')\n])`,
    r_pairEmissions),

  card('splitBy', 'partition',
    `const [evens, odds] = splitBy(\n  of(1, 2, 3, 4, 5),\n  x => x % 2 === 0\n)`,
    r_splitByEvens,
    `<span class="label">odds  →</span>${esc(fmt(r_splitByOdds))}`),

])}

${section('Join (Pipeable)', 'Combine the source stream with other observables mid-pipeline.', [

  card('prependWith', 'startWith',
    `of(3, 4, 5).pipe(\n  prependWith(1, 2)\n)`,
    r_prependWith),

  card('joinSequentially', 'concatAll',
    `of(of(1, 2), of(3, 4)).pipe(\n  joinSequentially()\n)`,
    r_joinSequentially),

  card('pairWithLatest', 'withLatestFrom',
    `of(1, 2, 3).pipe(\n  pairWithLatest(of(10))\n)`,
    r_pairWithLatest),

])}

${section('Error Handling', 'Catch errors and decide whether to recover or retry.', [

  card('handleError', 'catchError',
    `throwError(() => new Error('boom')).pipe(\n  handleError(() => of('recovered'))\n)`,
    r_handleError),

  card('tryAgain', 'retry',
    `new Observable(sub => {\n  if (++n < 3) sub.error('fail');\n  else { sub.next('succeeded'); sub.complete(); }\n}).pipe(tryAgain(5))`,
    r_tryAgain),

])}

${section('Mathematical & Aggregate', 'Compute a single result over the entire completed sequence.', [

  card('foldInto', 'reduce',
    `of(1, 2, 3, 4, 5).pipe(\n  foldInto((sum, n) => sum + n, 0)\n)`,
    r_foldInto),

  card('countValues', 'count',
    `of('a', 'b', 'c', 'd').pipe(\n  countValues()\n)`,
    r_countValues),

  card('maximum', 'max',
    `of(3, 1, 4, 1, 5, 9, 2, 6).pipe(\n  maximum()\n)`,
    r_maximum),

  card('minimum', 'min',
    `of(3, 1, 4, 1, 5, 9, 2, 6).pipe(\n  minimum()\n)`,
    r_minimum),

  card('collectAll', 'toArray',
    `of(1, 2, 3).pipe(\n  collectAll()\n)`,
    r_collectAll),

])}

${section('Conditional & Boolean', 'Evaluate a condition against the whole sequence.', [

  card('orDefault', 'defaultIfEmpty',
    `EMPTY.pipe(\n  orDefault('nothing here')\n)`,
    r_orDefault),

  card('allMatch', 'every',
    `of(2, 4, 6, 8).pipe(\n  allMatch(x => x % 2 === 0)\n)`,
    r_allMatch),

  card('firstMatch', 'find',
    `of(1, 2, 3, 4, 5).pipe(\n  firstMatch(x => x > 3)\n)`,
    r_firstMatch),

  card('firstMatchIndex', 'findIndex',
    `of(1, 2, 3, 4, 5).pipe(\n  firstMatchIndex(x => x > 3)\n)`,
    r_firstMatchIndex),

  card('hasNoValues', 'isEmpty',
    `EMPTY.pipe(\n  hasNoValues()\n)`,
    r_hasNoValues),

])}

${section('Utility', 'Side effects, scheduling, and observable lifecycle helpers.', [

  card('sideEffect', 'tap',
    `of(1, 2, 3).pipe(\n  sideEffect(x => log.push(x)),\n  transformWith(x => x * 10)\n)`,
    r_sideEffect,
    `<span class="label">logged →</span>${esc(fmt(tapLog))}`),

  asyncCard('delayBy', 'delay',
    `of('hello').pipe(\n  delayBy(1000)\n)`,
    'Shifts the emission forward by 1000 ms without changing the value.'),

  asyncCard('failAfter', 'timeout',
    `slowApi$.pipe(\n  failAfter({ each: 5000 })\n)`,
    'Throws a TimeoutError if the source goes 5 seconds without emitting.'),

  asyncCard('shareAmong', 'share',
    `const prices$ = ticker$.pipe(shareAmong());\nprices$.subscribe(chart);\nprices$.subscribe(table);`,
    'Multicasts a single upstream subscription to all subscribers. Resets when the ref-count drops to zero.'),

])}

</main>`;
