import './style.css';
import { of, EMPTY, throwError, Observable, fromEvent, interval, timer, Subject } from 'rxjs';
import {
  emitValues, streamFrom, emitEvery, emitAfterDelay, listenTo,
  lazyStream, emitNothing, failWith, emitRange, streamIf, generateSequence,
  transformWith, accumulate, withPrevious, replaceWith, collectN, expandRecursively,
  accumulateConcurrently, pickProperty,
  keepIf, limitTo, dropFirst, lastN, stopWhenNot, skipDuplicates, uniqueValues, atIndex,
  takeFirst, takeLast, dropLast, dropWhile, exactlyOne, suppressValues, skipDuplicatesBy,
  latestFromAll, runInSequence, waitForAll, pairEmissions, splitBy,
  prependWith, joinSequentially, pairWithLatest, joinConcurrently, combineAllLatest,
  handleError, tryAgain,
  foldInto, countValues, maximum, minimum, collectAll,
  orDefault, allMatch, firstMatch, firstMatchIndex, hasNoValues,
  sideEffect, withTimestamp,
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
// new transformation
const r_accumulateConcurrently = run(of(1, 2, 3).pipe(accumulateConcurrently((acc, n) => of(acc + n), 0)));
const r_pickProperty = run(of({ name: 'Alice', age: 30 }, { name: 'Bob', age: 25 }).pipe(pickProperty('name' as any)));
// new filtering
const r_takeFirst      = run(of(5, 10, 15, 20).pipe(takeFirst()));
const r_takeLast       = run(of(5, 10, 15, 20).pipe(takeLast()));
const r_dropLast       = run(of(1, 2, 3, 4, 5).pipe(dropLast(2)));
const r_dropWhile      = run(of(1, 2, 3, 4, 5, 1).pipe(dropWhile(x => x < 4)));
const r_exactlyOne     = run(of(1, 2, 3, 4, 5).pipe(exactlyOne(x => x === 3)));
const r_suppressValues = run(of(1, 2, 3).pipe(suppressValues()));
const r_skipDuplicatesBy = run(of({ k: 'x', v: 1 }, { k: 'x', v: 2 }, { k: 'y', v: 3 }).pipe(skipDuplicatesBy('k')));
// new join creation
const r_latestFromAll = run(latestFromAll([of(1), of(2), of(3)]));
// new join
const r_joinConcurrently = run(of(of(1, 2), of(3, 4)).pipe(joinConcurrently()));
const r_combineAllLatest = run(of(of(1), of(2), of(3)).pipe(combineAllLatest()));
// new utility
const r_withTimestamp = run(of('ping').pipe(withTimestamp()));
const tapLog: number[] = [];
const r_sideEffect = run(of(1, 2, 3).pipe(sideEffect(x => tapLog.push(x)), transformWith(x => x * 10)));
// creation
const r_emitValues       = run(emitValues(1, 2, 3));
const r_streamFrom       = run(streamFrom([1, 2, 3]));
const r_emitRange        = run(emitRange(1, 5));
const r_emitNothing      = run(emitNothing());
const r_failWith         = run(failWith(() => new Error('oops')).pipe(handleError(() => emitValues('rescued'))));
const r_lazyStream       = run(lazyStream(() => emitValues(1, 2, 3)));
const r_streamIf_t       = run(streamIf(() => true,  emitValues('yes'), emitValues('no')));
const r_streamIf_f       = run(streamIf(() => false, emitValues('yes'), emitValues('no')));
const r_generateSequence = run(generateSequence(0, (x: number) => x < 5, (x: number) => x + 1));

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

function asyncCard(friendly: string, original: string, code: string, note: string): string {
  return `
<div class="card">
  <div class="card-header">
    <span class="friendly">${friendly}</span>
    <span class="badges">
      <span class="badge">${original}</span>
      <span class="badge badge--async">● async</span>
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

// ── operator reference data ───────────────────────────────────────────────────

const OPERATORS: Array<{ cat: string; custom: string; original: string; rationale: string }> = [
  // ── Creation ────────────────────────────────────────────────────────────────
  { cat: 'Creation', custom: 'emitValues',       original: 'of',               rationale: '"Emit Values" describes creating a stream from literal values passed inline' },
  { cat: 'Creation', custom: 'streamFrom',        original: 'from',             rationale: '"Stream From" converts any array, iterable, or Promise into an observable stream' },
  { cat: 'Creation', custom: 'emitEvery',         original: 'interval',         rationale: '"Emit Every" describes the periodic tick at a fixed interval' },
  { cat: 'Creation', custom: 'emitAfterDelay',    original: 'timer',            rationale: '"Emit After Delay" captures the deferred first emission; an interval argument adds recurrence' },
  { cat: 'Creation', custom: 'listenTo',          original: 'fromEvent',        rationale: '"Listen To" captures the event listener mental model in a single phrase' },
  { cat: 'Creation', custom: 'fromEventHandlers', original: 'fromEventPattern', rationale: '"From Event Handlers" describes supplying explicit add/remove handler functions' },
  { cat: 'Creation', custom: 'lazyStream',        original: 'defer',            rationale: '"Lazy Stream" describes per-subscriber deferred creation — the factory runs fresh each time' },
  { cat: 'Creation', custom: 'emitNothing',       original: 'empty',            rationale: '"Emit Nothing" describes completing immediately with zero values' },
  { cat: 'Creation', custom: 'failWith',          original: 'throwError',       rationale: '"Fail With" describes creating a stream that immediately errors with a given value' },
  { cat: 'Creation', custom: 'emitRange',         original: 'range',            rationale: '"Emit Range" describes generating a sequential numeric range as a stream' },
  { cat: 'Creation', custom: 'streamIf',          original: 'iif',              rationale: '"Stream If" reads as a conditional: use one stream if true, another if false' },
  { cat: 'Creation', custom: 'generateSequence',  original: 'generate',         rationale: '"Generate Sequence" describes loop-based value generation driven by a state function' },
  { cat: 'Creation', custom: 'fetchHttp',         original: 'ajax',             rationale: '"Fetch Http" describes making an HTTP request as a stream in plain language' },
  { cat: 'Creation', custom: 'fromCallback',      original: 'bindCallback',     rationale: '"From Callback" describes wrapping a callback-style API into an observable factory' },
  { cat: 'Creation', custom: 'fromNodeCallback',  original: 'bindNodeCallback', rationale: '"From Node Callback" specifies the Node.js error-first (err, result) callback convention' },
  // ── Transformation ──────────────────────────────────────────────────────────
  { cat: 'Transformation', custom: 'transformWith',          original: 'map',           rationale: '"Transform" is the general term for changing shape or type; "With" implies supplying a function' },
  { cat: 'Transformation', custom: 'collectUntilSignal',     original: 'buffer',        rationale: '"Collect" describes accumulation; "Until Signal" clarifies what triggers the batch flush' },
  { cat: 'Transformation', custom: 'collectN',               original: 'bufferCount',   rationale: '"Collect N" is a minimal description of gathering a fixed number of values' },
  { cat: 'Transformation', custom: 'collectForDuration',     original: 'bufferTime',    rationale: '"For Duration" makes the time-window nature explicit' },
  { cat: 'Transformation', custom: 'collectBetweenSignals',  original: 'bufferToggle',  rationale: '"Between Signals" precisely describes the open/close collection boundaries' },
  { cat: 'Transformation', custom: 'collectUntilFactory',    original: 'bufferWhen',    rationale: '"Factory" signals a function that generates the closing observable on demand' },
  { cat: 'Transformation', custom: 'transformSequentially',  original: 'concatMap',     rationale: '"Sequentially" captures one-at-a-time, contrasting directly with transformConcurrently' },
  { cat: 'Transformation', custom: 'switchToSequentially',   original: 'concatMapTo',   rationale: '"Switch To" implies replacing values; "Sequentially" adds the ordering guarantee' },
  { cat: 'Transformation', custom: 'ignoreWhileBusy',        original: 'exhaust',       rationale: '"Ignore While Busy" captures drop-new-while-current-runs in plain language' },
  { cat: 'Transformation', custom: 'transformIfNotBusy',     original: 'exhaustMap',    rationale: '"If Not Busy" makes conditional subscription immediately explicit' },
  { cat: 'Transformation', custom: 'expandRecursively',      original: 'expand',        rationale: '"Recursively" describes the fan-out growth; the operator calls itself on its output' },
  { cat: 'Transformation', custom: 'groupInto',              original: 'groupBy',       rationale: '"Group Into" describes collecting values into named buckets by key' },
  { cat: 'Transformation', custom: 'replaceWith',            original: 'mapTo',         rationale: '"Replace With" describes swapping every value for a constant, unlike map\'s transform function' },
  { cat: 'Transformation', custom: 'transformConcurrently',  original: 'mergeMap',      rationale: '"Concurrently" contrasts directly with transformSequentially' },
  { cat: 'Transformation', custom: 'mergeInto',              original: 'mergeMapTo',    rationale: '"Merge Into" describes funneling every emission into the same shared stream' },
  { cat: 'Transformation', custom: 'accumulateConcurrently', original: 'mergeScan',     rationale: 'Combines running-total (accumulate) with concurrent inner observable execution' },
  { cat: 'Transformation', custom: 'withPrevious',           original: 'pairwise',      rationale: '"With Previous" describes the paired (current, prior) emission in plain language' },
  { cat: 'Transformation', custom: 'pickProperty',           original: 'pluck',         rationale: '"Pick" is a familiar term for selecting one; "Property" makes the key context explicit' },
  { cat: 'Transformation', custom: 'accumulate',             original: 'scan',          rationale: '"Accumulate" is the general term for building a running result over time' },
  { cat: 'Transformation', custom: 'accumulateSwitching',    original: 'switchScan',    rationale: '"Switching" distinguishes from scan: the previous inner observable is cancelled' },
  { cat: 'Transformation', custom: 'transformSwitching',     original: 'switchMap',     rationale: '"Switching" captures cancellation of the prior inner observable on each new value' },
  { cat: 'Transformation', custom: 'switchTo',               original: 'switchMapTo',   rationale: '"Switch To" describes jumping to the same static observable, discarding the previous' },
  { cat: 'Transformation', custom: 'windowUntilSignal',      original: 'window',        rationale: '"Window" kept for domain familiarity; "Until Signal" identifies the closing trigger' },
  { cat: 'Transformation', custom: 'windowOfN',              original: 'windowCount',   rationale: '"Of N" concisely states the fixed-size nature, mirroring collectN' },
  { cat: 'Transformation', custom: 'windowForDuration',      original: 'windowTime',    rationale: '"For Duration" mirrors collectForDuration, consistent across window and buffer families' },
  { cat: 'Transformation', custom: 'windowBetweenSignals',   original: 'windowToggle',  rationale: '"Between Signals" mirrors collectBetweenSignals, consistent across toggle variants' },
  { cat: 'Transformation', custom: 'windowUntilFactory',     original: 'windowWhen',    rationale: '"Until Factory" mirrors collectUntilFactory, signaling a per-window closing function' },
  // ── Filtering ───────────────────────────────────────────────────────────────
  { cat: 'Filtering', custom: 'keepIf',               original: 'filter',                  rationale: '"Keep" states the positive intent; "If" makes the condition explicit' },
  { cat: 'Filtering', custom: 'emitLatestAfterSignal', original: 'audit',                  rationale: '"Latest After Signal" describes exactly which value is emitted and when' },
  { cat: 'Filtering', custom: 'emitLatestAfterDelay',  original: 'auditTime',              rationale: '"After Delay" replaces "Signal" with a fixed duration' },
  { cat: 'Filtering', custom: 'afterSilence',          original: 'debounce',               rationale: '"After Silence" captures the required pause before a value is released' },
  { cat: 'Filtering', custom: 'afterSilenceOf',        original: 'debounceTime',           rationale: '"Of" suggests a measurable duration, making the fixed-time nature explicit' },
  { cat: 'Filtering', custom: 'uniqueValues',          original: 'distinct',               rationale: '"Unique" is the plain-language word for values never seen before' },
  { cat: 'Filtering', custom: 'skipDuplicates',        original: 'distinctUntilChanged',   rationale: '"Skip Duplicates" describes the adjacent-equality check in one phrase' },
  { cat: 'Filtering', custom: 'skipDuplicatesBy',      original: 'distinctUntilKeyChanged',rationale: '"By" indicates a specific key is used for comparison' },
  { cat: 'Filtering', custom: 'atIndex',               original: 'elementAt',              rationale: '"At Index" uses array-access language every developer already knows' },
  { cat: 'Filtering', custom: 'takeFirst',             original: 'first',                  rationale: '"Take First" aligns with the take/limitTo family, making count (one) implicit' },
  { cat: 'Filtering', custom: 'suppressValues',        original: 'ignoreElements',         rationale: '"Suppress" means actively silencing; "Values" clarifies what is dropped' },
  { cat: 'Filtering', custom: 'takeLast',              original: 'last',                   rationale: '"Take Last" mirrors takeFirst, forming a symmetric first/last pair' },
  { cat: 'Filtering', custom: 'snapshotOn',            original: 'sample',                 rationale: '"Snapshot" captures freezing the current value at a specific moment' },
  { cat: 'Filtering', custom: 'snapshotEvery',         original: 'sampleTime',             rationale: '"Every" signals periodic repetition, distinguishing from signal-triggered snapshotOn' },
  { cat: 'Filtering', custom: 'exactlyOne',            original: 'single',                 rationale: '"Exactly One" describes both the expected count and the error if violated' },
  { cat: 'Filtering', custom: 'dropFirst',             original: 'skip',                   rationale: '"Drop First" is the inverse of takeFirst/limitTo, forming an intuitive complement' },
  { cat: 'Filtering', custom: 'dropLast',              original: 'skipLast',               rationale: '"Drop Last" mirrors dropFirst for the end of the sequence' },
  { cat: 'Filtering', custom: 'startAfterSignal',      original: 'skipUntil',              rationale: '"Start After Signal" describes enabling the stream from a trigger point onward' },
  { cat: 'Filtering', custom: 'dropWhile',             original: 'skipWhile',              rationale: '"Drop While" mirrors stopWhenNot (takeWhile), forming a consistent symmetry' },
  { cat: 'Filtering', custom: 'limitTo',               original: 'take',                   rationale: '"Limit To" describes capping the stream at a maximum count' },
  { cat: 'Filtering', custom: 'lastN',                 original: 'takeLast',               rationale: '"Last N" is the most direct way to say "final N values before completion"' },
  { cat: 'Filtering', custom: 'stopWhen',              original: 'takeUntil',              rationale: '"Stop When" describes terminating the stream the moment a condition is met' },
  { cat: 'Filtering', custom: 'stopWhenNot',           original: 'takeWhile',              rationale: '"Stop When Not" describes halting once the predicate flips to false' },
  { cat: 'Filtering', custom: 'limitRate',             original: 'throttle',               rationale: '"Limit Rate" describes controlling emission frequency using a signal observable' },
  { cat: 'Filtering', custom: 'limitRateTo',           original: 'throttleTime',           rationale: '"Rate To" implies a fixed time window, distinguishing from signal-driven limitRate' },
  // ── Join Creation ───────────────────────────────────────────────────────────
  { cat: 'Join Creation', custom: 'latestFromAll', original: 'combineLatest', rationale: '"Latest" highlights recency; "From All" makes the multi-source nature clear' },
  { cat: 'Join Creation', custom: 'runInSequence', original: 'concat',        rationale: '"Run In Sequence" describes serial execution without needing to know what "concat" means' },
  { cat: 'Join Creation', custom: 'waitForAll',    original: 'forkJoin',      rationale: '"Wait For All" captures the blocking-until-all-complete behavior intuitively' },
  { cat: 'Join Creation', custom: 'mergeAll',      original: 'merge',         rationale: '"Merge All" preserves familiarity while making concurrent subscription implicit' },
  { cat: 'Join Creation', custom: 'splitBy',       original: 'partition',     rationale: '"Split" describes dividing into two; "By" indicates a predicate drives the split' },
  { cat: 'Join Creation', custom: 'firstToEmit',   original: 'race',          rationale: '"First To Emit" describes the winner-takes-all behavior precisely' },
  { cat: 'Join Creation', custom: 'pairEmissions', original: 'zip',           rationale: '"Pair" describes combining by position; "Emissions" clarifies we work with stream values' },
  // ── Join ────────────────────────────────────────────────────────────────────
  { cat: 'Join', custom: 'combineAllLatest', original: 'combineLatestAll', rationale: 'Reordering to "Combine All Latest" reads more naturally as an action sentence' },
  { cat: 'Join', custom: 'joinSequentially', original: 'concatAll',        rationale: '"Join" unifies the flattening family; "Sequentially" matches the concat naming pattern' },
  { cat: 'Join', custom: 'joinIfNotBusy',    original: 'exhaustAll',       rationale: '"If Not Busy" mirrors exhaustMap, consistent across the exhaust family' },
  { cat: 'Join', custom: 'joinConcurrently', original: 'mergeAll',         rationale: '"Join Concurrently" mirrors mergeMap, consistent across the merge family' },
  { cat: 'Join', custom: 'joinSwitching',    original: 'switchAll',        rationale: '"Switching" mirrors switchMap, consistent across the switch family' },
  { cat: 'Join', custom: 'prependWith',      original: 'startWith',        rationale: '"Prepend" is the standard term for inserting values at the front of a sequence' },
  { cat: 'Join', custom: 'pairWithLatest',   original: 'withLatestFrom',   rationale: '"Pair With Latest" describes attaching the most recent snapshot from a secondary stream' },
  // ── Multicasting ────────────────────────────────────────────────────────────
  { cat: 'Multicasting', custom: 'shareVia',            original: 'multicast',       rationale: '"Via" indicates routing through a provided Subject, making the mechanism explicit' },
  { cat: 'Multicasting', custom: 'makeHot',              original: 'publish',         rationale: '"Make Hot" uses the standard cold/hot distinction to describe the conversion' },
  { cat: 'Multicasting', custom: 'makeHotWithLatest',    original: 'publishBehavior', rationale: '"With Latest" indicates new subscribers receive the most recent value immediately' },
  { cat: 'Multicasting', custom: 'makeHotWithLastValue', original: 'publishLast',     rationale: '"With Last Value" indicates subscribers receive only the final value on completion' },
  { cat: 'Multicasting', custom: 'makeHotWithReplay',    original: 'publishReplay',   rationale: '"With Replay" indicates buffered past values are replayed to new subscribers' },
  { cat: 'Multicasting', custom: 'shareAmong',           original: 'share',           rationale: '"Among" implies distributed sharing across many subscribers' },
  // ── Error Handling ──────────────────────────────────────────────────────────
  { cat: 'Error Handling', custom: 'handleError',       original: 'catchError', rationale: '"Handle Error" is the universal software term, understood without RxJS knowledge' },
  { cat: 'Error Handling', custom: 'tryAgain',          original: 'retry',      rationale: '"Try Again" is the plain-language equivalent, immediately understood by anyone' },
  { cat: 'Error Handling', custom: 'retryWhenSignaled', original: 'retryWhen',  rationale: '"When Signaled" makes the external-control aspect explicit vs. a count-based retry' },
  // ── Utility ─────────────────────────────────────────────────────────────────
  { cat: 'Utility', custom: 'sideEffect',         original: 'tap',           rationale: '"Side Effect" is the precise functional term for actions that don\'t alter the stream' },
  { cat: 'Utility', custom: 'delayBy',            original: 'delay',         rationale: '"By" suggests a delta shift by an amount, not absolute-time scheduling' },
  { cat: 'Utility', custom: 'delayUntilSignal',   original: 'delayWhen',     rationale: '"Until Signal" describes waiting for a per-value observable before forwarding' },
  { cat: 'Utility', custom: 'unwrapNotification', original: 'dematerialize', rationale: '"Unwrap" is the natural inverse of wrap; "Notification" names what is unpacked' },
  { cat: 'Utility', custom: 'wrapAsNotification', original: 'materialize',   rationale: '"Wrap As Notification" describes boxing every event into a typed container' },
  { cat: 'Utility', custom: 'scheduleOn',         original: 'observeOn',     rationale: '"Schedule On" describes directing delivery to a specific scheduler' },
  { cat: 'Utility', custom: 'subscribeUsing',     original: 'subscribeOn',   rationale: '"Using" indicates the scheduler governs how and where subscription starts' },
  { cat: 'Utility', custom: 'withTimeInterval',   original: 'timeInterval',  rationale: '"With Time Interval" describes attaching elapsed-time as metadata to each value' },
  { cat: 'Utility', custom: 'withTimestamp',      original: 'timestamp',     rationale: '"With Timestamp" describes attaching a wall-clock time reading to each value' },
  { cat: 'Utility', custom: 'failAfter',          original: 'timeout',       rationale: '"Fail After" describes erroring out once the time limit is exceeded' },
  { cat: 'Utility', custom: 'fallbackAfter',      original: 'timeoutWith',   rationale: '"Fallback After" describes switching to an alternative stream on timeout' },
  { cat: 'Utility', custom: 'collectAll',         original: 'toArray',       rationale: '"All" means the entire stream in one batch, pairing with collectN and collectForDuration' },
  // ── Conditional & Boolean ───────────────────────────────────────────────────
  { cat: 'Conditional & Boolean', custom: 'orDefault',       original: 'defaultIfEmpty', rationale: '"Or Default" is the conditional expression pattern: value, or fallback if absent' },
  { cat: 'Conditional & Boolean', custom: 'allMatch',        original: 'every',          rationale: '"All Match" is standard predicate language for universal quantification' },
  { cat: 'Conditional & Boolean', custom: 'firstMatch',      original: 'find',           rationale: '"First Match" is the standard description for finding the first satisfying element' },
  { cat: 'Conditional & Boolean', custom: 'firstMatchIndex', original: 'findIndex',      rationale: '"Index" appended to firstMatch distinguishes position from value' },
  { cat: 'Conditional & Boolean', custom: 'hasNoValues',     original: 'isEmpty',        rationale: '"Has No Values" is a boolean property that reads naturally as a statement' },
  // ── Mathematical & Aggregate ────────────────────────────────────────────────
  { cat: 'Mathematical & Aggregate', custom: 'countValues', original: 'count',  rationale: '"Count Values" adds the object of counting for clarity over the bare verb' },
  { cat: 'Mathematical & Aggregate', custom: 'maximum',     original: 'max',    rationale: '"Maximum" is the full English word, avoiding single-letter abbreviation' },
  { cat: 'Mathematical & Aggregate', custom: 'minimum',     original: 'min',    rationale: '"Minimum" is the full English word, matching maximum for symmetry' },
  { cat: 'Mathematical & Aggregate', custom: 'foldInto',    original: 'reduce', rationale: '"Fold Into" is the functional term for collapsing a sequence to one accumulated value' },
];

function referenceList(): string {
  const cats = [...new Set(OPERATORS.map(o => o.cat))];
  const navBtns = cats.map(cat =>
    `<button class="ref-nav-btn" data-cat="${cat}">${cat}</button>`
  ).join('');
  const rows = cats.map(cat => {
    const items = OPERATORS
      .filter(o => o.cat === cat)
      .map(o => `<div class="op-row">
        <span class="op-custom">${o.custom}</span>
        <span class="op-arrow">→</span>
        <span class="op-original">${o.original}</span>
        <span class="op-rationale">${o.rationale}</span>
      </div>`).join('');
    return `<div class="op-cat" data-cat="${cat}">${cat}</div>${items}`;
  }).join('');
  return `
<section class="section">
  <div class="section-header">
    <h2>All Operators — Name Reference</h2>
    <p>111 RxJS operators with human-friendly aliases and the rationale for each chosen name.</p>
  </div>
  <div class="ref-layout">
    <nav class="ref-nav">${navBtns}</nav>
    <div class="op-list">${rows}</div>
  </div>
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

  // 8. emitEvery — start/stop an interval
  const everyStart = document.getElementById('i-every-start')!;
  const everyStop  = document.getElementById('i-every-stop')!;
  let every$ = new Subject<void>();
  let everyRunning = false;
  fromEvent(everyStart, 'click').subscribe(() => {
    if (everyRunning) return;
    everyRunning = true;
    every$ = new Subject<void>();
    appendLog('l-every', 'started');
    emitEvery(1000).pipe(stopWhen(every$)).subscribe({
      next: n => appendLog('l-every', `tick ${n + 1}`),
      complete: () => { everyRunning = false; appendLog('l-every', 'stopped ✓'); },
    });
  });
  fromEvent(everyStop, 'click').subscribe(() => { if (everyRunning) every$.next(); });

  // 9. emitAfterDelay — fires once 1 s after click; switchMap cancels if clicked again
  const timerBtn = document.getElementById('i-timer')!;
  fromEvent(timerBtn, 'click').pipe(
    transformSwitching(() => emitAfterDelay(1000))
  ).subscribe(() => appendLog('l-timer', 'fired after 1 s ✓'));

  // 10. listenTo — count clicks on a dedicated button
  const listenBtn = document.getElementById('i-listen')!;
  listenTo(listenBtn, 'click').pipe(
    transformWith((_, i) => `click #${i + 1}`)
  ).subscribe(msg => appendLog('l-listen', msg));
}

// ── render ─────────────────────────────────────────────────────────────────────

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
<nav class="site-nav">
  <a class="home-link" href="https://hansschenker.github.io">← hansschenker.github.io</a>
</nav>
<header class="page-header">
  <h1>RxJS — Human-Friendly Names</h1>
  <p>Every operator re-exported with a descriptive alias. Import from <code>src/index.ts</code>.</p>
</header>
<main>

${referenceList()}

${section('Creation', 'Create a new observable from values, sequences, events, or async sources.', [
  card('emitValues', 'of',
    `emitValues(1, 2, 3)`, r_emitValues),
  card('streamFrom', 'from',
    `streamFrom([1, 2, 3])`, r_streamFrom),
  card('emitRange', 'range',
    `emitRange(1, 5)`, r_emitRange),
  card('emitNothing', 'empty',
    `emitNothing()\n// completes with zero emissions`, r_emitNothing),
  card('failWith', 'throwError',
    `failWith(() => new Error('oops')).pipe(\n  handleError(() => emitValues('rescued'))\n)`, r_failWith),
  card('lazyStream', 'defer',
    `lazyStream(() => emitValues(1, 2, 3))\n// factory runs fresh per subscriber`, r_lazyStream),
  card('streamIf', 'iif',
    `streamIf(\n  () => true,\n  emitValues('yes'),\n  emitValues('no')\n)`,
    r_streamIf_t,
    `<span class="label">false →</span>${esc(fmt(r_streamIf_f))}`),
  card('generateSequence', 'generate',
    `generateSequence(\n  0, x => x < 5, x => x + 1\n)`, r_generateSequence),
  asyncCard('fetchHttp', 'ajax',
    `fetchHttp('/api/users').pipe(\n  transformWith(res => res.response)\n)`,
    'Makes an HTTP request and emits the parsed response. Requires a real browser or Node.js HTTP environment.'),
  asyncCard('fromEventHandlers', 'fromEventPattern',
    `fromEventHandlers(\n  h => document.addEventListener('click', h),\n  h => document.removeEventListener('click', h)\n)`,
    'Creates a stream from any event API using explicit add/remove handler functions — useful when fromEvent doesn\'t cover the API.'),
  asyncCard('fromCallback', 'bindCallback',
    `const getData = fromCallback(\n  (cb) => setTimeout(() => cb('hello'), 100)\n);\ngetData()`,
    'Wraps a callback-style function (last arg is cb) into an observable factory callable with the same arguments.'),
  asyncCard('fromNodeCallback', 'bindNodeCallback',
    `const readFile$ = fromNodeCallback(fs.readFile);\nreadFile$('data.txt', 'utf-8')`,
    'Wraps a Node.js error-first callback (err, result) into an observable factory — emits result or errors on err.'),
])}

${section('Transformation', 'Apply functions to reshape, accumulate, or expand emitted values.', [
  card('transformWith', 'map',       `of(1, 2, 3).pipe(\n  transformWith(x => x * 2)\n)`,              r_transformWith),
  card('accumulate',   'scan',       `of(1, 2, 3, 4, 5).pipe(\n  accumulate((sum, n) => sum + n, 0)\n)`, r_accumulate),
  card('withPrevious', 'pairwise',   `of(1, 2, 3, 4).pipe(\n  withPrevious()\n)`,                     r_withPrevious),
  card('replaceWith',  'mapTo',      `of('a', 'b', 'c').pipe(\n  replaceWith('x')\n)`,                r_replaceWith),
  card('collectN',     'bufferCount',`of(1, 2, 3, 4, 5, 6).pipe(\n  collectN(2)\n)`,                  r_collectN),
  card('expandRecursively', 'expand',`of(1).pipe(\n  expandRecursively(n => n >= 8 ? EMPTY : of(n * 2))\n)`, r_expandRecursively),
  card('accumulateConcurrently', 'mergeScan', `of(1, 2, 3).pipe(\n  accumulateConcurrently(\n    (acc, n) => of(acc + n), 0\n  )\n)`, r_accumulateConcurrently),
  card('pickProperty', 'pluck', `of({name:'Alice'}, {name:'Bob'}).pipe(\n  pickProperty('name')\n)`, r_pickProperty),
])}

${section('Filtering', 'Decide which values pass through and which are suppressed.', [
  card('keepIf',        'filter',              `of(1, 2, 3, 4, 5).pipe(\n  keepIf(x => x % 2 === 0)\n)`,  r_keepIf),
  card('limitTo',       'take',                `of(1, 2, 3, 4, 5).pipe(\n  limitTo(3)\n)`,                r_limitTo),
  card('dropFirst',     'skip',                `of(1, 2, 3, 4, 5).pipe(\n  dropFirst(2)\n)`,              r_dropFirst),
  card('lastN',         'takeLast',            `of(1, 2, 3, 4, 5).pipe(\n  lastN(2)\n)`,                  r_lastN),
  card('stopWhenNot',   'takeWhile',           `of(1, 2, 3, 4, 5).pipe(\n  stopWhenNot(x => x < 4)\n)`,  r_stopWhenNot),
  card('skipDuplicates','distinctUntilChanged',`of(1, 1, 2, 2, 3, 1).pipe(\n  skipDuplicates()\n)`,      r_skipDuplicates),
  card('uniqueValues',  'distinct',            `of(1, 2, 1, 3, 2, 3).pipe(\n  uniqueValues()\n)`,        r_uniqueValues),
  card('atIndex',          'elementAt',              `of('a', 'b', 'c', 'd').pipe(\n  atIndex(2)\n)`,                         r_atIndex),
  card('takeFirst',        'first',                  `of(5, 10, 15, 20).pipe(\n  takeFirst()\n)`,                               r_takeFirst),
  card('takeLast',         'last',                   `of(5, 10, 15, 20).pipe(\n  takeLast()\n)`,                                r_takeLast),
  card('dropLast',         'skipLast',               `of(1, 2, 3, 4, 5).pipe(\n  dropLast(2)\n)`,                               r_dropLast),
  card('dropWhile',        'skipWhile',              `of(1, 2, 3, 4, 5, 1).pipe(\n  dropWhile(x => x < 4)\n)`,                 r_dropWhile),
  card('exactlyOne',       'single',                 `of(1, 2, 3, 4, 5).pipe(\n  exactlyOne(x => x === 3)\n)`,                 r_exactlyOne),
  card('suppressValues',   'ignoreElements',         `of(1, 2, 3).pipe(\n  suppressValues()\n)`,                               r_suppressValues),
  card('skipDuplicatesBy', 'distinctUntilKeyChanged',`of({k:'x',v:1},{k:'x',v:2},{k:'y',v:3})\n  .pipe(skipDuplicatesBy('k'))`,r_skipDuplicatesBy),
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
  card('latestFromAll', 'combineLatest',
    `latestFromAll([\n  of(1), of(2), of(3)\n])`, r_latestFromAll),
])}

${section('Join (Pipeable)', 'Combine the source stream with other observables mid-pipeline.', [
  card('prependWith',    'startWith',    `of(3, 4, 5).pipe(\n  prependWith(1, 2)\n)`,           r_prependWith),
  card('joinSequentially','concatAll',   `of(of(1, 2), of(3, 4)).pipe(\n  joinSequentially()\n)`, r_joinSequentially),
  card('pairWithLatest',   'withLatestFrom', `of(1, 2, 3).pipe(\n  pairWithLatest(of(10))\n)`,              r_pairWithLatest),
  card('joinConcurrently', 'mergeAll',      `of(of(1, 2), of(3, 4)).pipe(\n  joinConcurrently()\n)`,         r_joinConcurrently),
  card('combineAllLatest', 'combineLatestAll', `of(of(1), of(2), of(3)).pipe(\n  combineAllLatest()\n)`,     r_combineAllLatest),
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
  card('withTimestamp', 'timestamp', `of('ping').pipe(\n  withTimestamp()\n)`, r_withTimestamp),
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

  interactiveCard('emitEvery', 'interval',
    `<button class="demo-btn" id="i-every-start">▶ Start</button>
     <button class="demo-btn" id="i-every-stop">■ Stop</button>`,
    `emitEvery(1000).pipe(\n  stopWhen(stop$)\n)`,
    'l-every'),

  interactiveCard('emitAfterDelay', 'timer',
    `<button class="demo-btn" id="i-timer">Fire after 1s</button>`,
    `fromEvent(btn, 'click').pipe(\n  transformSwitching(() =>\n    emitAfterDelay(1000)\n  )\n)`,
    'l-timer'),

  interactiveCard('listenTo', 'fromEvent',
    `<button class="demo-btn" id="i-listen">Click me!</button>`,
    `listenTo(btn, 'click').pipe(\n  transformWith((_, i) => \`click #\${i + 1}\`)\n)`,
    'l-listen'),

])}

</main>
<footer class="site-footer">
  <a href="https://hansschenker.github.io" class="footer-home">← hansschenker.github.io</a>
</footer>`;

setupInteractiveDemos();
setupRefNav();

function setupRefNav() {
  const opList = document.querySelector<HTMLElement>('.op-list')!;
  const btns = Array.from(document.querySelectorAll<HTMLButtonElement>('.ref-nav-btn'));

  btns.forEach(btn => {
    btn.addEventListener('click', () => {
      const target = opList.querySelector<HTMLElement>(`.op-cat[data-cat="${btn.dataset.cat}"]`);
      if (!target) return;
      const top = target.getBoundingClientRect().top - opList.getBoundingClientRect().top + opList.scrollTop;
      opList.scrollTo({ top, behavior: 'smooth' });
      setActive(btn.dataset.cat!);
    });
  });

  function setActive(cat: string) {
    btns.forEach(b => b.classList.toggle('active', b.dataset.cat === cat));
  }

  // Update active category as the list scrolls
  const observer = new IntersectionObserver(
    entries => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          setActive((entry.target as HTMLElement).dataset.cat!);
        }
      });
    },
    { root: opList, threshold: 0, rootMargin: '0px 0px -88% 0px' }
  );

  opList.querySelectorAll<HTMLElement>('.op-cat').forEach(el => observer.observe(el));
  // Initialise first category as active
  setActive(btns[0]?.dataset.cat ?? '');
}
